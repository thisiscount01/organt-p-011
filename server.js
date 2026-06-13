'use strict';

/**
 * 응급실 실시간 현황 백엔드 서버
 *
 * - 서버 단독 e-GEN 폴링 (≤1req/min) — 동시 접속자 무관 쿼터 고정
 * - REST GET /api/hospitals + SSE /api/stream 팬아웃
 * - e-GEN 장애 시 캐시 폴백 30분+ 서비스 지속
 * - API 키는 서버 전용, 브라우저 미노출
 *
 * 환경변수:
 *   EGEN_API_KEY  — 실제 e-GEN 공공데이터 API 키 (미설정 시 데모 모드)
 *   PORT          — 리슨 포트 (기본 3000)
 */

const http = require('http');
const https = require('https');
const url = require('url');
const { StringDecoder } = require('string_decoder');
const fs       = require('fs');
const nodePath = require('path');

// ──────────────────────────────────────────────
// 설정값 (외부화된 상수 — 출처: 팀 합의 + 공공 API 한도 기준)
// ──────────────────────────────────────────────
const PORT              = parseInt(process.env.PORT, 10) || 3000;
const EGEN_API_KEY      = process.env.EGEN_API_KEY || null;

// 폴링 주기: 실 API 60초, 데모 30초 (팀 합의)
const REAL_POLL_MS      = 60_000;
const DEMO_POLL_MS      = 30_000;

// 캐시 신선도 임계값 (팀 합의: 구 데이터 경고 기준 5분, 서비스 중단 기준 30분)
const STALE_WARN_SEC    = 5 * 60;    // 5분 초과 → isCacheStale = true
const STALE_LIMIT_SEC   = 30 * 60;  // 30분 초과 → 서비스 불가 상태

// e-GEN 실시간 병상 API
const EGEN_HOST    = 'apis.data.go.kr';
const EGEN_PATH    = '/B552657/ErmctInfoInqireService/getEmrrmRltmUsefulSckbdInfoInqire';
const EGEN_ROWS    = 100;

// CORS 허용 오리진 (배포 시 환경변수로 대체 가능)
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';

// ──────────────────────────────────────────────
// 데모 데이터 — 전국 응급실 17개 (현실적 좌표·병상수)
// beds.baseAvailable: 시뮬레이션 기준값 (30초마다 ±3 변동)
// ──────────────────────────────────────────────
const DEMO_BASE = [
  { id:'A1100001', name:'서울대학교병원 응급의료센터',     lat:37.5796, lng:126.9997, address:'서울 종로구 대학로 101',           phone:'02-2072-2499', totalBeds:45, baseAvail:12 },
  { id:'A1100002', name:'세브란스병원 응급센터',           lat:37.5623, lng:126.9414, address:'서울 서대문구 연세로 50-1',         phone:'02-2228-8988', totalBeds:50, baseAvail:8  },
  { id:'A1100003', name:'삼성서울병원 응급의료센터',       lat:37.4882, lng:127.0856, address:'서울 강남구 일원로 81',             phone:'02-3410-2000', totalBeds:60, baseAvail:15 },
  { id:'A1100004', name:'서울아산병원 응급의료센터',       lat:37.5271, lng:127.1083, address:'서울 송파구 올림픽로43길 88',       phone:'02-3010-5999', totalBeds:55, baseAvail:20 },
  { id:'A1100005', name:'고려대구로병원 응급센터',         lat:37.4934, lng:126.8559, address:'서울 구로구 구로동로 148',           phone:'02-2626-1295', totalBeds:35, baseAvail:5  },
  { id:'A1100006', name:'강남세브란스병원 응급센터',       lat:37.4886, lng:127.0619, address:'서울 강남구 언주로 211',            phone:'02-2019-3114', totalBeds:30, baseAvail:4  },
  { id:'A1100007', name:'가톨릭대서울성모병원 응급센터',   lat:37.5013, lng:126.9948, address:'서울 서초구 반포대로 222',          phone:'02-2258-5555', totalBeds:40, baseAvail:18 },
  { id:'A1200001', name:'아주대병원 권역응급의료센터',     lat:37.2790, lng:127.0435, address:'경기 수원시 영통구 월드컵로 164',   phone:'031-219-7777', totalBeds:42, baseAvail:10 },
  { id:'A1200002', name:'분당서울대병원 응급센터',         lat:37.3544, lng:127.1245, address:'경기 성남시 분당구 구미로173번길 82', phone:'031-787-7575', totalBeds:38, baseAvail:7  },
  { id:'A1200003', name:'인하대병원 권역응급의료센터',     lat:37.4485, lng:126.6530, address:'인천 중구 인항로 27',               phone:'032-890-3119', totalBeds:38, baseAvail:13 },
  { id:'A2600001', name:'부산대병원 권역응급의료센터',     lat:35.1097, lng:129.0431, address:'부산 서구 구덕로 179',              phone:'051-240-7119', totalBeds:48, baseAvail:14 },
  { id:'A2600002', name:'동아대병원 응급의료센터',         lat:35.1002, lng:128.9745, address:'부산 서구 대신공원로 26',           phone:'051-240-2000', totalBeds:28, baseAvail:6  },
  { id:'A2700001', name:'경북대병원 권역응급의료센터',     lat:35.8767, lng:128.6078, address:'대구 중구 달성로 56',               phone:'053-200-5555', totalBeds:44, baseAvail:16 },
  { id:'A2700002', name:'대구가톨릭대병원 응급센터',       lat:35.8714, lng:128.5984, address:'대구 남구 두류공원로17길 33',       phone:'053-650-4119', totalBeds:32, baseAvail:3  },
  { id:'A2900001', name:'전남대병원 권역응급의료센터',     lat:35.1667, lng:126.9103, address:'광주 동구 제봉로 42',               phone:'062-220-5555', totalBeds:40, baseAvail:11 },
  { id:'A3000001', name:'충남대병원 권역응급의료센터',     lat:36.3509, lng:127.3843, address:'대전 중구 문화로 282',              phone:'042-280-7777', totalBeds:36, baseAvail:9  },
  { id:'A3500001', name:'제주대병원 권역응급의료센터',     lat:33.4895, lng:126.5316, address:'제주 제주시 아란13길 15',           phone:'064-717-1119', totalBeds:25, baseAvail:6  },
];

// ──────────────────────────────────────────────
// 상태 저장소 (in-process — Redis 없는 단일 서버 구성)
// ──────────────────────────────────────────────
const store = {
  hospitals: [],          // 현재 병원 목록 (거리 제외)
  updatedAt:  null,       // ISO8601 — 마지막 성공 갱신 시각
  lastFetch:  null,       // ISO8601 — 마지막 폴링 시도 시각
  fetchError: null,       // 최근 폴링 실패 이유
  isCached:   false,      // 실제 API 갱신 실패 → 캐시 서비스 중
};

// SSE 연결 관리
const sseClients = new Set();

// ──────────────────────────────────────────────
// 유틸: Haversine 거리 (km)
// ──────────────────────────────────────────────
function haversine(lat1, lng1, lat2, lng2) {
  const R   = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2
          + Math.cos(lat1 * Math.PI / 180)
          * Math.cos(lat2 * Math.PI / 180)
          * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ──────────────────────────────────────────────
// 유틸: 캐시 신선도 (초)
// ──────────────────────────────────────────────
function dataAgeSec() {
  if (!store.updatedAt) return Infinity;
  return (Date.now() - new Date(store.updatedAt).getTime()) / 1000;
}

// ──────────────────────────────────────────────
// 데모 모드: 30초마다 병상 수를 소폭 변동시켜 실시간성 시뮬레이션
// ──────────────────────────────────────────────
function buildDemoData() {
  const now = new Date().toISOString();
  return DEMO_BASE.map(h => {
    // -3 ~ +3 범위 랜덤 변동 (Math.random 사용 허용 — 서버 내부 시뮬레이션용)
    const delta = Math.floor(Math.random() * 7) - 3;
    const avail = Math.max(0, Math.min(h.totalBeds, h.baseAvail + delta));
    return {
      id:        h.id,
      name:      h.name,
      lat:       h.lat,
      lng:       h.lng,
      address:   h.address,
      phone:     h.phone,
      available: avail > 0,
      beds:      { total: h.totalBeds, available: avail },
      distance:  null,   // per-request 계산
      updatedAt: now,
    };
  });
}

// ──────────────────────────────────────────────
// e-GEN 실 API 호출 (HTTPS GET → JSON)
// ──────────────────────────────────────────────
function fetchEGEN() {
  return new Promise((resolve, reject) => {
    const params = new URLSearchParams({
      ServiceKey: EGEN_API_KEY,
      pageNo:     '1',
      numOfRows:  String(EGEN_ROWS),
      _type:      'json',
    });
    const reqPath = `${EGEN_PATH}?${params.toString()}`;
    const options = { hostname: EGEN_HOST, path: reqPath, method: 'GET', timeout: 10_000 };

    const req = https.request(options, res => {
      if (res.statusCode !== 200) {
        reject(new Error(`e-GEN HTTP ${res.statusCode}`));
        res.resume();
        return;
      }
      const dec = new StringDecoder('utf8');
      let body = '';
      res.on('data', chunk => { body += dec.write(chunk); });
      res.on('end', () => {
        body += dec.end();
        try {
          const json = JSON.parse(body);
          resolve(json);
        } catch (e) {
          reject(new Error('e-GEN JSON parse error'));
        }
      });
    });

    req.on('timeout', () => { req.destroy(); reject(new Error('e-GEN timeout')); });
    req.on('error', reject);
    req.end();
  });
}

// ──────────────────────────────────────────────
// e-GEN 응답 → 내부 hospital 객체 변환
// ──────────────────────────────────────────────
function mapEGENItem(item) {
  const availBeds = Number(item.hvec  ?? NaN);   // undefined → NaN → isNaN → null (안전)
  // e-GEN 필드: hvec = 가용 일반병상, hpbdn = 총병상 (API 버전마다 상이)
  const total     = isNaN(Number(item.hpbdn)) ? null : Number(item.hpbdn);
  const available = isNaN(availBeds) ? null : availBeds;

  return {
    id:        item.hpid        || '',
    name:      item.dutyName    || '',
    lat:       parseFloat(item.wgs84Lat) || 0,
    lng:       parseFloat(item.wgs84Lon) || 0,
    address:   item.dutyAddr    || '',
    phone:     item.dutyTel3    || item.dutyTel1 || '',
    available: available === null ? true : available > 0,
    beds:      { total, available },
    distance:  null,
    updatedAt: new Date().toISOString(),
  };
}

// ──────────────────────────────────────────────
// 폴링 — 캐시 갱신 핵심 루프
// ──────────────────────────────────────────────
async function poll() {
  store.lastFetch = new Date().toISOString();

  if (!EGEN_API_KEY) {
    // ── 데모 모드 ──
    store.hospitals  = buildDemoData();
    store.updatedAt  = new Date().toISOString();
    store.isCached   = false;
    store.fetchError = null;
    broadcastSSE('full_update');
    return;
  }

  // ── 실 API 모드 ──
  try {
    const json     = await fetchEGEN();
    const items    = json?.response?.body?.items?.item;
    if (!Array.isArray(items) && !items) throw new Error('empty response');
    const arr      = Array.isArray(items) ? items : [items];
    const hospitals = arr
      .map(mapEGENItem)
      .filter(h => h.lat && h.lng);   // 좌표 없는 항목 제거

    store.hospitals  = hospitals;
    store.updatedAt  = new Date().toISOString();
    store.isCached   = false;
    store.fetchError = null;
    broadcastSSE('full_update');
    console.log(`[poll] e-GEN 갱신 — 병원 ${hospitals.length}개`);
  } catch (err) {
    store.fetchError = err.message;
    store.isCached   = true;   // 이전 캐시로 서비스 계속
    console.error(`[poll] e-GEN 실패 (캐시 폴백): ${err.message}`);
    // 캐시 30분 이내면 SSE에 stale 상태로 브로드캐스트
    if (dataAgeSec() <= STALE_LIMIT_SEC) {
      broadcastSSE('stale_notice');
    }
  }
}

// ──────────────────────────────────────────────
// SSE 브로드캐스트
// ──────────────────────────────────────────────
function buildSSEPayload(eventType) {
  const age = dataAgeSec();
  return {
    event_type:        eventType,                        // 'full_update' | 'stale_notice'
    hospitals:         store.hospitals,
    updatedAt:         store.updatedAt,
    last_updated:      store.updatedAt,
    isCached:          store.isCached,
    dataAge:           isFinite(age) ? Math.round(age) : null,
    connection_status: age > STALE_LIMIT_SEC
      ? 'disconnected'
      : age > STALE_WARN_SEC
        ? 'stale'
        : 'ok',
  };
}

function broadcastSSE(eventType) {
  if (sseClients.size === 0) return;
  const data = JSON.stringify(buildSSEPayload(eventType));
  const msg  = `event: hospitals\ndata: ${data}\n\n`;
  for (const client of sseClients) {
    try { client.write(msg); } catch (_) { sseClients.delete(client); }
  }
}

// ──────────────────────────────────────────────
// CORS 헤더
// ──────────────────────────────────────────────
function setCORS(res) {
  res.setHeader('Access-Control-Allow-Origin',  CORS_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

// ──────────────────────────────────────────────
// JSON 응답 헬퍼
// ──────────────────────────────────────────────
function sendJSON(res, statusCode, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(statusCode, {
    'Content-Type':  'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(body);
}

// ──────────────────────────────────────────────
// 라우팅
// ──────────────────────────────────────────────
function handleHospitals(req, res, parsedUrl) {
  const q      = parsedUrl.query;
  const lat    = parseFloat(q.lat);
  const lng    = parseFloat(q.lng);
  const radius = parseFloat(q.radius) || 50; // km, 기본 50km

  if (isNaN(lat) || isNaN(lng)) {
    return sendJSON(res, 400, { error: 'lat, lng 파라미터가 필요합니다.' });
  }

  // 캐시 30분 초과 → 서비스 불가
  const age = dataAgeSec();
  if (age > STALE_LIMIT_SEC && store.hospitals.length === 0) {
    return sendJSON(res, 503, {
      error: '데이터를 사용할 수 없습니다. 잠시 후 다시 시도해주세요.',
      lastFetch: store.lastFetch,
    });
  }

  // 거리 필터링 + 계산
  const hospitals = store.hospitals
    .map(h => {
      const dist = haversine(lat, lng, h.lat, h.lng);
      return { ...h, distance: Math.round(dist * 10) / 10 };
    })
    .filter(h => h.distance <= radius)
    .sort((a, b) => a.distance - b.distance);

  sendJSON(res, 200, {
    hospitals,
    updatedAt:         store.updatedAt,
    isCached:          store.isCached,
    dataAge:           isFinite(age) ? Math.round(age) : null,
    connection_status: age > STALE_LIMIT_SEC
      ? 'disconnected'
      : age > STALE_WARN_SEC
        ? 'stale'
        : 'ok',
  });
}

function handleStream(req, res) {
  // SSE 핸드셰이크
  res.writeHead(200, {
    'Content-Type':  'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache',
    'Connection':    'keep-alive',
    'X-Accel-Buffering': 'no',  // Nginx 버퍼링 비활성화
    'Access-Control-Allow-Origin': CORS_ORIGIN,
  });

  // 즉시 현재 상태 전송 (연결 즉시 데이터 수신)
  const init = JSON.stringify(buildSSEPayload('full_update'));
  res.write(`event: hospitals\ndata: ${init}\n\n`);

  // 30초 heartbeat (SSE 연결 유지 — 프록시 타임아웃 방지)
  const heartbeat = setInterval(() => {
    try { res.write(': heartbeat\n\n'); } catch (_) { /* 연결 끊김 */ }
  }, 30_000);

  sseClients.add(res);

  req.on('close', () => {
    sseClients.delete(res);
    clearInterval(heartbeat);
  });
}

function handleHealth(req, res) {
  const age = dataAgeSec();
  sendJSON(res, 200, {
    status:        store.hospitals.length > 0 ? 'ok' : 'no_data',
    mode:          EGEN_API_KEY ? 'real' : 'demo',
    lastFetch:     store.lastFetch,
    updatedAt:     store.updatedAt,
    dataAge:       isFinite(age) ? Math.round(age) : null,
    isCacheStale:  age > STALE_WARN_SEC,
    isServiceable: store.hospitals.length > 0 && age <= STALE_LIMIT_SEC,
    hospitalCount: store.hospitals.length,
    sseClients:    sseClients.size,
    fetchError:    store.fetchError,
  });
}

// ──────────────────────────────────────────────
// HTTP 서버
// ──────────────────────────────────────────────
const server = http.createServer((req, res) => {
  setCORS(res);

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsed = url.parse(req.url, true);
  const path   = parsed.pathname;

  if (req.method === 'GET' && path === '/api/hospitals') {
    return handleHospitals(req, res, parsed);
  }
  if (req.method === 'GET' && path === '/api/stream') {
    return handleStream(req, res);
  }
  if (req.method === 'GET' && path === '/health') {
    return handleHealth(req, res);
  }

  // 정적 파일 서빙 (public/)
  if (req.method === 'GET') {
    const safePath = (path === '/' || path === '') ? '/index.html' : path;
    const filePath = nodePath.join(__dirname, 'public', safePath);
    if (!filePath.startsWith(nodePath.join(__dirname, 'public'))) {
      return sendJSON(res, 403, { error: 'Forbidden' });
    }
    const mimeMap = {
      '.html': 'text/html; charset=utf-8',
      '.css':  'text/css; charset=utf-8',
      '.js':   'application/javascript; charset=utf-8',
      '.json': 'application/json',
      '.png':  'image/png',
      '.svg':  'image/svg+xml',
      '.ico':  'image/x-icon',
    };
    const mime = mimeMap[nodePath.extname(filePath)] || 'application/octet-stream';
    fs.readFile(filePath, (err, content) => {
      if (err) return sendJSON(res, 404, { error: 'Not Found' });
      res.writeHead(200, { 'Content-Type': mime, 'Cache-Control': 'no-cache' });
      res.end(content);
    });
    return;
  }

  sendJSON(res, 404, { error: 'Not Found' });
});

// ──────────────────────────────────────────────
// 기동
// ──────────────────────────────────────────────
server.listen(PORT, async () => {
  console.log(`[server] 포트 ${PORT} 기동 — 모드: ${EGEN_API_KEY ? '실 e-GEN API' : '데모(시뮬레이션)'}`);

  // 초기 데이터 즉시 로드
  await poll();
  console.log(`[server] 초기 폴링 완료 — 병원 ${store.hospitals.length}개 로드`);

  // 주기적 폴링 시작
  const interval = EGEN_API_KEY ? REAL_POLL_MS : DEMO_POLL_MS;
  setInterval(poll, interval);
  console.log(`[server] 폴링 주기: ${interval / 1000}초`);
});

// 예상치 못한 예외 — 서버는 유지, 로그만 기록
process.on('uncaughtException', err => {
  console.error('[uncaughtException]', err.message);
});
process.on('unhandledRejection', (reason) => {
  console.error('[unhandledRejection]', reason);
});

module.exports = server; // 테스트 용이성
