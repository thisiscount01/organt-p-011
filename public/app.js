/**
 * 응급실 실시간 현황 — app.js
 * 구조:
 *   1. 상수
 *   2. 앱 상태
 *   3. DOM 참조
 *   4. 병상 상태 판정
 *   5. 위치 요청
 *   6. REST API 호출
 *   7. SSE 스트림 관리
 *   8. 데이터 정규화 / 상태 머신 갱신
 *   9. 렌더링 (dirty-flag 기반)
 *  10. 배너 / 신선도 표시
 *  11. 뷰 전환
 *  12. 이벤트 리스너 + 초기화
 */
'use strict';

/* ============================================================
   1. 상수
   ============================================================ */
const DEFAULTS = {
  RADIUS_KM:           10,
  FRESHNESS_MS:   30_000,   // freshness 표시 갱신 주기
  STALE_THRESHOLD: 5 * 60 * 1000,  // 5분 — 이 이상 미갱신 시 stale
  SSE_BASE_DELAY:  1_000,
  SSE_MAX_DELAY:  30_000,
  GEO_TIMEOUT:    12_000,
  GEO_MAX_AGE:    60_000,
  HEALTH_POLL_MS: 2 * 60 * 1000,   // health 폴링 주기
};

/** 병상 상태별 표시 설정 */
const BED_STATUS_CONFIG = {
  available: {
    cssCard:   'card--available',
    cssBadge:  'status-badge--available',
    icon:      '✓',
    label:     '여유',
    ariaLabel: '병상 여유',
  },
  low: {
    cssCard:   'card--low',
    cssBadge:  'status-badge--low',
    icon:      '▲',
    label:     '부족',
    ariaLabel: '병상 부족',
  },
  full: {
    cssCard:   'card--full',
    cssBadge:  'status-badge--full',
    icon:      '✕',
    label:     '만원',
    ariaLabel: '병상 만원',
  },
  unknown: {
    cssCard:   'card--unknown',
    cssBadge:  'status-badge--unknown',
    icon:      '?',
    label:     '정보없음',
    ariaLabel: '병상 정보 없음',
  },
};

/* ============================================================
   2. 앱 상태
   ============================================================ */
const state = {
  coords:           null,   // { lat, lng }
  radius:           DEFAULTS.RADIUS_KM,
  hospitalsMap:     new Map(),  // id → hospital 객체 (dirty-flag 비교용)
  lastUpdated:      null,   // Date
  isCached:         false,
  isCacheStale:     false,
  connectionStatus: 'unknown',  // 'ok' | 'degraded' | 'unknown'
  sseState:         'idle',     // 'idle'|'connecting'|'open'|'reconnecting'
  sseRetryCount:    0,
  sseRetryTimer:    null,
  sse:              null,   // EventSource 인스턴스
  freshnessTimer:   null,
  healthTimer:      null,
  currentPanel:     'loading',
};

/* ============================================================
   3. DOM 참조 (DOMContentLoaded 이후 초기화)
   ============================================================ */
let DOM = {};

function initDOM() {
  DOM = {
    // 알림 배너
    alertStale:            qs('alertStale'),
    alertStaleMsg:         qs('alertStaleMsg'),
    alertDegraded:         qs('alertDegraded'),
    alertDisconnected:     qs('alertDisconnected'),
    alertDisconnectedMsg:  qs('alertDisconnectedMsg'),
    // 연결 위젯
    connDot:               qs('connDot'),
    connLabel:             qs('connLabel'),
    // 패널
    panelLoading:          qs('panelLoading'),
    panelResults:          qs('panelResults'),
    panelError:            qs('panelError'),
    panelLocationDenied:   qs('panelLocationDenied'),
    // 결과 내부
    metaCount:             qs('metaCount'),
    metaFresh:             qs('metaFresh'),
    hospitalList:          qs('hospitalList'),
    skeletonList:          qs('skeletonList'),
    emptyState:            qs('emptyState'),
    emptyRadiusText:       qs('emptyRadiusText'),
    // 오류
    errMsg:                qs('errMsg'),
    // 버튼
    btnRetry:              qs('btnRetry'),
    btnRetryLocation:      qs('btnRetryLocation'),
    btnExpandRadius:       qs('btnExpandRadius'),
    // FAB
    fabWrap:               qs('fabWrap'),
    radiusSel:             qs('radiusSel'),
  };
}

function qs(id) { return document.getElementById(id); }

/* ============================================================
   4. 병상 상태 판정 — spec 기반 우선순위 정확 구현
   ============================================================ */
/**
 * @param {{ available: boolean, beds: { total: number, available: number|null } }} hospital
 * @returns {'available'|'low'|'full'|'unknown'}
 */
function getBedStatus(hospital) {
  if (hospital.available === false) return 'full';
  const bedsAvail = hospital.beds?.available;
  if (bedsAvail === null || bedsAvail === undefined) return 'unknown';
  return bedsAvail >= 5 ? 'available' : 'low';
}

/* ============================================================
   5. 위치 요청
   ============================================================ */
function requestLocation() {
  return new Promise((resolve, reject) => {
    if (!navigator.geolocation) {
      reject(new Error('이 브라우저는 위치 서비스를 지원하지 않습니다.'));
      return;
    }
    navigator.geolocation.getCurrentPosition(
      pos => resolve({ lat: pos.coords.latitude, lng: pos.coords.longitude }),
      err => {
        const msgs = {
          1: '위치 접근 권한이 거부되었습니다. 브라우저 설정에서 위치를 허용해 주세요.',
          2: '현재 위치를 파악할 수 없습니다. 잠시 후 다시 시도해 주세요.',
          3: '위치 파악이 시간 초과되었습니다.',
        };
        reject(new Error(msgs[err.code] || '위치를 파악하는 데 실패했습니다.'));
      },
      { timeout: DEFAULTS.GEO_TIMEOUT, maximumAge: DEFAULTS.GEO_MAX_AGE, enableHighAccuracy: false }
    );
  });
}

/* ============================================================
   6. REST API 호출
   ============================================================ */
async function fetchHospitals(coords, radius) {
  const url = `/api/hospitals?lat=${coords.lat}&lng=${coords.lng}&radius=${radius}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15_000);
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) throw new Error(`서버 오류 ${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchHealth() {
  try {
    const res = await fetch('/health');
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

/* ============================================================
   7. SSE 스트림 관리
   ============================================================ */
function connectSSE() {
  // 기존 연결/타이머 정리
  if (state.sseRetryTimer) {
    clearTimeout(state.sseRetryTimer);
    state.sseRetryTimer = null;
  }
  if (state.sse) {
    state.sse.close();
    state.sse = null;
  }

  state.sseState = state.sseRetryCount === 0 ? 'connecting' : 'reconnecting';
  updateConnWidget();

  const sse = new EventSource('/api/stream');
  state.sse = sse;

  sse.addEventListener('hospitals', e => {
    try {
      const data = JSON.parse(e.data);
      applyServerData(data);
    } catch (parseErr) {
      console.error('[SSE] JSON parse 오류:', parseErr);
    }
  });

  sse.onopen = () => {
    state.sseState = 'open';
    state.sseRetryCount = 0;
    updateConnWidget();
    updateBanners();
  };

  sse.onerror = () => {
    sse.close();
    state.sse = null;
    state.sseState = 'reconnecting';
    updateConnWidget();

    const delay = Math.min(
      DEFAULTS.SSE_BASE_DELAY * (2 ** state.sseRetryCount),
      DEFAULTS.SSE_MAX_DELAY
    );
    state.sseRetryCount += 1;

    updateBanners(); // 재연결 중 배너 표시

    state.sseRetryTimer = setTimeout(() => {
      if (document.visibilityState === 'visible') {
        connectSSE();
      }
    }, delay);
  };
}

function disconnectSSE() {
  if (state.sseRetryTimer) { clearTimeout(state.sseRetryTimer); state.sseRetryTimer = null; }
  if (state.sse) { state.sse.close(); state.sse = null; }
  state.sseState = 'idle';
}

/* ============================================================
   8. 데이터 정규화 / 상태 갱신
   ============================================================ */
/**
 * REST API 응답과 SSE 페이로드를 모두 처리하는 단일 진입점.
 * REST:  { hospitals[], updatedAt, isCached, connection_status }
 * SSE:   { event_type, hospitals[], isCached, connection_status, last_updated }
 */
function applyServerData(raw) {
  // 필드 정규화
  const hospitals        = raw.hospitals || [];
  const isCached         = !!raw.isCached;
  const connectionStatus = raw.connection_status || 'unknown';
  const rawTs            = raw.last_updated || raw.updatedAt || null;
  const eventType        = raw.event_type || 'full_replace';

  state.isCached         = isCached;
  state.connectionStatus = connectionStatus;
  state.lastUpdated      = rawTs ? new Date(rawTs) : new Date();

  // 병원 목록 갱신 (dirty-flag)
  if (eventType === 'patch') {
    // 차등 업데이트: 변경된 항목만 교체
    hospitals.forEach(h => {
      const existing = state.hospitalsMap.get(h.id);
      if (!existing || JSON.stringify(existing) !== JSON.stringify(h)) {
        updateHospitalCard(h);
        state.hospitalsMap.set(h.id, h);
      }
    });
  } else {
    // 전체 교체 (full_replace)
    const incoming = new Map(hospitals.map(h => [h.id, h]));

    // 없어진 병원 카드 제거
    state.hospitalsMap.forEach((_, id) => {
      if (!incoming.has(id)) {
        document.getElementById(`hcard-${id}`)?.remove();
        state.hospitalsMap.delete(id);
      }
    });

    // 변경된 병원 갱신 / 신규 삽입
    hospitals.forEach(h => {
      const existing = state.hospitalsMap.get(h.id);
      if (!existing || JSON.stringify(existing) !== JSON.stringify(h)) {
        updateHospitalCard(h);
        state.hospitalsMap.set(h.id, h);
      }
    });
  }

  // 결과 뷰 전환 (아직 로딩 중이었다면)
  if (state.currentPanel === 'loading') showPanel('results');

  updateMeta();
  updateBanners();
  updateFreshness();
}

/* ============================================================
   9. 렌더링 (dirty-flag 기반)
   ============================================================ */

/** 병원 카드 DOM 생성 */
function createHospitalCard(hospital) {
  const status = getBedStatus(hospital);
  const cfg    = BED_STATUS_CONFIG[status];

  const li = document.createElement('li');
  li.id = `hcard-${hospital.id}`;
  li.className = `hospital-card ${cfg.cssCard}`;
  li.setAttribute('role', 'listitem');
  li.dataset.distance = hospital.distance ?? '999999';

  // 전화번호 정리
  const rawPhone  = hospital.phone || '';
  const cleanPhone = rawPhone.replace(/[^0-9+]/g, '');
  const hasPhone  = cleanPhone.length > 0;

  // 병상 표시 계산
  const bedsHtml = buildBedsHtml(hospital, cfg);

  li.innerHTML = `
    <div class="card-body">
      <div class="card-header">
        <div class="card-name-wrap">
          <span class="card-name" title="${esc(hospital.name)}">${esc(hospital.name)}</span>
          ${hospital.distance != null
            ? `<span class="card-distance">${fmtDist(hospital.distance)}</span>`
            : ''
          }
        </div>
        <div class="status-badge ${cfg.cssBadge}"
             aria-label="${cfg.ariaLabel}${hospital.beds?.available != null ? ` (${hospital.beds.available}개)` : ''}">
          <span class="badge-shape" aria-hidden="true">${cfg.icon}</span>
          <span>${cfg.label}</span>
        </div>
      </div>

      ${bedsHtml}

      ${hospital.address
        ? `<div class="card-address" title="${esc(hospital.address)}">${esc(hospital.address)}</div>`
        : ''
      }
    </div>

    <div class="card-actions">
      ${hasPhone
        ? `<a href="tel:${cleanPhone}"
              class="btn-call"
              aria-label="${esc(hospital.name)}에 전화 걸기 ${esc(rawPhone)}">
             <svg class="call-icon-svg" viewBox="0 0 24 24" fill="none"
                  stroke="currentColor" stroke-width="2.5"
                  stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
               <path d="M22 16.92v3a2 2 0 01-2.18 2
                        c-3.54-.46-6.9-1.87-9.76-4.12
                        a20 20 0 01-4.27-4.27
                        C3.54 10.88 2.13 7.52 1.68 3.98
                        A2 2 0 013.68 2h3a2 2 0 012 1.72
                        c.13 1.01.37 2 .72 2.94a2 2 0 01-.45 2.11L7.91 9.91
                        a16 16 0 006.18 6.18l1.14-1.06a2 2 0 012.11-.45
                        c.94.35 1.93.59 2.94.72A2 2 0 0122 16.92z"/>
             </svg>
             <span>${esc(rawPhone)}</span>
           </a>`
        : `<div class="btn-call btn-call--disabled" aria-label="전화번호 없음">
             <span>전화번호 정보 없음</span>
           </div>`
      }
    </div>
  `;

  return li;
}

function buildBedsHtml(hospital, cfg) {
  const beds  = hospital.beds;
  const avail = beds?.available;
  const total = beds?.total;

  if (avail === null || avail === undefined) {
    return `<div class="beds-section">
              <div class="beds-row">
                <span class="beds-label">병상 현황</span>
                <span class="beds-count">정보 없음</span>
              </div>
            </div>`;
  }

  const pct     = total > 0 ? Math.round((avail / total) * 100) : 0;
  const safeTotal = total ?? '?';

  return `
    <div class="beds-section" aria-label="병상 현황 ${avail}개 가용 / 전체 ${safeTotal}개">
      <div class="beds-row">
        <span class="beds-label">가용 병상</span>
        <span class="beds-count"><strong>${avail}</strong> / ${safeTotal}</span>
      </div>
      <div class="beds-bar-wrap" role="progressbar"
           aria-valuenow="${avail}" aria-valuemin="0" aria-valuemax="${total || 1}"
           aria-label="병상 여유 ${pct}%">
        <div class="beds-bar-fill" style="width:${pct}%"></div>
      </div>
    </div>
  `;
}

/** 카드 삽입/교체 (거리순 정렬 유지) */
function updateHospitalCard(hospital) {
  const existing = document.getElementById(`hcard-${hospital.id}`);
  const newCard  = createHospitalCard(hospital);

  if (existing) {
    // 깜빡임 애니메이션으로 갱신 신호
    existing.classList.remove('hospital-card--updated');
    void existing.offsetWidth; // reflow
    existing.replaceWith(newCard);
    newCard.classList.add('hospital-card--updated');
  } else {
    // 거리순으로 적절한 위치에 삽입
    insertCardSorted(newCard, hospital.distance ?? Infinity);
  }
}

function insertCardSorted(card, distance) {
  const list  = DOM.hospitalList;
  const items = Array.from(list.children);
  let insertBefore = null;

  for (const item of items) {
    if (parseFloat(item.dataset.distance ?? 'Infinity') > distance) {
      insertBefore = item;
      break;
    }
  }

  if (insertBefore) {
    list.insertBefore(card, insertBefore);
  } else {
    list.appendChild(card);
  }
}

/** 스켈레톤 카드 N개 주입 */
function renderSkeletons(count = 5) {
  DOM.skeletonList.innerHTML = '';
  for (let i = 0; i < count; i++) {
    const li = document.createElement('li');
    li.className = 'hospital-card hospital-card--skeleton';
    li.setAttribute('aria-hidden', 'true');
    li.innerHTML = `
      <div class="skel-line" style="width:62%;height:18px;"></div>
      <div class="skel-line mt-4" style="width:28%;height:13px;"></div>
      <div class="skel-line mt-4" style="width:85%;height:11px;"></div>
      <div class="skel-line mt-4" style="width:100%;height:52px;border-radius:12px;"></div>
    `;
    DOM.skeletonList.appendChild(li);
  }
}

/** 초기 전체 목록 렌더 (거리순 정렬 이미 된 배열 가정) */
function renderFullList(hospitals) {
  DOM.hospitalList.innerHTML = '';
  hospitals.forEach(h => {
    const card = createHospitalCard(h);
    DOM.hospitalList.appendChild(card);
  });
}

/* ============================================================
   10. 배너 / 신선도 표시
   ============================================================ */
function isDataStale() {
  if (!state.lastUpdated) return false;
  return Date.now() - state.lastUpdated.getTime() > DEFAULTS.STALE_THRESHOLD;
}

function updateBanners() {
  // ① 신선도 경고 (캐시 or 5분 미갱신)
  const stale = state.isCacheStale || isDataStale();
  DOM.alertStale.hidden = !stale;
  if (stale && state.lastUpdated) {
    DOM.alertStaleMsg.textContent =
      `${relativeTime(state.lastUpdated)} 기준 데이터입니다. 긴급한 경우 병원에 직접 전화하세요.`;
  }

  // ② e-GEN 연결 불안정
  DOM.alertDegraded.hidden = state.connectionStatus !== 'degraded';

  // ③ SSE 스트림 단절
  const disconnected = (state.sseState === 'reconnecting');
  DOM.alertDisconnected.hidden = !disconnected;
  if (disconnected) {
    const nextDelay = Math.min(
      DEFAULTS.SSE_BASE_DELAY * (2 ** Math.max(0, state.sseRetryCount - 1)),
      DEFAULTS.SSE_MAX_DELAY
    );
    DOM.alertDisconnectedMsg.textContent =
      `실시간 스트림 연결 끊김 — ${Math.ceil(nextDelay / 1000)}초 후 재연결 시도 중...`;
  }
}

function startFreshnessTimer() {
  if (state.freshnessTimer) clearInterval(state.freshnessTimer);
  state.freshnessTimer = setInterval(() => {
    updateFreshness();
    if (isDataStale()) updateBanners();
  }, DEFAULTS.FRESHNESS_MS);
}

function updateFreshness() {
  if (!state.lastUpdated) { DOM.metaFresh.textContent = ''; return; }
  const rel   = relativeTime(state.lastUpdated);
  const stale = isDataStale();
  DOM.metaFresh.textContent = `${rel} 업데이트${stale ? ' (갱신 지연)' : ''}`;
  DOM.metaFresh.className   = `meta-fresh${stale ? ' meta-fresh--stale' : ''}`;
}

function relativeTime(date) {
  const diffSec = Math.floor((Date.now() - date.getTime()) / 1000);
  if (diffSec <  10) return '방금 전';
  if (diffSec <  60) return `${diffSec}초 전`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}분 전`;
  return `${Math.floor(diffSec / 3600)}시간 전`;
}

function updateMeta() {
  const count = state.hospitalsMap.size;
  DOM.metaCount.textContent = `주변 응급실 ${count}곳 (${state.radius}km 이내)`;
  DOM.emptyState.hidden = count > 0;
  if (DOM.emptyRadiusText) DOM.emptyRadiusText.textContent = `${state.radius}km`;
}

/* ============================================================
   연결 위젯
   ============================================================ */
function updateConnWidget() {
  const dot   = DOM.connDot;
  const label = DOM.connLabel;

  dot.className = 'conn-dot'; // 초기화

  switch (state.sseState) {
    case 'open':
      dot.classList.add('conn-dot--green');
      label.textContent = '실시간';
      break;
    case 'connecting':
      dot.classList.add('conn-dot--yellow', 'conn-dot--pulse');
      label.textContent = '연결 중';
      break;
    case 'reconnecting':
      dot.classList.add('conn-dot--yellow', 'conn-dot--pulse');
      label.textContent = '재연결 중';
      break;
    default:
      dot.classList.add('conn-dot--red');
      label.textContent = '연결 끊김';
  }
}

/* ============================================================
   Health 폴링 (isCacheStale 감지)
   ============================================================ */
function startHealthPoll() {
  if (state.healthTimer) clearInterval(state.healthTimer);
  pollHealth();
  state.healthTimer = setInterval(pollHealth, DEFAULTS.HEALTH_POLL_MS);
}

async function pollHealth() {
  const data = await fetchHealth();
  if (!data) return;
  const prevStale = state.isCacheStale;
  state.isCacheStale = !!data.isCacheStale;
  if (state.isCacheStale !== prevStale) updateBanners();
}

/* ============================================================
   11. 뷰 전환
   ============================================================ */
const PANELS = ['panelLoading', 'panelResults', 'panelError', 'panelLocationDenied'];

function showPanel(name) {
  state.currentPanel = name;
  const panelKey = {
    loading:       'panelLoading',
    results:       'panelResults',
    error:         'panelError',
    locationDenied:'panelLocationDenied',
  }[name] || name;

  PANELS.forEach(p => {
    const el = DOM[p];
    if (!el) return;
    el.hidden = (p !== panelKey);
  });

  DOM.fabWrap.hidden = (name !== 'results');
}

function showError(message) {
  DOM.errMsg.textContent = message;
  showPanel('error');
}

/* ============================================================
   12. 이벤트 리스너 + 초기화
   ============================================================ */

/** 위치 획득 → API 호출 → SSE 연결 */
async function boot() {
  renderSkeletons(5);
  showPanel('loading');

  let coords;
  try {
    coords = await requestLocation();
    state.coords = coords;
  } catch (err) {
    // 위치 거부 → fallback 안내
    showPanel('locationDenied');
    return;
  }

  await loadHospitals(coords);
}

async function loadHospitals(coords) {
  state.coords = coords;

  // 기존 SSE 및 데이터 정리
  disconnectSSE();
  state.hospitalsMap.clear();
  DOM.hospitalList.innerHTML = '';

  renderSkeletons(5);
  showPanel('loading');

  try {
    const data = await fetchHospitals(coords, state.radius);

    // 정규화 + 상태 업데이트
    const hospitals        = data.hospitals || [];
    state.isCached         = !!data.isCached;
    state.connectionStatus = data.connection_status || 'unknown';
    state.lastUpdated      = data.updatedAt ? new Date(data.updatedAt) : new Date();

    // 초기 전체 렌더
    hospitals.forEach(h => state.hospitalsMap.set(h.id, h));
    renderFullList(hospitals);

    showPanel('results');
    updateMeta();
    updateBanners();
    updateFreshness();

    // SSE 스트림 시작
    connectSSE();
    startFreshnessTimer();
    startHealthPoll();

  } catch (err) {
    const msg = err.name === 'AbortError'
      ? '서버 응답이 너무 느립니다. 잠시 후 다시 시도해 주세요.'
      : `데이터를 불러오지 못했습니다: ${err.message}`;
    showError(msg);
  }
}

/** visibilitychange: 백그라운드 진입 시 SSE 종료, 포그라운드 복귀 시 재연결 */
function handleVisibilityChange() {
  if (document.visibilityState === 'hidden') {
    // 모바일 배터리 절감: 백그라운드에서 SSE 종료
    disconnectSSE();
  } else {
    // 포그라운드 복귀: SSE 재연결 (백오프 리셋)
    if (state.coords && state.sseState !== 'open' && state.sseState !== 'connecting') {
      state.sseRetryCount = 0;
      connectSSE();
    }
  }
}

/* ── 유틸 ── */
function esc(str) {
  if (!str) return '';
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function fmtDist(km) {
  if (km == null) return '';
  if (km < 1) return `${Math.round(km * 1000)}m`;
  return `${km.toFixed(1)}km`;
}

/* ── DOMContentLoaded ── */
document.addEventListener('DOMContentLoaded', () => {
  initDOM();

  // 버튼 이벤트
  DOM.btnRetry?.addEventListener('click', () => {
    if (state.coords) loadHospitals(state.coords);
    else boot();
  });

  DOM.btnRetryLocation?.addEventListener('click', () => boot());

  DOM.btnExpandRadius?.addEventListener('click', () => {
    state.radius = 50;
    if (DOM.radiusSel) DOM.radiusSel.value = '50';
    if (state.coords) loadHospitals(state.coords);
  });

  // 반경 선택기
  DOM.radiusSel?.addEventListener('change', e => {
    state.radius = parseInt(e.target.value, 10);
    if (state.coords) loadHospitals(state.coords);
  });

  // 도시 버튼 (위치 거부 시 fallback)
  document.querySelectorAll('.city-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const lat = parseFloat(btn.dataset.lat);
      const lng = parseFloat(btn.dataset.lng);
      loadHospitals({ lat, lng });
    });
  });

  // visibilitychange
  document.addEventListener('visibilitychange', handleVisibilityChange);

  // 앱 시작
  boot();
});

/* ── Service Worker 등록 (PWA) ── */
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/sw.js').catch(() => {
      // SW 미지원 환경에서도 앱은 정상 동작
    });
  });
}
