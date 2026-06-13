/**
 * 응급실 현황 Service Worker — PWA 오프라인 지원
 * 전략:
 *   - App Shell (HTML/CSS/JS): Cache First → network fallback
 *   - /api/hospitals:          Network First → cache fallback (마지막 성공 응답 보존)
 *   - /api/stream (SSE):       통과 (인터셉트하지 않음)
 *   - /health:                 Network First
 */
'use strict';

const CACHE_VER   = 'er-v1';
const SHELL_URLS  = ['/', '/style.css', '/app.js', '/manifest.json'];

/* ── Install ── */
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE_VER)
      .then(c => c.addAll(SHELL_URLS))
      .then(() => self.skipWaiting())
  );
});

/* ── Activate ── */
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => k !== CACHE_VER).map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

/* ── Fetch ── */
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // SSE 스트림은 절대 인터셉트하지 않음
  if (url.pathname === '/api/stream') return;

  // API 엔드포인트: Network First → Cache Fallback
  if (url.pathname.startsWith('/api/') || url.pathname === '/health') {
    e.respondWith(networkFirstWithCache(e.request));
    return;
  }

  // App Shell: Cache First → Network
  e.respondWith(cacheFirstWithNetwork(e.request));
});

async function networkFirstWithCache(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_VER);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    if (cached) return cached;
    return new Response(
      JSON.stringify({ error: '오프라인 상태입니다. 캐시된 데이터가 없습니다.' }),
      { status: 503, headers: { 'Content-Type': 'application/json' } }
    );
  }
}

async function cacheFirstWithNetwork(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_VER);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    return new Response('오프라인 상태입니다.', { status: 503 });
  }
}
