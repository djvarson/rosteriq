/**
 * RosterIQ Service Worker
 *
 * Provides offline support with intelligent caching strategy:
 * - Network-first for HTML pages (stale-while-revalidate)
 * - Cache-first for static assets (CSS, JS, images)
 * - Network-only for API calls (never cache API responses)
 * - Background sync for failed POST requests
 */

const CACHE_NAME = 'rosteriq-v1';
const CRITICAL_ASSETS = [
  '/static/dashboard.html',
  '/static/staff.html',
  '/static/admin.html',
  '/static/manifest.json',
  '/static/offline.html',
];

// ============================================================================
// Install Event: Pre-cache critical assets
// ============================================================================

self.addEventListener('install', event => {
  console.log('[ServiceWorker] Install event');

  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      console.log('[ServiceWorker] Caching critical assets');
      return cache.addAll(CRITICAL_ASSETS).catch(err => {
        console.warn('[ServiceWorker] Error caching critical assets:', err);
      });
    })
  );

  // Activate immediately without waiting
  self.skipWaiting();
});

// ============================================================================
// Activate Event: Clean up old cache versions
// ============================================================================

self.addEventListener('activate', event => {
  console.log('[ServiceWorker] Activate event');

  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames
          .filter(name => name.startsWith('rosteriq-') && name !== CACHE_NAME)
          .map(name => {
            console.log('[ServiceWorker] Deleting old cache:', name);
            return caches.delete(name);
          })
      );
    })
  );

  // Take control of all pages immediately
  self.clients.claim();
});

// ============================================================================
// Fetch Event: Intelligent caching strategy
// ============================================================================

self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // 1. Auth endpoints — network-only, never cache
  if (url.pathname.startsWith('/api/auth/')) {
    return event.respondWith(
      fetch(request)
        .catch(err => {
          console.warn('[ServiceWorker] Auth request failed:', err);
          return new Response(
            JSON.stringify({ error: 'Auth request failed offline' }),
            { status: 503, headers: { 'Content-Type': 'application/json' } }
          );
        })
    );
  }

  // 2. API calls — network-only (never cache API responses)
  if (url.pathname.startsWith('/api/')) {
    return event.respondWith(
      fetch(request)
        .then(response => {
          // Don't cache API responses
          return response;
        })
        .catch(err => {
          console.warn('[ServiceWorker] API request failed:', err);

          // Check if there's a pending request to retry
          if (request.method === 'POST' && !navigator.onLine) {
            // Queue for background sync (if available)
            if (self.registration && self.registration.sync) {
              self.registration.sync.register('retry-failed-requests');
            }
          }

          return new Response(
            JSON.stringify({ error: 'Request failed offline' }),
            { status: 503, headers: { 'Content-Type': 'application/json' } }
          );
        })
    );
  }

  // 3. HTML pages — network-first with cache fallback (stale-while-revalidate)
  if (request.headers.get('accept')?.includes('text/html')) {
    return event.respondWith(
      fetch(request)
        .then(response => {
          if (!response || response.status !== 200) {
            return getCachedOrOfflinePage(request);
          }

          // Cache successful HTML responses
          const clonedResponse = response.clone();
          caches.open(CACHE_NAME).then(cache => {
            cache.put(request, clonedResponse);
          });

          return response;
        })
        .catch(err => {
          console.warn('[ServiceWorker] HTML request failed:', err);
          return getCachedOrOfflinePage(request);
        })
    );
  }

  // 4. Static assets (CSS, JS, images) — cache-first
  return event.respondWith(
    caches.match(request).then(cached => {
      if (cached) {
        return cached;
      }

      return fetch(request).then(response => {
        if (!response || response.status !== 200) {
          return response;
        }

        // Cache successful responses
        const clonedResponse = response.clone();
        caches.open(CACHE_NAME).then(cache => {
          cache.put(request, clonedResponse);
        });

        return response;
      }).catch(err => {
        console.warn('[ServiceWorker] Asset request failed:', err);
        return new Response('Asset not available offline', { status: 503 });
      });
    })
  );
});

// ============================================================================
// Helper: Get cached page or offline page
// ============================================================================

async function getCachedOrOfflinePage(request) {
  const cache = await caches.open(CACHE_NAME);

  // Try to get cached version of requested page
  const cached = await cache.match(request);
  if (cached) {
    return cached;
  }

  // Fall back to offline page
  const offlineResponse = await cache.match('/static/offline.html');
  if (offlineResponse) {
    return offlineResponse;
  }

  // Last resort: generic offline response
  return new Response(
    'You are offline. Please check your connection and try again.',
    {
      status: 503,
      headers: { 'Content-Type': 'text/plain' }
    }
  );
}

// ============================================================================
// Message Handler: Cache invalidation from main thread
// ============================================================================

self.addEventListener('message', event => {
  const { type, data } = event.data;

  if (type === 'CLEAR_CACHE') {
    console.log('[ServiceWorker] Clearing cache:', data);
    caches.delete(CACHE_NAME).then(() => {
      console.log('[ServiceWorker] Cache cleared');
      event.ports[0].postMessage({ success: true });
    });
  }

  if (type === 'CACHE_URLS') {
    console.log('[ServiceWorker] Caching URLs:', data);
    caches.open(CACHE_NAME).then(cache => {
      cache.addAll(data).then(() => {
        console.log('[ServiceWorker] URLs cached');
        event.ports[0].postMessage({ success: true });
      }).catch(err => {
        console.warn('[ServiceWorker] Error caching URLs:', err);
        event.ports[0].postMessage({ success: false, error: err.message });
      });
    });
  }

  if (type === 'INVALIDATE_CACHE') {
    console.log('[ServiceWorker] Invalidating cache');
    caches.delete(CACHE_NAME).then(() => {
      caches.open(CACHE_NAME).then(cache => {
        cache.addAll(CRITICAL_ASSETS);
        event.ports[0].postMessage({ success: true });
      });
    });
  }
});

// ============================================================================
// Background Sync: Retry failed requests when online
// ============================================================================

self.addEventListener('sync', event => {
  if (event.tag === 'retry-failed-requests') {
    console.log('[ServiceWorker] Background sync triggered');
    event.waitUntil(retryFailedRequests());
  }
});

async function retryFailedRequests() {
  // Open the pending requests store
  const db = await openPendingRequestsDB();
  const requests = await getPendingRequests(db);

  if (requests.length === 0) {
    return;
  }

  for (const req of requests) {
    try {
      const response = await fetch(req.url, {
        method: req.method,
        headers: req.headers,
        body: req.body,
      });

      if (response.ok) {
        await deletePendingRequest(db, req.id);
        console.log('[ServiceWorker] Retry successful for:', req.url);

        // Notify all clients of success
        const clients = await self.clients.matchAll();
        clients.forEach(client => {
          client.postMessage({
            type: 'REQUEST_RETRY_SUCCESS',
            url: req.url,
          });
        });
      }
    } catch (err) {
      console.warn('[ServiceWorker] Retry failed for:', req.url, err);
    }
  }
}

// Simple in-memory storage for pending requests (in production, use IndexedDB)
let pendingRequests = [];

async function openPendingRequestsDB() {
  return {
    add: (req) => {
      pendingRequests.push({ id: Math.random(), ...req });
    },
    get: () => pendingRequests,
    delete: (id) => {
      pendingRequests = pendingRequests.filter(r => r.id !== id);
    },
  };
}

async function getPendingRequests(db) {
  return db.get();
}

async function deletePendingRequest(db, id) {
  db.delete(id);
}

console.log('[ServiceWorker] Loaded and ready');
