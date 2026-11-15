const CACHE_NAME = 'whatsapp-platform-v3';

const STATIC_ASSETS = [
    '/static/dashboard.html',
];

// Install: pre-cache static assets
self.addEventListener('install', event => {
    event.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => cache.addAll(STATIC_ASSETS))
            .then(() => self.skipWaiting())
    );
});

// Activate: clean old caches, claim clients immediately
self.addEventListener('activate', event => {
    event.waitUntil(
        caches.keys()
            .then(keys => Promise.all(
                keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
            ))
            .then(() => self.clients.claim())
    );
});

// Push: show a notification for an inbound WhatsApp message
self.addEventListener('push', event => {
    let data = { title: 'New message', body: '', url: '/static/dashboard.html', tag: 'wa-msg' };
    try {
        if (event.data) data = Object.assign(data, event.data.json());
    } catch (e) { /* keep defaults */ }

    event.waitUntil(
        self.registration.showNotification(data.title, {
            body: data.body,
            tag: data.tag,
            icon: '/static/icon-192.png',
            badge: '/static/icon-192.png',
            data: { url: data.url },
            renotify: true,
        })
    );
});

// Notification click: focus an existing tab or open the dashboard
self.addEventListener('notificationclick', event => {
    event.notification.close();
    const target = (event.notification.data && event.notification.data.url) || '/static/dashboard.html';
    event.waitUntil(
        self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clients => {
            for (const client of clients) {
                if ('focus' in client) { client.focus(); return; }
            }
            if (self.clients.openWindow) return self.clients.openWindow(target);
        })
    );
});

// Fetch: network-first for API, cache-first for static
self.addEventListener('fetch', event => {
    const url = new URL(event.request.url);

    // Network-first for API calls and auth
    if (url.pathname.startsWith('/api/') ||
        url.pathname.startsWith('/auth/') ||
        url.pathname.startsWith('/calls') ||
        url.pathname === '/manifest.json') {
        event.respondWith(
            fetch(event.request).catch(() => caches.match(event.request))
        );
        return;
    }

    // Cache-first for static assets
    event.respondWith(
        caches.match(event.request).then(cached => {
            if (cached) {
                // Update cache in background
                fetch(event.request).then(response => {
                    if (response.ok) {
                        caches.open(CACHE_NAME).then(cache => cache.put(event.request, response));
                    }
                }).catch(() => {});
                return cached;
            }
            return fetch(event.request);
        })
    );
});
