// Tiny offline cache for shell
const CACHE = 'trancheready-v1';
const ASSETS = ['/', '/style.css', '/ui.js', '/logo.svg', '/manifest.webmanifest'];

self.addEventListener('install', e=>{
  e.waitUntil(caches.open(CACHE).then(c=>c.addAll(ASSETS)));
});
self.addEventListener('activate', e=>{
  e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k!==CACHE).map(k=>caches.delete(k)))));
});
self.addEventListener('fetch', e=>{
  const req = e.request;
  if (req.method !== 'GET') return;
  e.respondWith(
    caches.match(req).then(cached => cached ||
      fetch(req).then(res=>{
        const resClone = res.clone();
        caches.open(CACHE).then(c=>c.put(req, resClone));
        return res;
      }).catch(()=> caches.match('/'))
    )
  );
});
