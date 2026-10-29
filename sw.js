/* Agenda Comercial: guarda la app en el teléfono para abrirla sin señal. Los datos viajan por sync.js. */
const CACHE="agenda-v1";
const ARCHIVOS=["./","./index.html","./sync.js","./manifest.webmanifest","./icon-192.png","./icon-512.png"];
self.addEventListener("install",e=>{e.waitUntil(caches.open(CACHE).then(c=>c.addAll(ARCHIVOS)).then(()=>self.skipWaiting()))});
self.addEventListener("activate",e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==CACHE).map(k=>caches.delete(k)))).then(()=>self.clients.claim()))});
self.addEventListener("fetch",e=>{
  const u=new URL(e.request.url);
  if(e.request.method!=="GET"||u.origin!==location.origin)return; // el script de Google va siempre por la red
  // primero la red (para tener la última versión), si no hay señal la copia guardada
  e.respondWith(fetch(e.request).then(r=>{const c=r.clone();caches.open(CACHE).then(k=>k.put(e.request,c));return r}).catch(()=>caches.match(e.request,{ignoreSearch:true}).then(r=>r||caches.match("./index.html"))));
});
