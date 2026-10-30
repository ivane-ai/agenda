/* Agenda Comercial · sincronización con Google Sheets y modo sin señal.
 * Reemplaza la base de datos de claude.ai con la misma forma de uso:
 *   db.doc(ruta).get() / set() / update() / delete() / onSnapshot()
 *   db.collection(col).where(campo,"==",valor).onSnapshot()
 * Todo se guarda primero en el teléfono (IndexedDB) y se envía al script cuando hay señal.
 */
(function () {
  "use strict";
  var API = window.AGENDA_API_URL;
  var DBN = "agenda-comercial", ST = "kv";
  var S = {}, T = {}, meta = { token: "", user: "", desde: 0, desdeBase: 0, lastOk: 0, sync1: false }, outbox = [];
  var L = { doc: {}, col: [] }, idb = null, sucio = {}, guardando = null, flushing = false, pulling = false, pullP = null, baseP = null, estado = "", cambiados = {};

  // ---------- IndexedDB ----------
  function abrir() {
    return new Promise(function (ok) {
      try {
        var r = indexedDB.open(DBN, 1);
        r.onupgradeneeded = function () { r.result.createObjectStore(ST); };
        r.onsuccess = function () { idb = r.result; ok(); };
        r.onerror = function () { ok(); };
      } catch (e) { ok(); }
    });
  }
  function leerTodo() {
    return new Promise(function (ok) {
      if (!idb) return ok();
      var tx = idb.transaction(ST, "readonly"), st = tx.objectStore(ST), rq = st.openCursor();
      rq.onsuccess = function () {
        var c = rq.result; if (!c) return ok();
        var k = c.key, v = c.value;
        if (k === "__meta") meta = Object.assign(meta, v);
        else if (k === "__outbox") outbox = v || [];
        else if (v) { S[k] = v.d; T[k] = v.t || 0; }
        c.continue();
      };
      rq.onerror = function () { ok(); };
    });
  }
  function persistir() { // agrupa escrituras al teléfono
    if (guardando) return guardando;
    guardando = new Promise(function (ok) {
      setTimeout(function () {
        var keys = Object.keys(sucio); sucio = {};
        if (!idb) { guardando = null; return ok(); }
        try {
          var tx = idb.transaction(ST, "readwrite"), st = tx.objectStore(ST);
          keys.forEach(function (k) {
            if (k === "__meta") st.put(meta, k);
            else if (k === "__outbox") st.put(outbox, k);
            else if (k in S) st.put({ d: S[k], t: T[k] || 0 }, k); else st.delete(k);
          });
          tx.oncomplete = tx.onerror = function () { guardando = null; ok(); if (Object.keys(sucio).length) persistir(); };
        } catch (e) { guardando = null; ok(); }
      }, 150);
    });
    return guardando;
  }
  function marcar(k) { sucio[k] = 1; persistir(); }

  // ---------- red ----------
  function llamar(body, ms) {
    var ctl = typeof AbortController !== "undefined" ? new AbortController() : null;
    var to = setTimeout(function () { if (ctl) ctl.abort(); }, ms || 45000);
    return fetch(API, { method: "POST", body: JSON.stringify(body), headers: { "Content-Type": "text/plain;charset=utf-8" }, redirect: "follow", signal: ctl ? ctl.signal : undefined })
      .then(function (r) { clearTimeout(to); return r.json(); })
      .catch(function (e) { clearTimeout(to); throw e; });
  }
  var online = function () { return navigator.onLine !== false; };
  var GZ = typeof DecompressionStream !== "undefined" && typeof Response !== "undefined";
  function docsDe(r) {
    if (!r || !r.gz) return Promise.resolve((r && r.docs) || []);
    var bin = atob(r.gz), u = new Uint8Array(bin.length);
    for (var i = 0; i < bin.length; i++) u[i] = bin.charCodeAt(i);
    return new Response(new Blob([u]).stream().pipeThrough(new DecompressionStream("gzip"))).text().then(function (t) { return JSON.parse(t); });
  }

  // ---------- avisos a la pantalla ----------
  var clone = function (x) { return x == null ? x : JSON.parse(JSON.stringify(x)); };
  function snap(p) { return { id: p.split("/").pop(), exists: (p in S) && S[p] != null, data: function () { return clone(S[p]); }, ref: { path: p } }; }
  function colSnap(c, w) {
    var docs = Object.keys(S).filter(function (p) { return S[p] != null && p.split("/").length === 2 && p.indexOf(c + "/") === 0; })
      .filter(function (p) { return !w || w.every(function (f) { return S[p] && S[p][f[0]] === f[2]; }); }).map(snap);
    return { docs: docs, size: docs.length, empty: !docs.length, forEach: function (fn) { docs.forEach(fn); } };
  }
  var pendAviso = {}, avisoT = null;
  function avisar(p) {
    pendAviso[p] = 1;
    if (avisoT) return;
    avisoT = setTimeout(function () {
      var ps = Object.keys(pendAviso); pendAviso = {}; avisoT = null;
      ps.forEach(function (q) { (L.doc[q] || []).slice().forEach(function (cb) { try { cb(snap(q)); } catch (e) { } }); });
      L.col.slice().forEach(function (l) { if (ps.some(function (q) { return q.indexOf(l.c + "/") === 0; })) { try { l.cb(colSnap(l.c, l.w)); } catch (e) { } } });
      pendUIx();
    }, 30);
  }
  function aplicar(p, d, t) { if (d == null) delete S[p]; else S[p] = d; T[p] = t || T[p] || 0; marcar(p); avisar(p); cambiados[p] = 1; }
  function avisarSync() { var ps = Object.keys(cambiados); cambiados = {}; if (ps.length) try { window.dispatchEvent(new CustomEvent("agenda-sync", { detail: ps })); } catch (e) { } }

  // ---------- escribir (primero en el teléfono, después al script) ----------
  function encolar(op) { outbox.push(op); marcar("__outbox"); pendUIx(); setTimeout(enviar, 300); }
  function doc(p) {
    return {
      path: p, id: p.split("/").pop(),
      get: function () { if (!((p in S) && S[p] != null) && (pullP || (meta.token && !meta.sync1))) return (pullP || bajar()).then(function () { return snap(p); }); return Promise.resolve(snap(p)); },
      set: function (d) { aplicar(p, clone(d)); encolar({ k: "set", r: p, d: clone(d) }); return Promise.resolve(); },
      update: function (d) {
        if (!(p in S) || S[p] == null) { var e = new Error("not found"); e.code = "not-found"; return Promise.reject(e); }
        aplicar(p, Object.assign({}, S[p], clone(d))); encolar({ k: "update", r: p, d: clone(d) }); return Promise.resolve();
      },
      delete: function () { aplicar(p, null); encolar({ k: "delete", r: p }); return Promise.resolve(); },
      onSnapshot: function (cb) { (L.doc[p] = L.doc[p] || []).push(cb); setTimeout(function () { cb(snap(p)); }, 0); return function () { L.doc[p] = (L.doc[p] || []).filter(function (x) { return x !== cb; }); }; }
    };
  }
  function col(c, w) {
    return {
      where: function (f, op, v) { return col(c, (w || []).concat([[f, op, v]])); },
      get: function () { return Promise.resolve(colSnap(c, w)); },
      doc: function (id) { return doc(c + "/" + id); },
      onSnapshot: function (cb) { var l = { c: c, w: w, cb: cb }; L.col.push(l); setTimeout(function () { cb(colSnap(c, w)); }, 0); return function () { L.col = L.col.filter(function (x) { return x !== l; }); }; }
    };
  }
  var db = { doc: doc, collection: function (c) { return col(c); } };

  function enviar() {
    if (flushing || !outbox.length || !meta.token || !online()) return Promise.resolve();
    flushing = true;
    var lote = [], tam = 0;
    for (var i = 0; i < outbox.length; i++) { var s = JSON.stringify(outbox[i]).length; if (lote.length && tam + s > 1500000) break; lote.push(outbox[i]); tam += s; }
    return llamar({ op: "write", token: meta.token, ops: lote }, 120000).then(function (r) {
      flushing = false;
      if (!r || !r.ok) { if (r && r.error === "sesion") avisoSesion(); return; }
      outbox = outbox.slice(lote.length); marcar("__outbox"); meta.lastOk = Date.now(); marcar("__meta");
      var rech = (r.res || []).filter(function (x) { return !x.ok; });
      if (rech.length && window.console) console.warn("Rechazados por el servidor", rech);
      pendUIx();
      if (outbox.length) return enviar();
    }).catch(function () { flushing = false; pendUIx(); });
  }
  function bajar() {
    if (pullP) return pullP;
    if (!meta.token || !online()) return Promise.resolve(false);
    pulling = true; estado = "sincronizando"; pendUIx();
    var uno = function () {
      return llamar({ op: "pull", token: meta.token, desde: meta.desde || 0, sin: ["base"], gz: GZ ? 1 : 0 }, 120000).then(function (r) {
        if (!r || !r.ok) { if (r && r.error === "sesion") avisoSesion(); else estado = "error: " + ((r && (r.detalle || r.error)) || "respuesta vacía"); return false; }
        return docsDe(r).then(function (docs) {
          var pendientes = {}; outbox.forEach(function (o) { pendientes[o.r] = 1; });
          docs.forEach(function (x) { if (x.r.indexOf("base/") === 0) return; if (!pendientes[x.r]) aplicar(x.r, x.d, x.t); }); // lo cargado sin señal no se pisa
          meta.desde = r.hasta || meta.desde; meta.lastOk = Date.now(); marcar("__meta");
          return r.completo ? true : uno();
        });
      });
    };
    pullP = uno().then(function (ok) { if (ok) { meta.sync1 = true; marcar("__meta"); } return ok; }).catch(function (e) { estado = "error: no se pudo leer la planilla (" + ((e && e.message) || e) + ")"; return false; })
      .then(function (ok) { pulling = false; pullP = null; if (ok || estado === "sincronizando") estado = ""; pendUIx(); avisarSync(); return ok; });
    return pullP;
  }
  // historial de ventas y cobros (solo administración, al actualizar con el crudo)
  function bajarBase() {
    if (baseP) return baseP;
    if (!meta.token || !online()) return Promise.resolve(false);
    var uno = function () {
      return llamar({ op: "pull", token: meta.token, desde: meta.desdeBase || 0, solo: ["base"], gz: GZ ? 1 : 0 }, 180000).then(function (r) {
        if (!r || !r.ok) return false;
        return docsDe(r).then(function (docs) {
          var pendientes = {}; outbox.forEach(function (o) { pendientes[o.r] = 1; });
          docs.forEach(function (x) { if (!pendientes[x.r]) aplicar(x.r, x.d, x.t); });
          meta.desdeBase = r.hasta || meta.desdeBase; marcar("__meta");
          return r.completo ? true : uno();
        });
      });
    };
    baseP = uno().catch(function () { return false; }).then(function (ok) { baseP = null; return ok; });
    return baseP;
  }
  function sincronizar() { return enviar().then(bajar); }
  function avisoSesion() { estado = "sesion"; pendUIx(); }

  // ---------- usuarios ----------
  function publico() {
    return llamar({ op: "publico" }, 8000).then(function (r) {
      if (!r || !r.ok) return;
      if (!meta.token) { // sin sesión: lista de usuarios y quién ya tiene clave
        var c = {}; Object.keys(r.conClave || {}).forEach(function (k) { c[k] = { hash: r.conClave[k] ? "x" : "" }; });
        aplicar("config/vendedores", { vendedores: r.vendedores || [] }, 0); aplicar("config/claves", c, 0);
      }
    }).catch(function () { });
  }
  function login(u, p) {
    return llamar({ op: "login", usuario: u, clave: p }, 30000).then(function (r) {
      if (!r || !r.ok) return { ok: false, error: (r && r.error) || "No se pudo ingresar." };
      if (meta.user && meta.user !== u) { Object.keys(S).forEach(function (k) { if (k.indexOf("config/") !== 0) { delete S[k]; marcar(k); } }); meta.desde = 0; meta.desdeBase = 0; }
      var cambio = meta.user !== u;
      meta.token = r.token; meta.user = u; estado = ""; if (cambio) meta.sync1 = false; marcar("__meta");
      return Promise.resolve(pullP).then(function () { return bajar(); }).then(function () { return { ok: true }; });
    }).catch(function () { return { ok: false, error: "Sin conexión. Para ingresar la primera vez hace falta señal." }; });
  }
  function logout() {
    meta.token = ""; meta.user = ""; meta.desde = 0; meta.desdeBase = 0; meta.sync1 = false; marcar("__meta");
    Object.keys(S).forEach(function (k) { if (k.indexOf("config/") !== 0) { delete S[k]; marcar(k); } });
    outbox = []; marcar("__outbox");
  }

  // ---------- aviso de pendientes ----------
  function pendUIx() { try { if (typeof window.__agendaPendUI === "function") window.__agendaPendUI(); } catch (e) { } }
  window.AGENDA = {
    login: login, logout: logout, sync: sincronizar, bajarBase: bajarBase,
    user: function () { return meta.token ? meta.user : ""; },
    pending: function () { return outbox.length; },
    estado: function () { return estado; },
    ultima: function () { return meta.lastOk; },
    rebajar: function () { meta.desde = 0; meta.sync1 = false; marcar("__meta"); return Promise.resolve(pullP).then(function () { return bajar(); }); },
    importar: function (obj) { Object.keys(obj).forEach(function (p) { if (obj[p] != null) doc(p).set(obj[p]); }); return enviar(); }
  };

  var listo = abrir().then(leerTodo).then(function () {
    var pub = publico();
    // con datos guardados no se espera a la red; la primera vez sí (máximo 8 s)
    return (S["config/vendedores"] ? Promise.resolve() : pub).then(function () { if (meta.token) setTimeout(sincronizar, 500); });
  });
  window.claude = { use: function (n) { return n === "db" ? listo.then(function () { return db; }) : Promise.resolve(null); } };

  addEventListener("online", function () { sincronizar(); });
  addEventListener("offline", pendUIx);
  document.addEventListener("visibilitychange", function () { if (document.visibilityState === "visible") sincronizar(); });
  setInterval(function () { if (document.visibilityState !== "hidden") sincronizar(); }, 60000);
})();
