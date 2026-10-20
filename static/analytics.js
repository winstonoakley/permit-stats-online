// Analytics identity and page views, shared by the calculator (index.html)
// and the content pages. Loaded as a classic script, so analyticsIds(),
// sendAnalytics() and reportError() are globals the calculator's own script
// can call.
//
// Two random ids live in localStorage: a visitor id (rotated after ~13
// months) and a visit id that starts over after 30 minutes without an
// event. seq_in_visit counts events within the visit. Nothing else about
// the browser is stored client-side; device and browser are parsed
// server-side from the User-Agent header.

const VISIT_GAP_MS = 30 * 60 * 1000;
const VISITOR_MAX_AGE_MS = 13 * 30 * 24 * 60 * 60 * 1000;
const memStore = {}; // fallback when localStorage is unavailable (private mode etc.)

function storeGet(key) {
  try { return localStorage.getItem(key); } catch (e) { return memStore[key] || null; }
}
function storeSet(key, value) {
  try { localStorage.setItem(key, value); } catch (e) { memStore[key] = value; }
}
function storeDel(key) {
  try { localStorage.removeItem(key); } catch (e) { delete memStore[key]; }
}
function randomId() {
  if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
  return 'id-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);
}
function readJson(key) {
  try { return JSON.parse(storeGet(key) || 'null'); } catch (e) { return null; }
}

// Returns {visitor_id, visit_id, seq_in_visit, is_new_visitor} and
// advances the visit; call once per event.
function analyticsIds() {
  const now = Date.now();
  let visitor = readJson('psVisitor');
  let isNew = false;
  if (!visitor || !visitor.id || (now - (visitor.created || 0)) > VISITOR_MAX_AGE_MS) {
    visitor = { id: randomId(), created: now };
    storeSet('psVisitor', JSON.stringify(visitor));
    isNew = true;
  }
  let visit = readJson('psVisit');
  if (!visit || !visit.id || (now - (visit.last || 0)) > VISIT_GAP_MS) {
    visit = { id: randomId(), last: now, seq: 0 };
  }
  visit.seq = (visit.seq || 0) + 1;
  visit.last = now;
  storeSet('psVisit', JSON.stringify(visit));
  return { visitor_id: visitor.id, visit_id: visit.id, seq_in_visit: visit.seq, is_new_visitor: isNew };
}

// Retire the v1 keys (a lifetime session id and a lifetime query counter).
storeDel('sessionId');
storeDel('queryIndex');

// Fire-and-forget delivery to /log_event; must never throw or block.
function sendAnalytics(body) {
  try {
    const json = JSON.stringify(body);
    if (navigator.sendBeacon) {
      navigator.sendBeacon('/log_event', new Blob([json], { type: 'text/plain' }));
    } else {
      fetch('/log_event', {
        method: 'POST', headers: { 'Content-Type': 'text/plain' }, body: json, keepalive: true
      }).catch(() => {});
    }
  } catch (e) { /* analytics must never break the page */ }
}

function reportError(endpoint, httpStatus, message) {
  sendAnalytics(Object.assign({
    event_type: 'error',
    detail: { endpoint: endpoint, http_status: httpStatus, message: message }
  }, analyticsIds()));
}

// One page_view per load, with the external referrer (query string
// dropped; a link from another page of this site counts as no referrer,
// since the visit's first page_view already carries how it arrived), any
// UTM tags, and the viewport width. Sent after the load event, when the
// browser is idle, so the beacon never competes with the page itself for
// bandwidth (it was costing up to ~150 ms of mobile LCP).
function whenIdleAfterLoad(fn) {
  const run = () => (window.requestIdleCallback ? requestIdleCallback(fn, { timeout: 2000 }) : setTimeout(fn, 0));
  if (document.readyState === 'complete') run();
  else window.addEventListener('load', run, { once: true });
}
whenIdleAfterLoad(function logPageView() {
  let referrer = null;
  try {
    if (document.referrer && new URL(document.referrer).origin !== window.location.origin) {
      referrer = document.referrer.split(/[?#]/)[0];
    }
  } catch (e) {}
  let params;
  try { params = new URLSearchParams(window.location.search); } catch (e) { params = new URLSearchParams(); }
  sendAnalytics(Object.assign({
    event_type: 'page_view',
    page: {
      path: window.location.pathname,
      referrer: referrer,
      utm_source: params.get('utm_source'),
      utm_medium: params.get('utm_medium'),
      utm_campaign: params.get('utm_campaign'),
      viewport_w: window.innerWidth || null
    }
  }, analyticsIds()));
});
