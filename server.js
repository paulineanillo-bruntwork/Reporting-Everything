require('dotenv').config();
const express = require('express');
const path = require('path');
const crypto = require('crypto');
const session = require('express-session');
const { Issuer, generators } = require('openid-client');

const app = express();
const PORT = process.env.PORT || 3000;
const HUBSPOT_KEY = process.env.HUBSPOT_TOKEN;
const GONG_API_KEY = process.env.GONG_API_KEY || 'IVZIJZ5HRXUX2YGVVOVIBYFK6MQJJFI3';
const GONG_API_SECRET = process.env.GONG_API_SECRET || 'eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjIwOTA2NDA2NTgsImFjY2Vzc0tleSI6IklWWklKWjVIUlhVWDJZR1ZWT1ZJQllGSzZNUUpKRkkzIn0.JJVRHRxOvza9mNYZ-gJM5Wjw_UDeSSQMneq5SES-_Ys';
const GONG_BASE_URL = 'https://us-66463.api.gong.io/v2';
const HUBSPOT_API = 'https://api.hubapi.com/crm/v3/objects/tickets/search';
const PIPELINES = ['4483329', '3857063', '20565603'];

const KEYCLOAK_URL = process.env.KEYCLOAK_URL;
const KEYCLOAK_REALM = process.env.KEYCLOAK_REALM;
const KEYCLOAK_CLIENT_ID = process.env.KEYCLOAK_CLIENT_ID;
const KEYCLOAK_CLIENT_SECRET = process.env.KEYCLOAK_CLIENT_SECRET;
const APP_URL = process.env.APP_URL || ('http://localhost:' + PORT);
const SESSION_SECRET = process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');

if (!KEYCLOAK_URL || !KEYCLOAK_REALM || !KEYCLOAK_CLIENT_ID) {
  console.error('Missing required env vars: KEYCLOAK_URL, KEYCLOAK_REALM, KEYCLOAK_CLIENT_ID');
  process.exit(1);
}

// Trust Railway's reverse proxy so secure cookies work over HTTPS
app.set('trust proxy', 1);

// Parse form data and JSON
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// Session middleware
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  rolling: true,
  cookie: {
    httpOnly: true,
    sameSite: 'lax',
    secure: APP_URL.startsWith('https'),
    maxAge: 24 * 60 * 60 * 1000 // 24 hours
  }
}));

// OIDC client — initialised before server starts
var oidcClient;

async function initOIDC() {
  var issuerUrl = KEYCLOAK_URL + '/realms/' + KEYCLOAK_REALM;
  var issuer = await Issuer.discover(issuerUrl);
  oidcClient = new issuer.Client({
    client_id: KEYCLOAK_CLIENT_ID,
    client_secret: KEYCLOAK_CLIENT_SECRET,
    redirect_uris: [APP_URL + '/auth/callback'],
    response_types: ['code']
  });
  console.log('OIDC client initialised for issuer:', issuer.issuer);
}

// Block search engines
app.get('/robots.txt', function(req, res) {
  res.type('text/plain');
  res.send('User-agent: *\nDisallow: /\n');
});

// Redirect legacy /login to /auth/login
app.get('/login', function(req, res) {
  res.redirect('/auth/login');
});

// ===== Email allowlist =====
var ALLOWED_EMAILS_SHEET_ID = '1_kbicBlpJbm0kOBkoIU4k6CrPUHRanI8FAZnVtyrshM';
var ALLOWED_EMAILS_RANGE = 'A:A'; // column A of first tab; includes header which is harmless
var ALLOWED_EMAILS_TTL = 5 * 60 * 1000; // 5 minutes
var allowedEmailsCache = { set: null, ts: 0 };

async function getAllowedEmails(force) {
  var now = Date.now();
  if (!force && allowedEmailsCache.set && (now - allowedEmailsCache.ts) < ALLOWED_EMAILS_TTL) {
    return allowedEmailsCache.set;
  }
  if (!GOOGLE_SA_KEY) {
    console.error('[Allowlist] GOOGLE_SA_KEY not configured — allowlist cannot be fetched');
    return null;
  }
  try {
    var data = await sheetsGet(ALLOWED_EMAILS_SHEET_ID, ALLOWED_EMAILS_RANGE);
    var rows = data.values || [];
    var set = {};
    for (var i = 0; i < rows.length; i++) {
      var v = (rows[i][0] || '').trim().toLowerCase();
      if (v && v.indexOf('@') !== -1) set[v] = true;
    }
    allowedEmailsCache = { set: set, ts: now };
    console.log('[Allowlist] Loaded ' + Object.keys(set).length + ' allowed emails');
    return set;
  } catch (e) {
    console.error('[Allowlist] Failed to fetch allowed emails:', e.message);
    // If we have a stale cache, keep using it rather than locking everyone out
    if (allowedEmailsCache.set) return allowedEmailsCache.set;
    return null;
  }
}

// Access denied page
app.get('/access-denied', function(req, res) {
  var email = (req.session && req.session.pendingEmail) || '';
  res.status(403).send('<!DOCTYPE html><html><head><title>Access Denied</title>' +
    '<style>body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f8fafc;color:#1e293b;margin:0;padding:0;display:flex;align-items:center;justify-content:center;min-height:100vh}' +
    '.card{background:#fff;border-radius:12px;box-shadow:0 10px 30px rgba(0,0,0,.08);padding:40px 48px;max-width:480px;text-align:center}' +
    'h1{font-size:22px;margin:0 0 12px;color:#dc2626}' +
    'p{color:#475569;line-height:1.5;margin:8px 0}' +
    '.email{background:#f1f5f9;padding:4px 10px;border-radius:6px;font-family:monospace;font-size:13px;color:#0f172a}' +
    '.btn{display:inline-block;margin-top:16px;background:#0f172a;color:#fff;text-decoration:none;padding:10px 18px;border-radius:8px;font-weight:600}' +
    '.btn:hover{background:#1e293b}</style></head><body>' +
    '<div class="card"><h1>Access Denied</h1>' +
    '<p>Your account <span class="email">' + email.replace(/[<>&"]/g, '') + '</span> is not authorised to view this dashboard.</p>' +
    '<p>If you think this is a mistake, please contact an administrator.</p>' +
    '<a class="btn" href="/logout">Sign out</a></div></body></html>');
});

// Debug: refresh allowlist cache (admin only-ish: requires valid session)
app.get('/api/debug/refresh-allowlist', async function(req, res) {
  var set = await getAllowedEmails(true);
  res.json({ count: set ? Object.keys(set).length : 0, emails: set ? Object.keys(set) : [] });
});

// Redirect to Keycloak login
app.get('/auth/login', function(req, res) {
  if (req.session.user) return res.redirect('/');
  var state = generators.state();
  var nonce = generators.nonce();
  req.session.oidcState = state;
  req.session.oidcNonce = nonce;
  var url = oidcClient.authorizationUrl({
    scope: 'openid profile email',
    state: state,
    nonce: nonce
  });
  req.session.save(function(err) {
    if (err) {
      console.error('Session save error:', err);
      return res.status(500).send('Session error');
    }
    res.redirect(url);
  });
});

// Keycloak callback — exchange code for tokens
app.get('/auth/callback', async function(req, res) {
  try {
    var params = oidcClient.callbackParams(req);
    var tokenSet = await oidcClient.callback(
      APP_URL + '/auth/callback',
      params,
      { state: req.session.oidcState, nonce: req.session.oidcNonce }
    );
    var claims = tokenSet.claims();
    var userEmail = (claims.email || '').toLowerCase();

    // Check email against allowlist
    var allowed = await getAllowedEmails();
    if (allowed && (!userEmail || !allowed[userEmail])) {
      console.log('[Auth] Denied: ' + userEmail + ' not in allowlist');
      req.session.pendingEmail = userEmail;
      req.session.idToken = tokenSet.id_token; // keep so logout end-session works
      delete req.session.oidcState;
      delete req.session.oidcNonce;
      return req.session.save(function() {
        res.redirect('/access-denied');
      });
    }

    req.session.user = {
      sub: claims.sub,
      name: claims.name || claims.preferred_username,
      email: userEmail
    };
    req.session.idToken = tokenSet.id_token;
    delete req.session.oidcState;
    delete req.session.oidcNonce;
    delete req.session.pendingEmail;
    req.session.save(function(err) {
      if (err) {
        console.error('Session save error:', err);
        return res.status(500).send('Session error');
      }
      res.redirect('/');
    });
  } catch (err) {
    console.error('OIDC callback error:', err.message);
    res.redirect('/auth/login?error=auth_failed');
  }
});

// Logout — destroy session and redirect to Keycloak end-session
app.get('/logout', function(req, res) {
  var idToken = req.session.idToken;
  req.session.destroy(function() {
    res.clearCookie('connect.sid');
    try {
      var logoutUrl = oidcClient.endSessionUrl({
        post_logout_redirect_uri: APP_URL,
        id_token_hint: idToken
      });
      res.redirect(logoutUrl);
    } catch (e) {
      res.redirect(APP_URL);
    }
  });
});

// Auth middleware - protect everything except auth routes and robots.txt
var PUBLIC_PATHS = ['/auth/login', '/auth/callback', '/login', '/logout', '/access-denied', '/robots.txt', '/api/debug/schemas'];
app.use(async function(req, res, next) {
  if (PUBLIC_PATHS.indexOf(req.path) !== -1 || req.path.startsWith('/api/debug/')) return next();
  if (!req.session.user) {
    if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Unauthorized' });
    return res.redirect('/auth/login');
  }
  // Re-verify email against allowlist (cached for 5 min, so this is cheap)
  try {
    var allowed = await getAllowedEmails();
    var email = (req.session.user.email || '').toLowerCase();
    if (allowed && !allowed[email]) {
      console.log('[Auth] Session revoked for ' + email + ' (no longer on allowlist)');
      if (req.path.startsWith('/api/')) return res.status(403).json({ error: 'Forbidden' });
      return res.redirect('/access-denied');
    }
  } catch (e) {
    console.error('[Auth] Allowlist check failed:', e.message);
    // Fail open if the allowlist fetch itself crashed unexpectedly — prevents locking everyone out
  }
  next();
});

// Serve static files (only after auth check)
app.use(express.static(path.join(__dirname)));

async function hubspotSearch(body) {
  var url = HUBSPOT_API;
  var headers = { 'Content-Type': 'application/json' };

  if (HUBSPOT_KEY && HUBSPOT_KEY.startsWith('pat-')) {
    headers['Authorization'] = 'Bearer ' + HUBSPOT_KEY;
  } else {
    url = HUBSPOT_API + '?hapikey=' + HUBSPOT_KEY;
  }

  const res = await fetch(url, {
    method: 'POST',
    headers: headers,
    body: JSON.stringify(body)
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error('HubSpot API error ' + res.status + ': ' + text);
  }
  return res.json();
}

// Generic HubSpot CRM object search (contacts, deals, etc.)
async function hubspotSearchObject(objectType, body) {
  var url = 'https://api.hubapi.com/crm/v3/objects/' + objectType + '/search';
  var headers = { 'Content-Type': 'application/json' };
  if (HUBSPOT_KEY && HUBSPOT_KEY.startsWith('pat-')) {
    headers['Authorization'] = 'Bearer ' + HUBSPOT_KEY;
  } else {
    url += '?hapikey=' + HUBSPOT_KEY;
  }
  var res = await fetch(url, { method: 'POST', headers: headers, body: JSON.stringify(body) });
  if (!res.ok) {
    var text = await res.text();
    throw new Error('HubSpot ' + objectType + ' search error ' + res.status + ': ' + text);
  }
  return res.json();
}

async function fetchAllPagesObject(objectType, baseBody) {
  var results = [];
  var after = undefined;
  var hasMore = true;
  var page = 0;
  while (hasMore) {
    var body = Object.assign({}, baseBody, { limit: 200 });
    if (after) body.after = after;
    if (page > 0) await sleep(400);
    var data;
    try {
      data = await hubspotSearchObject(objectType, body);
    } catch (err) {
      if (err.message && err.message.indexOf('429') !== -1) {
        console.log('Rate limited on ' + objectType + ', waiting 3s and retrying...');
        await sleep(3000);
        data = await hubspotSearchObject(objectType, body);
      } else {
        throw err;
      }
    }
    results = results.concat(data.results || []);
    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
    } else {
      hasMore = false;
    }
    page++;
  }
  return results;
}

// HubSpot GET request helper (for schemas, properties, etc.)
async function hubspotGet(url) {
  var headers = {};
  if (HUBSPOT_KEY && HUBSPOT_KEY.startsWith('pat-')) {
    headers['Authorization'] = 'Bearer ' + HUBSPOT_KEY;
  } else {
    url += (url.indexOf('?') >= 0 ? '&' : '?') + 'hapikey=' + HUBSPOT_KEY;
  }
  var res = await fetch(url, { headers: headers });
  if (!res.ok) {
    var text = await res.text();
    throw new Error('HubSpot GET error ' + res.status + ': ' + text);
  }
  return res.json();
}

// Discover custom object type ID for "jobs" (cached)
var _jobsObjectTypeId = null;
async function getJobsObjectTypeId() {
  if (_jobsObjectTypeId) return _jobsObjectTypeId;
  var schemas = await hubspotGet('https://api.hubapi.com/crm/v3/schemas');
  var results = schemas.results || schemas;
  for (var i = 0; i < results.length; i++) {
    var s = results[i];
    var name = (s.name || '').toLowerCase();
    var label = (s.labels && s.labels.singular || '').toLowerCase();
    if (name === 'job' || name === 'jobs' || label === 'job' || label === 'jobs' ||
        name.indexOf('_job') !== -1 || name.indexOf('jobs') !== -1) {
      _jobsObjectTypeId = s.objectTypeId;
      console.log('[HubSpot] Found jobs custom object: objectTypeId=' + s.objectTypeId + ', name=' + s.name);
      return _jobsObjectTypeId;
    }
  }
  // Log all schemas to help debug
  console.log('[HubSpot] Custom object schemas found: ' + results.map(function(s) { return s.name + ' (' + s.objectTypeId + ')'; }).join(', '));
  throw new Error('Could not find "jobs" custom object in HubSpot schemas');
}

// Diagnostic endpoint to list all custom object schemas
app.get('/api/debug/months', async function(req, res) {
  try {
    var data = await sheetsGet(KPI_SOURCE_SHEET_ID, KPI_SOURCE_TAB + '!A1:AZ');
    var rows = data.values || [];
    var headers = rows[2] || [];
    var colMap = {};
    for (var i = 0; i < Math.min(headers.length, 15); i++) {
      colMap['col_' + i] = headers[i] || '';
    }
    var dataRows = rows.slice(3);
    var months = dataRows.map(function(r) { return r[0] || ''; }).filter(function(m) { return m.trim(); });
    // Show last 3 months with first 10 column values
    var last3 = dataRows.slice(-3).map(function(r) {
      var obj = {};
      for (var j = 0; j < Math.min(r.length, 15); j++) {
        obj['col_' + j + '_' + (headers[j] || '').substring(0, 30)] = r[j] || '';
      }
      return obj;
    });
    res.json({ headers_first15: colMap, months: months, last5: months.slice(-5), last3_data: last3 });
  } catch (e) {
    res.json({ error: e.message });
  }
});

// One-time fix: write a value to a specific month/col in KPI sheet
app.post('/api/debug/fix-cell', async function(req, res) {
  try {
    var month = req.body.month; // e.g. "April 2025"
    var col = parseInt(req.body.col);
    var value = req.body.value;
    if (!month || isNaN(col) || value === undefined) return res.status(400).json({ error: 'month, col, value required' });
    var data = await sheetsGet(KPI_SOURCE_SHEET_ID, KPI_SOURCE_TAB + '!A1:AZ');
    var rows = data.values || [];
    var dataRows = rows.slice(3);
    var targetRowIdx = -1;
    for (var i = 0; i < dataRows.length; i++) {
      if (dataRows[i][0] && dataRows[i][0].trim() === month.trim()) { targetRowIdx = i; break; }
    }
    if (targetRowIdx === -1) return res.status(404).json({ error: 'Month not found: ' + month });
    var sheetRow = targetRowIdx + 4;
    var cell = KPI_SOURCE_TAB + '!' + colLetter(col) + sheetRow;
    var numVal = parseFloat(value);
    await sheetsUpdate(KPI_SOURCE_SHEET_ID, cell, [[isNaN(numVal) ? value : numVal]]);
    kpiHistoryCache = { data: null, ts: 0 };
    res.json({ success: true, cell: cell, value: isNaN(numVal) ? value : numVal, month: month });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DEBUG: Show service account email
app.get('/api/debug/sa-email', function(req, res) {
  res.json({ serviceAccountEmail: GOOGLE_SA_KEY ? GOOGLE_SA_KEY.client_email : 'NOT CONFIGURED' });
});

// DEBUG: Which date fields are populated for outcome rows?
app.get('/api/debug/ci-datefields', async function(req, res) {
  try {
    var slotNums = ['n1st', 'n2nd', 'n3rd', 'n4th', 'n5th'];
    var out = {};
    for (var i = 0; i < slotNums.length; i++) {
      var n = slotNums[i];
      var outcome = n + '_client_interview_outcome';
      var candidates = [
        n + '_client_interview_date',
        n + '_interview_date_and_time__your_timezone_',
        n + '_interview__created_date'
      ];
      var slotOut = { outcome_prop: outcome };
      for (var c = 0; c < candidates.length; c++) {
        var dp = candidates[c];
        try {
          var r = await hubspotSearchObject('2-38227027', {
            filterGroups: [{ filters: [
              { propertyName: outcome, operator: 'HAS_PROPERTY' },
              { propertyName: dp, operator: 'HAS_PROPERTY' }
            ]}],
            properties: [outcome, dp], limit: 2
          });
          slotOut[dp] = { count: r.total, sample: (r.results||[]).map(function(x){ return x.properties; }) };
        } catch (e) { slotOut[dp] = { err: e.message.substring(0,150) }; }
      }
      out[n] = slotOut;
    }
    res.json(out);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DEBUG: Candidate interviews diagnostic
app.get('/api/debug/ci-diag', async function(req, res) {
  try {
    var slots = [
      { prop: 'n1st_client_interview_outcome', dateProp: 'n1st_client_interview_date' },
      { prop: 'n2nd_client_interview_outcome', dateProp: 'n2nd_client_interview_date' },
      { prop: 'n3rd_client_interview_outcome', dateProp: 'n3rd_client_interview_date' },
      { prop: 'n4th_client_interview_outcome', dateProp: 'n4th_interview_date_and_time__your_timezone_' },
      { prop: 'n5th_client_interview_outcome', dateProp: 'n5th_interview_date_and_time__your_timezone_' }
    ];
    var results = {};
    for (var i = 0; i < slots.length; i++) {
      var slot = slots[i];
      var slotOut = {};
      // Total with outcome property set (any value)
      try {
        var r1 = await hubspotSearchObject('2-38227027', {
          filterGroups: [{ filters: [{ propertyName: slot.prop, operator: 'HAS_PROPERTY' }] }],
          properties: [slot.prop], limit: 1
        });
        slotOut.outcome_set = r1.total;
      } catch (e) { slotOut.outcome_err = e.message.substring(0,150); }
      // Total with date property set
      try {
        var r2 = await hubspotSearchObject('2-38227027', {
          filterGroups: [{ filters: [{ propertyName: slot.dateProp, operator: 'HAS_PROPERTY' }] }],
          properties: [slot.dateProp], limit: 1
        });
        slotOut.date_set = r2.total;
      } catch (e) { slotOut.date_err = e.message.substring(0,150); }
      // Both set
      try {
        var r3 = await hubspotSearchObject('2-38227027', {
          filterGroups: [{ filters: [
            { propertyName: slot.prop, operator: 'HAS_PROPERTY' },
            { propertyName: slot.dateProp, operator: 'HAS_PROPERTY' }
          ]}],
          properties: [slot.prop, slot.dateProp], limit: 3
        });
        slotOut.both_set = r3.total;
        slotOut.sample = (r3.results || []).map(function(x) { return { id: x.id, outcome: x.properties[slot.prop], date: x.properties[slot.dateProp] }; });
      } catch (e) { slotOut.both_err = e.message.substring(0,150); }
      results[slot.prop] = slotOut;
    }
    res.json(results);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DEBUG: List Application object properties (to find interview outcome property names)
app.get('/api/debug/app-properties', async function(req, res) {
  try {
    var url = 'https://api.hubapi.com/crm/v3/properties/2-38227027';
    var headers = { 'Content-Type': 'application/json' };
    if (HUBSPOT_KEY && HUBSPOT_KEY.startsWith('pat-')) headers['Authorization'] = 'Bearer ' + HUBSPOT_KEY;
    var r = await fetch(url, { headers: headers });
    var data = await r.json();
    var props = (data.results || []).map(function(p) {
      return { name: p.name, label: p.label, type: p.type, fieldType: p.fieldType };
    });
    // Filter to interview/outcome related
    var q = (req.query.q || '').toLowerCase();
    if (q) props = props.filter(function(p) { return (p.label + ' ' + p.name).toLowerCase().indexOf(q) !== -1; });
    res.json({ count: props.length, props: props });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// One-time: update header label in KPI sheet
app.post('/api/debug/fix-header', async function(req, res) {
  try {
    var col = parseInt(req.body.col);
    var value = req.body.value;
    var row = parseInt(req.body.row) || 3; // default to header row (row 3 = column names)
    if (isNaN(col) || !value) return res.status(400).json({ error: 'col, value required' });
    var cell = KPI_SOURCE_TAB + '!' + colLetter(col) + row;
    await sheetsUpdate(KPI_SOURCE_SHEET_ID, cell, [[value]]);
    kpiHistoryCache = { data: null, ts: 0 };
    res.json({ success: true, cell: cell, value: value });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/debug/endorsements', async function(req, res) {
  try {
    var includedStages = [
      '978051969', '977990485', '977990486', '977990487',
      '1075896997', '977990488', '977990489', '1015966551'
    ];
    // Test 1: Simple pipeline-only query
    var test1 = await hubspotSearchObject('2-38227027', {
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'EQ', value: '666493306' }
        ]
      }],
      properties: ['hs_pipeline_stage', 'client__cloned_'],
      limit: 5
    });
    // Test 2: With IN filter on stages
    var test2err = null, test2 = null;
    try {
      test2 = await hubspotSearchObject('2-38227027', {
        filterGroups: [{
          filters: [
            { propertyName: 'hs_pipeline', operator: 'EQ', value: '666493306' },
            { propertyName: 'hs_pipeline_stage', operator: 'IN', values: includedStages }
          ]
        }],
        properties: ['hs_pipeline_stage', 'client__cloned_'],
        limit: 5
      });
    } catch (e2) { test2err = e2.message; }
    // Test 3: Single stage EQ
    var test3err = null, test3 = null;
    try {
      test3 = await hubspotSearchObject('2-38227027', {
        filterGroups: [{
          filters: [
            { propertyName: 'hs_pipeline', operator: 'EQ', value: '666493306' },
            { propertyName: 'hs_pipeline_stage', operator: 'EQ', value: '978051969' }
          ]
        }],
        properties: ['hs_pipeline_stage', 'client__cloned_'],
        limit: 5
      });
    } catch (e3) { test3err = e3.message; }
    res.json({
      test1_pipeline_only: { total: test1.total },
      test2_IN_stages: test2err ? { error: test2err } : { total: test2.total },
      test3_single_stage: test3err ? { error: test3err } : { total: test3.total }
    });
  } catch (e) {
    res.json({ error: e.message });
  }
});

app.get('/api/debug/schemas', async function(req, res) {
  try {
    var schemas = await hubspotGet('https://api.hubapi.com/crm/v3/schemas');
    var results = schemas.results || schemas;
    var summary = results.map(function(s) {
      return {
        objectTypeId: s.objectTypeId,
        name: s.name,
        labels: s.labels,
        fullyQualifiedName: s.fullyQualifiedName
      };
    });
    // Also fetch jobs properties if jobs schema found
    var jobsSchema = results.find(function(s) { return s.name === 'jobs'; });
    var jobsProps = null;
    if (jobsSchema) {
      try {
        var propsData = await hubspotGet('https://api.hubapi.com/crm/v3/properties/' + jobsSchema.objectTypeId);
        jobsProps = (propsData.results || []).map(function(p) {
          return { name: p.name, label: p.label, type: p.type };
        });
      } catch (pe) { jobsProps = { error: pe.message }; }
    }
    // Also fetch endorsements (applications) properties and pipelines
    var endorseSchema = results.find(function(s) { return s.name === 'endorsements'; });
    var endorseProps = null;
    var endorsePipelines = null;
    if (endorseSchema) {
      try {
        var ePropsData = await hubspotGet('https://api.hubapi.com/crm/v3/properties/' + endorseSchema.objectTypeId);
        endorseProps = (ePropsData.results || []).map(function(p) {
          return { name: p.name, label: p.label, type: p.type };
        });
      } catch (epe) { endorseProps = { error: epe.message }; }
      try {
        endorsePipelines = await hubspotGet('https://api.hubapi.com/crm/v3/pipelines/' + endorseSchema.objectTypeId);
      } catch (eple) { endorsePipelines = { error: eple.message }; }
    }
    res.json({ count: summary.length, schemas: summary, jobsProperties: jobsProps, endorsementsProperties: endorseProps, endorsementsPipelines: endorsePipelines });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

function sleep(ms) { return new Promise(function(r) { setTimeout(r, ms); }); }

async function fetchAllPages(baseBody) {
  var results = [];
  var after = undefined;
  var hasMore = true;
  var page = 0;
  while (hasMore) {
    var body = Object.assign({}, baseBody, { limit: 200 });
    if (after) body.after = after;
    // Rate limit: wait 500ms between pages to avoid HubSpot 429 errors
    if (page > 0) await sleep(500);
    var data;
    try {
      data = await hubspotSearch(body);
    } catch (err) {
      if (err.message && err.message.indexOf('429') !== -1) {
        console.log('[HubSpot] Rate limited on page ' + page + ', waiting 5s and retrying...');
        await sleep(5000);
        try {
          data = await hubspotSearch(body);
        } catch (err2) {
          if (err2.message && err2.message.indexOf('429') !== -1) {
            console.log('[HubSpot] Still rate limited, waiting 10s...');
            await sleep(10000);
            data = await hubspotSearch(body);
          } else {
            throw err2;
          }
        }
      } else {
        throw err;
      }
    }
    results = results.concat(data.results || []);
    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
    } else {
      hasMore = false;
    }
    page++;
  }
  return results;
}

// Cache ticket data for 3 minutes to avoid slow repeated HubSpot fetches
var ticketCache = { data: null, timestamp: 0 };
var CACHE_TTL = 5 * 60 * 1000; // 5 minutes

app.get('/api/tickets', async function(req, res) {
  try {
    // Return cached data if fresh
    if (ticketCache.data && (Date.now() - ticketCache.timestamp) < CACHE_TTL) {
      console.log('[Cache] Returning cached ticket data (' + Math.round((Date.now() - ticketCache.timestamp) / 1000) + 's old)');
      return res.json(ticketCache.data);
    }

    console.log('[HubSpot] Fetching fresh ticket data...');
    var cutoff = Date.now() - (120 * 24 * 60 * 60 * 1000);
    var cutoffStr = String(cutoff);

    var createdResults = await fetchAllPages({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'createdate', operator: 'GTE', value: cutoffStr }
        ]
      }],
      properties: ['createdate', 'assignment_type', 'hs_pipeline', 'job_source', 'subject'],
      sorts: [{ propertyName: 'createdate', direction: 'DESCENDING' }]
    });

    var offboardResults = await fetchAllPages({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'offboarding_date', operator: 'GTE', value: cutoffStr }
        ]
      }],
      properties: ['offboarding_date', 'assignment_type', 'hs_pipeline', 'job_source', 'subject']
    });

    var raw = createdResults.map(function(r) {
      return {
        d: r.properties.createdate,
        t: r.properties.assignment_type || 'Unknown',
        p: r.properties.hs_pipeline,
        s: r.properties.job_source || '',
        n: r.properties.subject || ''
      };
    });

    var offboard = offboardResults.map(function(r) {
      return {
        o: r.properties.offboarding_date,
        t: r.properties.assignment_type || 'Unknown',
        s: r.properties.job_source || '',
        n: r.properties.subject || ''
      };
    });

    var now = new Date();
    var pht = new Date(now.getTime() + (8 * 60 * 60 * 1000));
    var months = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    var h = pht.getUTCHours();
    var ampm = h >= 12 ? 'PM' : 'AM';
    var h12 = h % 12 || 12;
    var timestamp = months[pht.getUTCMonth()] + ' ' + pht.getUTCDate() + ', ' + pht.getUTCFullYear() + ' ' + h12 + ':' + String(pht.getUTCMinutes()).padStart(2, '0') + ' ' + ampm + ' (GMT+8)';

    var responseData = {
      raw: raw,
      offboard: offboard,
      timestamp: timestamp,
      counts: { created: raw.length, offboarded: offboard.length }
    };

    // Cache the response
    ticketCache.data = responseData;
    ticketCache.timestamp = Date.now();
    console.log('[Cache] Ticket data cached (' + raw.length + ' created, ' + offboard.length + ' offboarded)');

    res.json(responseData);
  } catch (err) {
    console.error('API Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ===== Google Chat scheduled posting =====
var GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;
var DASHBOARD_URL = process.env.DASHBOARD_URL || process.env.APP_URL || ('http://localhost:' + PORT);

function getFTEWeight(type) {
  if (type === 'Full-Time') return 1;
  if (type === 'Part-Time-Under-20-Hours') return 0.25;
  return 0.5;
}

function fmtFTE(val) {
  if (val % 1 === 0) return val.toFixed(0);
  if ((val * 4) % 1 === 0 && val % 0.5 !== 0) return val.toFixed(2);
  return val.toFixed(1);
}

function getMonthKey(dateStr) {
  var d = new Date(dateStr);
  var gmt8 = new Date(d.getTime() + (8 * 60 * 60 * 1000));
  return gmt8.getUTCFullYear() + '-' + String(gmt8.getUTCMonth() + 1).padStart(2, '0');
}

function sleep(ms) { return new Promise(function(r) { setTimeout(r, ms); }); }

// Parse number from sheet cell that may have commas (e.g. "5,565" -> 5565)
function parseSheetNum(val) {
  if (typeof val === 'number') return val;
  if (!val) return 0;
  return parseFloat(String(val).replace(/,/g, '')) || 0;
}

async function fetchAllPagesWithRetry(baseBody) {
  var results = [];
  var after = undefined;
  var hasMore = true;
  var page = 0;
  while (hasMore) {
    var body = Object.assign({}, baseBody, { limit: 200 });
    if (after) body.after = after;
    if (page > 0) await sleep(400);
    var data;
    try {
      data = await hubspotSearch(body);
    } catch (err) {
      if (err.message && err.message.indexOf('429') !== -1) {
        console.log('Rate limited, waiting 3s and retrying...');
        await sleep(3000);
        data = await hubspotSearch(body);
      } else {
        throw err;
      }
    }
    results = results.concat(data.results || []);
    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
    } else {
      hasMore = false;
    }
    page++;
  }
  return results;
}

async function postGChatUpdate() {
  if (!GCHAT_WEBHOOK) {
    console.log('GCHAT_WEBHOOK not set, skipping Google Chat post');
    return;
  }

  try {
    console.log('[GChat] Fetching data from HubSpot...');
    var cutoff = Date.now() - (120 * 24 * 60 * 60 * 1000);
    var cutoffStr = String(cutoff);

    var createdResults = await fetchAllPagesWithRetry({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'createdate', operator: 'GTE', value: cutoffStr }
        ]
      }],
      properties: ['createdate', 'assignment_type', 'hs_pipeline'],
      sorts: [{ propertyName: 'createdate', direction: 'DESCENDING' }]
    });

    var offboardResults = await fetchAllPagesWithRetry({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'offboarding_date', operator: 'GTE', value: cutoffStr }
        ]
      }],
      properties: ['offboarding_date', 'assignment_type', 'hs_pipeline']
    });

    console.log('[GChat] Fetched: ' + createdResults.length + ' created, ' + offboardResults.length + ' offboarded');

    // Build current month summary
    var now = new Date();
    var gmt8 = new Date(now.getTime() + (8 * 60 * 60 * 1000));
    var curMonthKey = gmt8.getUTCFullYear() + '-' + String(gmt8.getUTCMonth() + 1).padStart(2, '0');
    var monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    var monthLabel = monthNames[gmt8.getUTCMonth()] + ' ' + gmt8.getUTCFullYear();

    // Count by type
    var types = ['Full-Time', 'Part-Time', 'Part-Time-Under-20-Hours', 'Output-Based', 'Project-Based', 'Trial'];
    var hire = {}, churn = {};
    types.forEach(function(t) { hire[t] = 0; churn[t] = 0; });
    hire._total = 0; churn._total = 0; hire._fte = 0; churn._fte = 0;

    createdResults.forEach(function(r) {
      var k = getMonthKey(r.properties.createdate);
      if (k !== curMonthKey) return;
      var type = r.properties.assignment_type || 'Unknown';
      if (hire[type] === undefined) hire[type] = 0;
      hire[type]++;
      hire._total++;
      hire._fte += getFTEWeight(type);
    });

    offboardResults.forEach(function(r) {
      var oDate = r.properties.offboarding_date;
      if (!oDate) return;
      var k = getMonthKey(oDate + 'T00:00:00Z');
      if (k !== curMonthKey) return;
      var type = r.properties.assignment_type || 'Unknown';
      if (churn[type] === undefined) churn[type] = 0;
      churn[type]++;
      churn._total++;
      churn._fte += getFTEWeight(type);
    });

    var netFTE = Math.round((hire._fte - churn._fte) * 100) / 100;
    var netPrefix = netFTE >= 0 ? '+' : '';
    var netEmoji = netFTE > 0 ? '\u{1F4C8}' : (netFTE < 0 ? '\u{1F4C9}' : '\u{2796}');

    var h = gmt8.getUTCHours();
    var ampm = h >= 12 ? 'PM' : 'AM';
    var h12 = h % 12 || 12;
    var timeStr = monthNames[gmt8.getUTCMonth()] + ' ' + gmt8.getUTCDate() + ', ' + gmt8.getUTCFullYear() + ' ' + h12 + ':' + String(gmt8.getUTCMinutes()).padStart(2, '0') + ' ' + ampm + ' (GMT+8)';

    var typeLabels = {
      'Full-Time': 'Full Time', 'Part-Time': 'Part Time',
      'Part-Time-Under-20-Hours': 'PT Under 20hrs',
      'Output-Based': 'Output-Based', 'Project-Based': 'Project-Based', 'Trial': 'Trial'
    };

    var message = {
      cardsV2: [{
        cardId: 'fte-update',
        card: {
          header: {
            title: 'FTE Running Update',
            subtitle: 'Current Month \u2014 ' + monthLabel,
            imageUrl: 'https://fonts.gstatic.com/s/i/short-term/release/googlesymbols/analytics/default/48px.svg',
            imageType: 'CIRCLE'
          },
          sections: [
            {
              header: '<b>SUMMARY</b>',
              widgets: [
                { decoratedText: { topLabel: 'NEW HIRES (Headcount \u2192 FTE)', text: '<b>' + hire._total + ' HC  \u2192  ' + fmtFTE(Math.round(hire._fte * 100) / 100) + ' FTE</b>', startIcon: { knownIcon: 'PERSON' } } },
                { decoratedText: { topLabel: 'CHURN (Headcount \u2192 FTE)', text: '<b>' + churn._total + ' HC  \u2192  ' + fmtFTE(Math.round(churn._fte * 100) / 100) + ' FTE</b>', startIcon: { knownIcon: 'MEMBERSHIP' } } },
                { decoratedText: { topLabel: 'NET FTE ' + netEmoji, text: '<b><font color="' + (netFTE >= 0 ? '#16a34a' : '#dc2626') + '">' + netPrefix + fmtFTE(netFTE) + '</font></b>', startIcon: { knownIcon: 'BOOKMARK' } } }
              ]
            },
            {
              header: '<b>BY CONTRACT TYPE</b>  (Hires | Churn)',
              widgets: (function() {
                var widgets = [];
                types.forEach(function(t) {
                  var hc = hire[t] || 0;
                  var ch = churn[t] || 0;
                  if (hc === 0 && ch === 0) return;
                  widgets.push({ decoratedText: { topLabel: (typeLabels[t] || t).toUpperCase(), text: '<b>' + hc + '</b>  |  <b>' + ch + '</b>' } });
                });
                if (widgets.length === 0) widgets.push({ decoratedText: { text: 'No data for this month yet' } });
                return widgets;
              })()
            },
            {
              widgets: [
                { decoratedText: { topLabel: 'Data as of', text: timeStr } },
                { buttonList: { buttons: [{ text: 'OPEN DASHBOARD', onClick: { openLink: { url: DASHBOARD_URL } }, color: { red: 0.15, green: 0.39, blue: 0.92, alpha: 1 } }] } }
              ]
            }
          ]
        }
      }]
    };

    // Post to Google Chat
    var body = JSON.stringify(message);
    var urlObj = new URL(GCHAT_WEBHOOK);
    await new Promise(function(resolve, reject) {
      var https = require('https');
      var req = https.request({
        hostname: urlObj.hostname,
        path: urlObj.pathname + urlObj.search,
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
      }, function(res) {
        var data = '';
        res.on('data', function(chunk) { data += chunk; });
        res.on('end', function() {
          if (res.statusCode >= 200 && res.statusCode < 300) resolve(data);
          else reject(new Error('Google Chat error ' + res.statusCode + ': ' + data));
        });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });

    console.log('[GChat] Posted successfully at ' + timeStr);
  } catch (err) {
    console.error('[GChat] FAILED:', err.message);
  }
}

// Schedule Google Chat posts at 8AM and 8PM GMT+8
// 8AM GMT+8 = 0:00 UTC, 8PM GMT+8 = 12:00 UTC
function scheduleGChatPosts() {
  if (!GCHAT_WEBHOOK) {
    console.log('No GCHAT_WEBHOOK set, skipping scheduler');
    return;
  }

  function msUntilNextSlot() {
    var now = new Date();
    var utcH = now.getUTCHours();
    var utcM = now.getUTCMinutes();
    var utcS = now.getUTCSeconds();
    // Target hours in UTC: 0 (8AM GMT+8) and 12 (8PM GMT+8)
    var slots = [0, 12];
    var currentMins = utcH * 60 + utcM;
    var nextMins = null;
    for (var i = 0; i < slots.length; i++) {
      var slotMins = slots[i] * 60;
      if (slotMins > currentMins) { nextMins = slotMins; break; }
    }
    if (nextMins === null) nextMins = slots[0] * 60 + 24 * 60; // next day 0:00 UTC
    var diffMs = (nextMins - currentMins) * 60 * 1000 - utcS * 1000;
    return diffMs;
  }

  function scheduleNext() {
    var ms = msUntilNextSlot();
    var nextTime = new Date(Date.now() + ms);
    var gmt8 = new Date(nextTime.getTime() + (8 * 60 * 60 * 1000));
    console.log('[GChat] Next post scheduled in ' + Math.round(ms / 60000) + ' minutes (at ' + gmt8.toISOString().substring(0, 16) + ' GMT+8)');
    setTimeout(function() {
      postGChatUpdate().then(function() {
        scheduleNext();
      }).catch(function() {
        scheduleNext();
      });
    }, ms);
  }

  // Only post at scheduled times — do NOT post on startup/redeploy
  console.log('[GChat] Scheduler started (posts at 8AM and 8PM GMT+8 only)');
  scheduleNext();
}

// ===== Monthly KPI Reports =====
var REPORT_SHEET_ID = process.env.REPORT_SHEET_ID;
var REPORT_PROJECTS_GID = process.env.REPORT_PROJECTS_GID || '1'; // second tab
var GOOGLE_SA_KEY = null;
try {
  if (process.env.GOOGLE_SERVICE_ACCOUNT_KEY) {
    GOOGLE_SA_KEY = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
  }
} catch (e) {
  console.error('Failed to parse GOOGLE_SERVICE_ACCOUNT_KEY:', e.message);
}

// KPI column definitions — order must match the sheet header row
var REPORT_COLUMNS = [
  'month',
  // Automated - Company
  'active_outsource_fte', 'monthly_contract_value',
  // Automated - Marketing/Ads
  'google_ads_spend', 'google_ads_conversions', 'cost_per_lead',
  // Automated - Hires
  'total_fte_hires', 'hires_full_time', 'hires_part_time', 'hires_pt_under_20',
  'hires_project_based', 'hires_output_based',
  // Automated - New Jobs
  'new_jobs_created', 'new_jobs_backfill', 'new_jobs_existing_client', 'new_jobs_new_client',
  // Automated - Offboardings
  'lost_ftes', 'offboardings_full_time', 'offboardings_part_time', 'offboardings_pt_under_20',
  'offboardings_project_based', 'offboardings_output_based',
  'pct_offboardings_under_30_days', 'fte_churn_rate', 'backfill_ftes_hired',
  // Automated - Client mix
  'new_client_hires', 'existing_client_hires',
  // Manual KPIs
  'monthly_contract_margin_pct', 'marketing_qualified_leads',
  'lead_to_new_job_conv_rate', 'lead_to_closed_fte_conv_rate',
  'time_to_first_candidate_submission', 'candidate_endorsements_per_recruitment_hc',
  'internal_staff_headcount', 'recruitment_team_hc', 'sales_team_hc',
  'expansion_rate',
  // Metadata
  'generated_at', 'last_edited_at'
];

var PROJECT_COLUMNS = ['month', 'pillar', 'project', 'status', 'description'];
var QUARTERLY_COLUMNS = ['quarter', 'category', 'goal', 'status', 'notes'];

// Google Sheets API v4 — JWT auth using built-in crypto (zero extra deps)
var googleTokenCache = { token: null, expiry: 0 };

function createGoogleJWT() {
  if (!GOOGLE_SA_KEY) throw new Error('Google service account key not configured');
  var now = Math.floor(Date.now() / 1000);
  var header = { alg: 'RS256', typ: 'JWT' };
  var payload = {
    iss: GOOGLE_SA_KEY.client_email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600
  };
  var headerB64 = Buffer.from(JSON.stringify(header)).toString('base64url');
  var payloadB64 = Buffer.from(JSON.stringify(payload)).toString('base64url');
  var toSign = headerB64 + '.' + payloadB64;
  var sign = crypto.createSign('RSA-SHA256');
  sign.update(toSign);
  var signature = sign.sign(GOOGLE_SA_KEY.private_key, 'base64url');
  return toSign + '.' + signature;
}

async function getGoogleAccessToken() {
  var now = Date.now();
  if (googleTokenCache.token && now < googleTokenCache.expiry) {
    return googleTokenCache.token;
  }
  var jwt = createGoogleJWT();
  var body = 'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=' + jwt;
  var resp = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body
  });
  if (!resp.ok) {
    var txt = await resp.text();
    throw new Error('Google token exchange failed: ' + txt);
  }
  var data = await resp.json();
  googleTokenCache = { token: data.access_token, expiry: now + (data.expires_in - 60) * 1000 };
  return data.access_token;
}

async function sheetsGet(spreadsheetId, range, opts) {
  var token = await getGoogleAccessToken();
  var url = 'https://sheets.googleapis.com/v4/spreadsheets/' + encodeURIComponent(spreadsheetId)
    + '/values/' + encodeURIComponent(range);
  if (opts && opts.valueRenderOption) {
    url += '?valueRenderOption=' + opts.valueRenderOption;
  }
  var resp = await fetch(url, { headers: { 'Authorization': 'Bearer ' + token } });
  if (!resp.ok) {
    var txt = await resp.text();
    throw new Error('Sheets GET failed (' + resp.status + '): ' + txt);
  }
  return resp.json();
}

async function sheetsUpdate(spreadsheetId, range, values) {
  var token = await getGoogleAccessToken();
  var url = 'https://sheets.googleapis.com/v4/spreadsheets/' + encodeURIComponent(spreadsheetId)
    + '/values/' + encodeURIComponent(range) + '?valueInputOption=RAW';
  var resp = await fetch(url, {
    method: 'PUT',
    headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
    body: JSON.stringify({ range: range, majorDimension: 'ROWS', values: values })
  });
  if (!resp.ok) {
    var txt = await resp.text();
    throw new Error('Sheets PUT failed (' + resp.status + '): ' + txt);
  }
  return resp.json();
}

async function sheetsAppend(spreadsheetId, range, values) {
  var token = await getGoogleAccessToken();
  var url = 'https://sheets.googleapis.com/v4/spreadsheets/' + encodeURIComponent(spreadsheetId)
    + '/values/' + encodeURIComponent(range) + ':append?valueInputOption=RAW&insertDataOption=INSERT_ROWS';
  var resp = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
    body: JSON.stringify({ range: range, majorDimension: 'ROWS', values: values })
  });
  if (!resp.ok) {
    var txt = await resp.text();
    throw new Error('Sheets APPEND failed (' + resp.status + '): ' + txt);
  }
  return resp.json();
}

// Ensure required tabs exist in the report spreadsheet
var sheetsInitialised = false;
async function ensureSheetTabs() {
  if (sheetsInitialised || !REPORT_SHEET_ID || !GOOGLE_SA_KEY) return;
  try {
    var token = await getGoogleAccessToken();
    // Get spreadsheet metadata to check existing sheets
    var metaResp = await fetch(
      'https://sheets.googleapis.com/v4/spreadsheets/' + encodeURIComponent(REPORT_SHEET_ID) + '?fields=sheets.properties',
      { headers: { 'Authorization': 'Bearer ' + token } }
    );
    if (!metaResp.ok) throw new Error('Failed to read spreadsheet metadata');
    var meta = await metaResp.json();
    var existingSheets = (meta.sheets || []).map(function(s) { return s.properties.title; });

    var requests = [];
    // Ensure "Sheet1" exists (it should by default)
    // Ensure "Project Updates" tab exists
    if (existingSheets.indexOf('Project Updates') === -1) {
      requests.push({ addSheet: { properties: { title: 'Project Updates' } } });
    }
    if (existingSheets.indexOf('Quarterly Goals') === -1) {
      requests.push({ addSheet: { properties: { title: 'Quarterly Goals' } } });
    }
    if (existingSheets.indexOf('Gong Cache') === -1) {
      requests.push({ addSheet: { properties: { title: 'Gong Cache' } } });
    }
    if (existingSheets.indexOf('Conversion Cache') === -1) {
      requests.push({ addSheet: { properties: { title: 'Conversion Cache' } } });
    }
    if (requests.length > 0) {
      var batchResp = await fetch(
        'https://sheets.googleapis.com/v4/spreadsheets/' + encodeURIComponent(REPORT_SHEET_ID) + ':batchUpdate',
        {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
          body: JSON.stringify({ requests: requests })
        }
      );
      if (!batchResp.ok) {
        var errText = await batchResp.text();
        console.error('Failed to create sheet tabs:', errText);
      } else {
        console.log('[Report] Created missing sheet tabs');
      }
    }

    // Ensure headers exist in Sheet1
    var headerCheck = await sheetsGet(REPORT_SHEET_ID, 'Sheet1!A1:A1');
    if (!headerCheck.values || !headerCheck.values[0] || headerCheck.values[0][0] !== 'month') {
      await sheetsUpdate(REPORT_SHEET_ID, 'Sheet1!A1:AQ1', [REPORT_COLUMNS]);
      console.log('[Report] Wrote KPI headers to Sheet1');
    }

    // Ensure headers exist in Project Updates
    try {
      var projHeaderCheck = await sheetsGet(REPORT_SHEET_ID, 'Project Updates!A1:A1');
      if (!projHeaderCheck.values || !projHeaderCheck.values[0] || projHeaderCheck.values[0][0] !== 'month') {
        await sheetsUpdate(REPORT_SHEET_ID, 'Project Updates!A1:E1', [PROJECT_COLUMNS]);
        console.log('[Report] Wrote Project Updates headers');
      }
    } catch (e) {
      // Tab may have just been created, try writing headers
      await sheetsUpdate(REPORT_SHEET_ID, 'Project Updates!A1:E1', [PROJECT_COLUMNS]);
    }

    // Ensure headers exist in Quarterly Goals (must have 5 columns: quarter, category, goal, status, notes)
    try {
      var qHeaderCheck = await sheetsGet(REPORT_SHEET_ID, 'Quarterly Goals!A1:E1');
      var qHeaders = (qHeaderCheck.values && qHeaderCheck.values[0]) || [];
      if (qHeaders[0] !== 'quarter' || qHeaders[1] !== 'category' || qHeaders.length < 5) {
        await sheetsUpdate(REPORT_SHEET_ID, 'Quarterly Goals!A1:E1', [QUARTERLY_COLUMNS]);
        console.log('[Report] Wrote/updated Quarterly Goals headers (was: ' + JSON.stringify(qHeaders) + ')');
      }
    } catch (e) {
      await sheetsUpdate(REPORT_SHEET_ID, 'Quarterly Goals!A1:E1', [QUARTERLY_COLUMNS]);
    }

    // Ensure headers exist in Gong Cache
    try {
      var gongHeaderCheck = await sheetsGet(REPORT_SHEET_ID, 'Gong Cache!A1:C1');
      var gongHeaders = (gongHeaderCheck.values && gongHeaderCheck.values[0]) || [];
      if (gongHeaders[0] !== 'month' || gongHeaders[1] !== 'discovery_calls') {
        await sheetsUpdate(REPORT_SHEET_ID, 'Gong Cache!A1:C1', [['month', 'discovery_calls', 'updated_at']]);
        console.log('[Report] Wrote Gong Cache headers');
      }
    } catch (e) {
      await sheetsUpdate(REPORT_SHEET_ID, 'Gong Cache!A1:C1', [['month', 'discovery_calls', 'updated_at']]);
    }

    // Ensure headers exist in Conversion Cache
    try {
      var convHeaderCheck = await sheetsGet(REPORT_SHEET_ID, 'Conversion Cache!A1:C1');
      var convHeaders = (convHeaderCheck.values && convHeaderCheck.values[0]) || [];
      if (convHeaders[0] !== 'month') {
        await sheetsUpdate(REPORT_SHEET_ID, 'Conversion Cache!A1:C1', [['month', 'data_json', 'updated_at']]);
        console.log('[Report] Wrote Conversion Cache headers');
      }
    } catch (e) {
      await sheetsUpdate(REPORT_SHEET_ID, 'Conversion Cache!A1:C1', [['month', 'data_json', 'updated_at']]);
    }

    sheetsInitialised = true;
    console.log('[Report] Sheet initialisation complete');
  } catch (err) {
    console.error('[Report] Sheet init error:', err.message);
  }
}

// Read all report rows from sheet, returning array of objects keyed by column name
async function readReportSheet() {
  if (!REPORT_SHEET_ID) throw new Error('REPORT_SHEET_ID not configured');
  var data = await sheetsGet(REPORT_SHEET_ID, 'Sheet1!A:AQ');
  var rows = data.values || [];
  if (rows.length < 2) return []; // no data rows
  var headers = rows[0];
  var results = [];
  for (var i = 1; i < rows.length; i++) {
    var row = rows[i];
    if (!row[0]) continue; // skip empty rows
    var obj = {};
    for (var j = 0; j < headers.length; j++) {
      obj[headers[j]] = row[j] || '';
    }
    results.push(obj);
  }
  return results;
}

// Read project updates from second tab
async function readProjectsSheet() {
  if (!REPORT_SHEET_ID) throw new Error('REPORT_SHEET_ID not configured');
  var data = await sheetsGet(REPORT_SHEET_ID, 'Project Updates!A:E');
  var rows = data.values || [];
  if (rows.length < 2) return [];
  var headers = rows[0];
  var results = [];
  for (var i = 1; i < rows.length; i++) {
    var row = rows[i];
    if (!row[0]) continue;
    var obj = {};
    for (var j = 0; j < headers.length; j++) {
      obj[headers[j]] = row[j] || '';
    }
    results.push(obj);
  }
  return results;
}

// Write or update a report row for a given month
async function writeReportRow(month, kpiData) {
  if (!REPORT_SHEET_ID) throw new Error('REPORT_SHEET_ID not configured');
  // Read existing data to find the row for this month
  var existing = await sheetsGet(REPORT_SHEET_ID, 'Sheet1!A:A');
  var rows = existing.values || [];
  var rowIdx = -1;
  for (var i = 1; i < rows.length; i++) {
    if (rows[i][0] === month) { rowIdx = i; break; }
  }
  // Build the row values in column order
  var rowValues = REPORT_COLUMNS.map(function(col) {
    if (col === 'month') return month;
    return kpiData[col] !== undefined ? String(kpiData[col]) : '';
  });
  if (rowIdx >= 0) {
    // Update existing row
    var range = 'Sheet1!A' + (rowIdx + 1) + ':AQ' + (rowIdx + 1);
    await sheetsUpdate(REPORT_SHEET_ID, range, [rowValues]);
  } else {
    // Append new row (ensure headers exist first)
    if (rows.length === 0) {
      await sheetsUpdate(REPORT_SHEET_ID, 'Sheet1!A1:AQ1', [REPORT_COLUMNS]);
    }
    await sheetsAppend(REPORT_SHEET_ID, 'Sheet1!A:AQ', [rowValues]);
  }
}

// Write project updates for a month (replace all for that month)
async function writeProjectUpdates(month, projects) {
  if (!REPORT_SHEET_ID) throw new Error('REPORT_SHEET_ID not configured');
  // Read all existing project rows
  var existing = await sheetsGet(REPORT_SHEET_ID, 'Project Updates!A:E');
  var rows = existing.values || [];
  // Keep header + rows for other months, replace rows for target month
  var newRows = [];
  if (rows.length > 0) newRows.push(rows[0]); // header
  else newRows.push(PROJECT_COLUMNS);
  for (var i = 1; i < rows.length; i++) {
    if (rows[i][0] !== month) newRows.push(rows[i]);
  }
  for (var j = 0; j < projects.length; j++) {
    var p = projects[j];
    newRows.push([month, p.pillar || '', p.project || '', p.status || '', p.description || '']);
  }
  // Clear and rewrite the entire sheet
  await sheetsUpdate(REPORT_SHEET_ID, 'Project Updates!A1:E' + Math.max(newRows.length, rows.length + 1),
    newRows.concat(Array(Math.max(0, rows.length - newRows.length)).fill(['', '', '', '', '']))
  );
}

// Read quarterly goals from sheet
async function readQuarterlySheet() {
  if (!REPORT_SHEET_ID) throw new Error('REPORT_SHEET_ID not configured');
  var data = await sheetsGet(REPORT_SHEET_ID, 'Quarterly Goals!A:E');
  var rows = data.values || [];
  if (rows.length < 2) return [];
  var headers = rows[0];
  var results = [];
  for (var i = 1; i < rows.length; i++) {
    var row = rows[i];
    if (!row[0]) continue;
    var obj = {};
    for (var j = 0; j < headers.length; j++) {
      obj[headers[j]] = row[j] || '';
    }
    results.push(obj);
  }
  return results;
}

// Write quarterly goals for specific quarters (replace all for those quarters)
async function writeQuarterlyGoals(quarters, goals) {
  if (!REPORT_SHEET_ID) throw new Error('REPORT_SHEET_ID not configured');
  var existing = await sheetsGet(REPORT_SHEET_ID, 'Quarterly Goals!A:E');
  var rows = existing.values || [];
  // Keep header + rows for quarters NOT being updated
  var quartersSet = {};
  for (var q = 0; q < quarters.length; q++) quartersSet[quarters[q]] = true;
  var newRows = [];
  if (rows.length > 0) newRows.push(rows[0]);
  else newRows.push(QUARTERLY_COLUMNS);
  for (var i = 1; i < rows.length; i++) {
    if (!quartersSet[rows[i][0]]) newRows.push(rows[i]);
  }
  for (var j = 0; j < goals.length; j++) {
    var g = goals[j];
    newRows.push([g.quarter || '', g.category || '', g.goal || '', g.status || '', g.notes || '']);
  }
  await sheetsUpdate(REPORT_SHEET_ID, 'Quarterly Goals!A1:E' + Math.max(newRows.length, rows.length + 1),
    newRows.concat(Array(Math.max(0, rows.length - newRows.length)).fill(['', '', '', '', '']))
  );
}

// ===== Report API Endpoints =====

// GET /api/reports — list all available months
app.get('/api/reports', async function(req, res) {
  try {
    if (!REPORT_SHEET_ID || !GOOGLE_SA_KEY) {
      return res.json({ error: 'Reports not configured', months: [] });
    }
    await ensureSheetTabs();
    var reports = await readReportSheet();
    var months = reports.map(function(r) {
      return {
        month: r.month,
        generated_at: r.generated_at || null,
        last_edited_at: r.last_edited_at || null,
        total_fte_hires: r.total_fte_hires || null,
        lost_ftes: r.lost_ftes || null,
        google_ads_spend: r.google_ads_spend || null
      };
    }).sort(function(a, b) { return b.month.localeCompare(a.month); });
    res.json({ months: months });
  } catch (err) {
    console.error('GET /api/reports error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/report?month=YYYY-MM — return full data for a month + previous month
app.get('/api/report', async function(req, res) {
  try {
    var month = req.query.month;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) {
      return res.status(400).json({ error: 'Invalid month format. Use YYYY-MM' });
    }
    if (!REPORT_SHEET_ID || !GOOGLE_SA_KEY) {
      return res.status(500).json({ error: 'Reports not configured' });
    }
    await ensureSheetTabs();
    var reports = await readReportSheet();
    var projects = await readProjectsSheet();

    var current = null;
    var previous = null;
    // Find current month
    for (var i = 0; i < reports.length; i++) {
      if (reports[i].month === month) { current = reports[i]; break; }
    }
    // Compute previous month key
    var parts = month.split('-');
    var yr = parseInt(parts[0]);
    var mo = parseInt(parts[1]);
    if (mo === 1) { yr--; mo = 12; } else { mo--; }
    var prevMonth = yr + '-' + String(mo).padStart(2, '0');
    for (var j = 0; j < reports.length; j++) {
      if (reports[j].month === prevMonth) { previous = reports[j]; break; }
    }

    // Filter projects for this month
    var monthProjects = projects.filter(function(p) { return p.month === month; });

    // Parse numeric fields
    function parseNum(obj) {
      if (!obj) return null;
      var result = { month: obj.month };
      for (var key in obj) {
        if (key === 'month' || key === 'generated_at' || key === 'last_edited_at') {
          result[key] = obj[key];
        } else {
          var v = parseFloat(obj[key]);
          result[key] = isNaN(v) ? (obj[key] || null) : v;
        }
      }
      return result;
    }

    res.json({
      month: month,
      current: parseNum(current),
      previous: parseNum(previous),
      previous_month: prevMonth,
      projects: monthProjects,
      exists: current !== null
    });
  } catch (err) {
    console.error('GET /api/report error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/report — save manual KPIs + project updates
app.post('/api/report', async function(req, res) {
  try {
    var month = req.body.month;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) {
      return res.status(400).json({ error: 'Invalid month format. Use YYYY-MM' });
    }
    if (!REPORT_SHEET_ID || !GOOGLE_SA_KEY) {
      return res.status(500).json({ error: 'Reports not configured' });
    }
    await ensureSheetTabs();

    // Read existing row to merge with manual updates
    var reports = await readReportSheet();
    var existing = {};
    for (var i = 0; i < reports.length; i++) {
      if (reports[i].month === month) { existing = reports[i]; break; }
    }

    // Merge manual KPI fields from request body
    var kpiData = Object.assign({}, existing);
    var manualFields = [
      'monthly_contract_margin_pct', 'marketing_qualified_leads',
      'lead_to_new_job_conv_rate', 'lead_to_closed_fte_conv_rate',
      'time_to_first_candidate_submission', 'candidate_endorsements_per_recruitment_hc',
      'internal_staff_headcount', 'recruitment_team_hc', 'sales_team_hc',
      'expansion_rate'
    ];
    for (var j = 0; j < manualFields.length; j++) {
      var f = manualFields[j];
      if (req.body[f] !== undefined) kpiData[f] = req.body[f];
    }
    kpiData.last_edited_at = new Date().toISOString();

    await writeReportRow(month, kpiData);

    // Save project updates if provided
    if (req.body.projects && Array.isArray(req.body.projects)) {
      await writeProjectUpdates(month, req.body.projects);
    }

    res.json({ success: true, month: month });
  } catch (err) {
    console.error('POST /api/report error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/quarterly?quarters=Q3 FY26,Q4 FY26 — return goals for specified quarters
app.get('/api/quarterly', async function(req, res) {
  try {
    var qParam = req.query.quarters;
    if (!qParam) return res.status(400).json({ error: 'Missing quarters parameter' });
    if (!REPORT_SHEET_ID || !GOOGLE_SA_KEY) {
      return res.json({ goals: [] });
    }
    await ensureSheetTabs();
    var allGoals = await readQuarterlySheet();
    var requested = qParam.split(',').map(function(s) { return s.trim(); });
    var filtered = allGoals.filter(function(g) { return requested.indexOf(g.quarter) >= 0; });
    res.json({ goals: filtered });
  } catch (err) {
    console.error('GET /api/quarterly error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/quarterly — save quarterly goals
app.post('/api/quarterly', async function(req, res) {
  try {
    if (!REPORT_SHEET_ID || !GOOGLE_SA_KEY) {
      return res.status(500).json({ error: 'Reports not configured' });
    }
    await ensureSheetTabs();
    var quarters = req.body.quarters; // array of quarter labels being saved
    var goals = req.body.goals; // array of {quarter, goal, status, notes}
    if (!quarters || !Array.isArray(quarters) || !goals || !Array.isArray(goals)) {
      return res.status(400).json({ error: 'Missing quarters and goals arrays' });
    }
    await writeQuarterlyGoals(quarters, goals);
    res.json({ success: true });
  } catch (err) {
    console.error('POST /api/quarterly error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/report/generate — pull automated KPIs from HubSpot + Google Ads
app.post('/api/report/generate', async function(req, res) {
  try {
    var month = req.body.month;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) {
      return res.status(400).json({ error: 'Invalid month format. Use YYYY-MM' });
    }
    if (!REPORT_SHEET_ID || !GOOGLE_SA_KEY) {
      return res.status(500).json({ error: 'Reports not configured' });
    }
    await ensureSheetTabs();

    console.log('[Report] Generating automated data for ' + month);
    var parts = month.split('-');
    var yr = parseInt(parts[0]);
    var mo = parseInt(parts[1]);
    // Month start/end in YYYY-MM-DD format
    var monthStart = month + '-01';
    var lastDay = new Date(yr, mo, 0).getDate();
    var monthEnd = month + '-' + String(lastDay).padStart(2, '0');
    // Timestamps for HubSpot date filters (milliseconds) — HubSpot requires timestamps, not date strings
    var startMs = String(new Date(monthStart + 'T00:00:00Z').getTime());
    var endMs = String(new Date(monthEnd + 'T23:59:59Z').getTime());

    // Read existing row to preserve manual fields
    var reports = await readReportSheet();
    var existing = {};
    for (var i = 0; i < reports.length; i++) {
      if (reports[i].month === month) { existing = reports[i]; break; }
    }

    var kpiData = Object.assign({}, existing);

    // ===== 1. HubSpot Tickets: Hires (onboarding_date in month) =====
    console.log('[Report] Fetching HubSpot hires...');
    var hireResults = await fetchAllPagesWithRetry({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'onboarding_date', operator: 'GTE', value: startMs },
          { propertyName: 'onboarding_date', operator: 'LTE', value: endMs }
        ]
      }],
      properties: ['onboarding_date', 'assignment_type', 'type_of_recruitment', 'hs_pipeline'],
      sorts: [{ propertyName: 'onboarding_date', direction: 'ASCENDING' }]
    });
    console.log('[Report] Hires found: ' + hireResults.length);

    // FTE weighting
    function fteWeight(type) {
      if (type === 'Full-Time') return 1;
      if (type === 'Part-Time') return 0.5;
      return 0.25; // PT-Under-20, Project-Based, Output-Based
    }

    var totalFTEHires = 0;
    var hiresByType = { 'Full-Time': 0, 'Part-Time': 0, 'Part-Time-Under-20-Hours': 0, 'Project-Based': 0, 'Output-Based': 0 };
    var newClientHires = 0, existingClientHires = 0, backfillHires = 0;
    var newJobsNewClient = 0, newJobsExistingClient = 0, newJobsBackfill = 0;

    for (var hi = 0; hi < hireResults.length; hi++) {
      var hp = hireResults[hi].properties;
      var aType = hp.assignment_type || 'Unknown';
      var recType = hp.type_of_recruitment || '';
      var w = fteWeight(aType);
      totalFTEHires += w;
      if (hiresByType[aType] !== undefined) hiresByType[aType] += w;

      // Categorize by recruitment type
      var recLower = recType.toLowerCase();
      if (recLower.indexOf('backfill') !== -1 || recLower.indexOf('back-fill') !== -1 || recLower.indexOf('replacement') !== -1) {
        backfillHires += w;
        newJobsBackfill++;
      } else if (recLower.indexOf('new client') !== -1 || recLower.indexOf('new_client') !== -1) {
        newClientHires += w;
        newJobsNewClient++;
      } else {
        existingClientHires += w;
        newJobsExistingClient++;
      }
    }

    kpiData.total_fte_hires = Math.round(totalFTEHires * 100) / 100;
    kpiData.hires_full_time = hiresByType['Full-Time'];
    kpiData.hires_part_time = hiresByType['Part-Time'];
    kpiData.hires_pt_under_20 = hiresByType['Part-Time-Under-20-Hours'];
    kpiData.hires_project_based = hiresByType['Project-Based'];
    kpiData.hires_output_based = hiresByType['Output-Based'];
    kpiData.new_client_hires = Math.round(newClientHires * 100) / 100;
    kpiData.existing_client_hires = Math.round(existingClientHires * 100) / 100;
    kpiData.backfill_ftes_hired = Math.round(backfillHires * 100) / 100;
    kpiData.new_jobs_created = hireResults.length;
    kpiData.new_jobs_backfill = newJobsBackfill;
    kpiData.new_jobs_existing_client = newJobsExistingClient;
    kpiData.new_jobs_new_client = newJobsNewClient;

    // ===== 2. HubSpot Tickets: Offboardings (offboarding_date in month) =====
    console.log('[Report] Fetching HubSpot offboardings...');
    var offResults = await fetchAllPagesWithRetry({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'offboarding_date', operator: 'GTE', value: startMs },
          { propertyName: 'offboarding_date', operator: 'LTE', value: endMs }
        ]
      }],
      properties: ['offboarding_date', 'assignment_type', 'onboarding_date', 'days_between_onboarding_offboarding', 'hs_pipeline'],
      sorts: [{ propertyName: 'offboarding_date', direction: 'ASCENDING' }]
    });
    console.log('[Report] Offboardings found: ' + offResults.length);

    var lostFTEs = 0;
    var offByType = { 'Full-Time': 0, 'Part-Time': 0, 'Part-Time-Under-20-Hours': 0, 'Project-Based': 0, 'Output-Based': 0 };
    var under30Count = 0;

    for (var oi = 0; oi < offResults.length; oi++) {
      var op = offResults[oi].properties;
      var oType = op.assignment_type || 'Unknown';
      var ow = fteWeight(oType);
      lostFTEs += ow;
      if (offByType[oType] !== undefined) offByType[oType] += ow;

      // Check <30 day offboardings
      var daysBetween = parseFloat(op.days_between_onboarding_offboarding);
      if (!isNaN(daysBetween) && daysBetween < 30) {
        under30Count++;
      } else if (op.onboarding_date && op.offboarding_date) {
        var onD = new Date(op.onboarding_date);
        var offD = new Date(op.offboarding_date);
        var diffDays = (offD - onD) / (1000 * 60 * 60 * 24);
        if (diffDays < 30) under30Count++;
      }
    }

    kpiData.lost_ftes = Math.round(lostFTEs * 100) / 100;
    kpiData.offboardings_full_time = offByType['Full-Time'];
    kpiData.offboardings_part_time = offByType['Part-Time'];
    kpiData.offboardings_pt_under_20 = offByType['Part-Time-Under-20-Hours'];
    kpiData.offboardings_project_based = offByType['Project-Based'];
    kpiData.offboardings_output_based = offByType['Output-Based'];
    kpiData.pct_offboardings_under_30_days = offResults.length > 0
      ? Math.round((under30Count / offResults.length) * 1000) / 10 : 0;

    // ===== 3. HubSpot Tickets: Active FTE (onboarded, not offboarded, as of month end) =====
    console.log('[Report] Fetching active FTE...');
    // Tickets with onboarding_date <= monthEnd and no offboarding_date (or offboarding_date > monthEnd)
    var activeResults = await fetchAllPagesWithRetry({
      filterGroups: [
        {
          filters: [
            { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
            { propertyName: 'onboarding_date', operator: 'LTE', value: endMs },
            { propertyName: 'offboarding_date', operator: 'NOT_HAS_PROPERTY' }
          ]
        },
        {
          filters: [
            { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
            { propertyName: 'onboarding_date', operator: 'LTE', value: endMs },
            { propertyName: 'offboarding_date', operator: 'GT', value: endMs }
          ]
        }
      ],
      properties: ['assignment_type', 'onboarding_date', 'offboarding_date'],
    });
    console.log('[Report] Active tickets found: ' + activeResults.length);

    var activeFTE = 0;
    for (var ai = 0; ai < activeResults.length; ai++) {
      var ap = activeResults[ai].properties;
      activeFTE += fteWeight(ap.assignment_type || 'Unknown');
    }
    kpiData.active_outsource_fte = Math.round(activeFTE * 100) / 100;

    // Churn rate
    kpiData.fte_churn_rate = activeFTE > 0
      ? Math.round((lostFTEs / activeFTE) * 1000) / 10 : 0;

    // ===== 4. Google Ads: Monthly spend + conversions =====
    console.log('[Report] Fetching Google Ads data...');
    try {
      var adsCsv = await fetchAdsCsv(ADS_CSV_URL);
      var adsRows = parseAdsCsv(adsCsv);
      var adsResult = processAdsData(adsRows);
      // Filter timeseries to target month
      var monthAds = adsResult.timeseries.filter(function(d) {
        return d.day >= monthStart && d.day <= monthEnd;
      });
      var adsSpend = 0, adsConversions = 0;
      for (var mi = 0; mi < monthAds.length; mi++) {
        adsSpend += monthAds[mi].cost;
        adsConversions += monthAds[mi].conversions;
      }
      kpiData.google_ads_spend = Math.round(adsSpend * 100) / 100;
      kpiData.google_ads_conversions = Math.round(adsConversions * 100) / 100;
      kpiData.cost_per_lead = adsConversions > 0
        ? Math.round((adsSpend / adsConversions) * 100) / 100 : 0;
      console.log('[Report] Ads: $' + kpiData.google_ads_spend + ' spend, ' + kpiData.google_ads_conversions + ' conversions');
    } catch (adsErr) {
      console.error('[Report] Ads data fetch failed:', adsErr.message);
      // Don't fail the whole generation if ads fail
    }

    // ===== 5. HubSpot Deals: Monthly Contract Value =====
    console.log('[Report] Fetching HubSpot deals for MCV...');
    try {
      var DEALS_API = 'https://api.hubapi.com/crm/v3/objects/deals/search';
      var dealHeaders = { 'Content-Type': 'application/json' };
      if (HUBSPOT_KEY && HUBSPOT_KEY.startsWith('pat-')) {
        dealHeaders['Authorization'] = 'Bearer ' + HUBSPOT_KEY;
      }
      // Search for active deals
      var dealResults = [];
      var dealAfter = undefined;
      var dealHasMore = true;
      while (dealHasMore) {
        var dealBody = {
          filterGroups: [{
            filters: [
              { propertyName: 'dealstage', operator: 'NOT_IN', values: ['closedlost', 'closedwon'] }
            ]
          }],
          properties: ['monthly_revenue_aud', 'mrr', 'amount', 'dealstage'],
          limit: 200
        };
        if (dealAfter) dealBody.after = dealAfter;
        var dealUrl = DEALS_API;
        if (HUBSPOT_KEY && !HUBSPOT_KEY.startsWith('pat-')) {
          dealUrl += '?hapikey=' + HUBSPOT_KEY;
        }
        if (dealResults.length > 0) await sleep(400);
        var dealResp = await fetch(dealUrl, {
          method: 'POST',
          headers: dealHeaders,
          body: JSON.stringify(dealBody)
        });
        if (dealResp.ok) {
          var dealData = await dealResp.json();
          dealResults = dealResults.concat(dealData.results || []);
          if (dealData.paging && dealData.paging.next && dealData.paging.next.after) {
            dealAfter = dealData.paging.next.after;
          } else {
            dealHasMore = false;
          }
        } else {
          console.error('[Report] Deals API error:', dealResp.status);
          dealHasMore = false;
        }
      }
      var mcv = 0;
      for (var di = 0; di < dealResults.length; di++) {
        var dp = dealResults[di].properties;
        var rev = parseFloat(dp.monthly_revenue_aud) || parseFloat(dp.mrr) || parseFloat(dp.amount) || 0;
        mcv += rev;
      }
      kpiData.monthly_contract_value = Math.round(mcv * 100) / 100;
      console.log('[Report] MCV: $' + kpiData.monthly_contract_value + ' from ' + dealResults.length + ' deals');
    } catch (dealErr) {
      console.error('[Report] Deals fetch failed:', dealErr.message);
    }

    // Set metadata
    kpiData.generated_at = new Date().toISOString();

    // Write to sheet
    await writeReportRow(month, kpiData);
    console.log('[Report] Saved report for ' + month);

    res.json({ success: true, month: month, kpis: kpiData });
  } catch (err) {
    console.error('POST /api/report/generate error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DEBUG: Check HubSpot token status
app.get('/api/debug/hs-status', async function(req, res) {
  try {
    var keyPrefix = HUBSPOT_KEY ? HUBSPOT_KEY.substring(0, 10) + '...' : 'NOT SET';
    var isPat = HUBSPOT_KEY && HUBSPOT_KEY.startsWith('pat-');
    // Test a minimal query
    var testBody = {
      filterGroups: [{ filters: [
        { propertyName: 'hs_pipeline', operator: 'EQ', value: '4483329' }
      ]}],
      properties: ['subject'],
      limit: 1
    };
    var url = HUBSPOT_API;
    var headers = { 'Content-Type': 'application/json' };
    if (isPat) {
      headers['Authorization'] = 'Bearer ' + HUBSPOT_KEY;
    } else {
      url = HUBSPOT_API + '?hapikey=' + HUBSPOT_KEY;
    }
    var testRes = await fetch(url, { method: 'POST', headers: headers, body: JSON.stringify(testBody) });
    var testText = await testRes.text();
    res.json({ keyPrefix: keyPrefix, isPat: isPat, testStatus: testRes.status, testResponse: testText.substring(0, 500) });
  } catch (e) {
    res.json({ error: e.message });
  }
});

// DEBUG: Test specific filter combinations
app.get('/api/debug/test-filters', async function(req, res) {
  var results = {};
  // Test 1: IN operator with pipelines
  try {
    var d = await hubspotSearch({
      filterGroups: [{ filters: [
        { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES }
      ]}], properties: ['subject'], limit: 1
    });
    results.test_in = { ok: true, total: d.total };
  } catch (e) { results.test_in = { ok: false, error: e.message.substring(0, 300) }; }
  // Test 2: Date with timestamp format
  var tStartMs = String(new Date('2026-04-01T00:00:00Z').getTime());
  var tEndMs = String(new Date('2026-04-30T23:59:59Z').getTime());
  try {
    var d2 = await hubspotSearch({
      filterGroups: [{ filters: [
        { propertyName: 'hs_pipeline', operator: 'EQ', value: '4483329' },
        { propertyName: 'onboarding_date', operator: 'GTE', value: tStartMs },
        { propertyName: 'onboarding_date', operator: 'LTE', value: tEndMs }
      ]}], properties: ['subject'], limit: 1
    });
    results.test_date_timestamp = { ok: true, total: d2.total };
  } catch (e) { results.test_date_timestamp = { ok: false, error: e.message.substring(0, 300) }; }
  // Test 4: Gong discovery calls for April
  try {
    var gongCount = await getDiscoveryCountForMonth('2026-04');
    results.gong_april = { ok: true, discoveryCalls: gongCount };
  } catch (e) { results.gong_april = { ok: false, error: e.message.substring(0, 300) }; }
  res.json(results);
});

// DEBUG: Test individual HubSpot queries for report generation
app.get('/api/debug/test-generate', async function(req, res) {
  try {
    var month = req.query.month || '2026-04';
    var parts = month.split('-');
    var yr = parseInt(parts[0]);
    var mo = parseInt(parts[1]);
    var monthStart = month + '-01';
    var lastDay = new Date(yr, mo, 0).getDate();
    var monthEnd = month + '-' + String(lastDay).padStart(2, '0');
    var sMs = String(new Date(monthStart + 'T00:00:00Z').getTime());
    var eMs = String(new Date(monthEnd + 'T23:59:59Z').getTime());
    var results = { month: month, monthStart: monthStart, monthEnd: monthEnd, startMs: sMs, endMs: eMs, steps: {} };

    // Step 1: Hires
    try {
      var hires = await fetchAllPagesWithRetry({
        filterGroups: [{ filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'onboarding_date', operator: 'GTE', value: sMs },
          { propertyName: 'onboarding_date', operator: 'LTE', value: eMs }
        ]}],
        properties: ['onboarding_date', 'assignment_type', 'type_of_recruitment', 'hs_pipeline']
      });
      results.steps.hires = { ok: true, count: hires.length };
    } catch (e) { results.steps.hires = { ok: false, error: e.message }; }

    // Step 2: Offboardings
    try {
      var offs = await fetchAllPagesWithRetry({
        filterGroups: [{ filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'offboarding_date', operator: 'GTE', value: sMs },
          { propertyName: 'offboarding_date', operator: 'LTE', value: eMs }
        ]}],
        properties: ['offboarding_date', 'assignment_type', 'onboarding_date', 'days_between_onboarding_offboarding']
      });
      results.steps.offboardings = { ok: true, count: offs.length };
    } catch (e) { results.steps.offboardings = { ok: false, error: e.message }; }

    // Step 3: Active FTE (two filter groups)
    try {
      var active = await fetchAllPagesWithRetry({
        filterGroups: [
          { filters: [
            { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
            { propertyName: 'onboarding_date', operator: 'LTE', value: eMs },
            { propertyName: 'offboarding_date', operator: 'NOT_HAS_PROPERTY' }
          ]},
          { filters: [
            { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
            { propertyName: 'onboarding_date', operator: 'LTE', value: eMs },
            { propertyName: 'offboarding_date', operator: 'GT', value: eMs }
          ]}
        ],
        properties: ['assignment_type', 'onboarding_date', 'offboarding_date']
      });
      results.steps.active_fte = { ok: true, count: active.length };
    } catch (e) { results.steps.active_fte = { ok: false, error: e.message }; }

    res.json(results);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Serve report pages
app.get('/reports', function(req, res) {
  res.sendFile(path.join(__dirname, 'reports.html'));
});
app.get('/report', function(req, res) {
  res.sendFile(path.join(__dirname, 'report.html'));
});

// ===== KPI History (reads from "Monthly KPI Tracker - Detailed" spreadsheet) =====
var KPI_SOURCE_SHEET_ID = '1u_v1atm_TtgrStgEZCaMBVMvaVV7nUiuNIwIz36SVp4';
var KPI_SOURCE_TAB = 'Monthly';
var kpiHistoryCache = { data: null, ts: 0 };
var KPI_HISTORY_CACHE_TTL = 10 * 60 * 1000; // 10 minutes

app.get('/api/kpi-history', async function(req, res) {
  try {
    if (!GOOGLE_SA_KEY) {
      return res.status(500).json({ error: 'Google service account key not configured' });
    }
    var now = Date.now();
    if (kpiHistoryCache.data && (now - kpiHistoryCache.ts) < KPI_HISTORY_CACHE_TTL) {
      return res.json(Object.assign({}, kpiHistoryCache.data, { cached: true }));
    }
    // Read all data from the Monthly tab
    var data = await sheetsGet(KPI_SOURCE_SHEET_ID, KPI_SOURCE_TAB + '!A1:AZ');
    var rows = data.values || [];
    if (rows.length < 4) {
      return res.json({ error: 'Not enough data in source sheet', headers: [], months: [] });
    }
    // Row 0 = team grouping, Row 1 = data location, Row 2 = column names, Row 3+ = data
    var teamRow = rows[0];
    var locationRow = rows[1];
    var headerRow = rows[2];
    var dataRows = rows.slice(3);

    // Build column metadata
    var columns = [];
    var currentTeam = '';
    for (var i = 0; i < headerRow.length; i++) {
      if (teamRow[i] && teamRow[i].trim()) currentTeam = teamRow[i].trim();
      columns.push({
        key: i,
        name: (headerRow[i] || '').trim(),
        team: currentTeam,
        source: (locationRow[i] || '').trim()
      });
    }

    // Filter to last 24 months of data (non-empty rows with a month value)
    var monthRows = [];
    for (var j = 0; j < dataRows.length; j++) {
      var row = dataRows[j];
      if (row && row[0] && row[0].trim()) {
        monthRows.push(row);
      }
    }
    // Take last 24
    var last24 = monthRows.slice(-24);

    // Build response
    var months = [];
    for (var k = 0; k < last24.length; k++) {
      var r = last24[k];
      var values = [];
      for (var m = 0; m < columns.length; m++) {
        values.push(r[m] !== undefined ? r[m] : '');
      }
      months.push(values);
    }

    var result = { columns: columns, months: months };
    kpiHistoryCache = { data: result, ts: now };
    res.json(Object.assign({}, result, { cached: false }));
  } catch (err) {
    console.error('KPI History API error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// FTE weighting by assignment type
function fteWeight(type) {
  if (type === 'Full-Time') return 1;
  if (type === 'Part-Time') return 0.5;
  return 0.25; // PT-Under-20, Project-Based, Output-Based
}

// Parse month labels like "March 2026" or "Mar-25" into { year, month, start, end }
var FULL_MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];
var SHORT_MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
function parseMonthLabel(label) {
  if (!label) return null;
  var trimmed = label.trim();
  var yr, mi;

  // Try "March 2026" format first
  var spaceIdx = trimmed.lastIndexOf(' ');
  if (spaceIdx > 0) {
    var monthPart = trimmed.substring(0, spaceIdx).trim();
    var yearPart = trimmed.substring(spaceIdx + 1).trim();
    mi = FULL_MONTHS.indexOf(monthPart);
    if (mi === -1) mi = SHORT_MONTHS.indexOf(monthPart);
    yr = parseInt(yearPart);
    if (mi >= 0 && !isNaN(yr)) {
      var mo = mi + 1;
      var lastDay = new Date(yr, mo, 0).getDate();
      var moStr = String(mo).padStart(2, '0');
      return { year: yr, month: mo, start: yr + '-' + moStr + '-01', end: yr + '-' + moStr + '-' + String(lastDay).padStart(2, '0') };
    }
  }

  // Try "Mar-25" format
  var parts = trimmed.split('-');
  if (parts.length === 2) {
    mi = SHORT_MONTHS.indexOf(parts[0]);
    if (mi === -1) mi = FULL_MONTHS.indexOf(parts[0]);
    yr = parseInt(parts[1]);
    if (mi >= 0 && !isNaN(yr)) {
      if (yr < 100) yr += yr < 50 ? 2000 : 1900;
      var mo2 = mi + 1;
      var lastDay2 = new Date(yr, mo2, 0).getDate();
      var moStr2 = String(mo2).padStart(2, '0');
      return { year: yr, month: mo2, start: yr + '-' + moStr2 + '-01', end: yr + '-' + moStr2 + '-' + String(lastDay2).padStart(2, '0') };
    }
  }

  return null;
}

// Find column index by header name (case-insensitive partial match)
function findCol(headerRow, name) {
  var lower = name.toLowerCase();
  for (var i = 0; i < headerRow.length; i++) {
    if ((headerRow[i] || '').toLowerCase().trim() === lower) return i;
  }
  return -1;
}

// Convert column index to A1 letter (0=A, 1=B, ... 25=Z, 26=AA, etc.)
function colLetter(idx) {
  var s = '';
  idx++;
  while (idx > 0) {
    idx--;
    s = String.fromCharCode(65 + (idx % 26)) + s;
    idx = Math.floor(idx / 26);
  }
  return s;
}

// ===== KPI Cell Edit (writes directly to Monthly sheet) =====
app.post('/api/kpi-history/edit-cell', async function(req, res) {
  try {
    var col = req.body.col;
    var month = req.body.month;
    var value = req.body.value;
    if (col === undefined || !month || value === undefined) {
      return res.status(400).json({ error: 'col, month, value required' });
    }
    var data = await sheetsGet(KPI_SOURCE_SHEET_ID, KPI_SOURCE_TAB + '!A1:AZ');
    var rows = data.values || [];
    var dataRows = rows.slice(3);
    var targetRowIdx = -1;
    for (var i = 0; i < dataRows.length; i++) {
      if (dataRows[i][0] && dataRows[i][0].trim() === month.trim()) { targetRowIdx = i; break; }
    }
    if (targetRowIdx === -1) {
      return res.status(404).json({ error: 'Month not found: ' + month });
    }
    var sheetRow = targetRowIdx + 4;
    var cell = KPI_SOURCE_TAB + '!' + colLetter(parseInt(col)) + sheetRow;
    var numVal = parseFloat(value);
    await sheetsUpdate(KPI_SOURCE_SHEET_ID, cell, [[isNaN(numVal) ? value : numVal]]);
    kpiHistoryCache = { data: null, ts: 0 };
    res.json({ success: true, cell: cell });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Calculate ratio metrics only (reads existing sheet values, computes % rates)
app.post('/api/kpi-history/calc-ratios', async function(req, res) {
  try {
    if (!GOOGLE_SA_KEY) return res.status(500).json({ error: 'Google service account key not configured' });
    var month = req.body.month;
    if (!month) return res.status(400).json({ error: 'month is required' });

    var data = await sheetsGet(KPI_SOURCE_SHEET_ID, KPI_SOURCE_TAB + '!A1:AZ');
    var rows = data.values || [];
    if (rows.length < 4) return res.status(500).json({ error: 'Source sheet has no data' });
    var dataRows = rows.slice(3);

    var targetRowIdx = -1;
    for (var i = 0; i < dataRows.length; i++) {
      if (dataRows[i][0] && dataRows[i][0].trim() === month.trim()) { targetRowIdx = i; break; }
    }
    if (targetRowIdx === -1) return res.status(404).json({ error: 'Month "' + month + '" not found in sheet' });

    var sheetRow = targetRowIdx + 4;
    var cur = dataRows[targetRowIdx];
    var prevRow = targetRowIdx > 0 ? dataRows[targetRowIdx - 1] : [];
    var updates = {};

    // Read current month values from sheet
    var _adsSpend = parseSheetNum(cur[15]) || 0;
    var _mqlCount = parseSheetNum(cur[6]) || 0;
    var _totalFTEHires = parseSheetNum(cur[12]) || 0;
    var _lostFTEs = parseSheetNum(cur[34]) || 0;
    var _backfillFTE = parseSheetNum(cur[38]) || 0;
    var _existingClientFTE = parseSheetNum(cur[43]) || 0;
    var _newClientJobs = parseSheetNum(cur[9]) || 0;
    var fabiusCalls = parseSheetNum(cur[7]) || 0;
    var adminStaff = parseSheetNum(cur[5]) || 0;
    var startOfPeriodFTE = parseSheetNum(cur[4]) || 0;
    var endorsements = parseSheetNum(cur[24]) || 0;
    var recruitmentHC = parseSheetNum(cur[23]) || 0;
    var salesHC = parseSheetNum(cur[22]) || 0;
    var rolesToBackfill = parseSheetNum(cur[36]) || 0;
    var existingClients = parseSheetNum(cur[40]) || 0;
    var existingClientJobs = parseSheetNum(cur[41]) || 0;
    var newClientHires = parseSheetNum(cur[48]) || 0;
    var under30FTE = parseSheetNum(cur[26]) || 0;

    // Previous month values
    var prevMQLs = parseSheetNum(prevRow[6]) || 0;
    var prevTotalHires = parseSheetNum(prevRow[12]) || 0;
    var prevNewClientJobs = parseSheetNum(prevRow[9]) || 0;
    var prevExistingClients = parseSheetNum(prevRow[40]) || 0;
    var prevChurned = parseSheetNum(prevRow[34]) || 0;
    var prevExistingClientJobs = parseSheetNum(prevRow[41]) || 0;

    // Col 2: Active Staff Per Admin
    if (adminStaff > 0 && startOfPeriodFTE > 0)
      updates['Active Staff Per Admin'] = { col: 2, value: Math.round((startOfPeriodFTE / adminStaff) * 100) / 100 };

    // Col 8: Show Rate = Fabius Calls / MQLs
    if (_mqlCount > 0)
      updates['Show Rate'] = { col: 8, value: Math.round((fabiusCalls / _mqlCount) * 10000) / 10000 };

    // Col 10: New Client Jobs vs Discovery Calls
    if (fabiusCalls > 0)
      updates['New Client Jobs vs Discovery Calls'] = { col: 10, value: Math.round((_newClientJobs / fabiusCalls) * 10000) / 10000 };

    // Col 13: FTE Close Rate = Total FTE Hires / prev month MQLs
    if (prevMQLs > 0)
      updates['FTE Close Rate'] = { col: 13, value: Math.round((_totalFTEHires / prevMQLs) * 10000) / 10000 };

    // Col 14: FTE Conv from Job = Total FTE Hires / prev month Jobs
    if (prevNewClientJobs > 0)
      updates['FTE Conv Close Rate from Job'] = { col: 14, value: Math.round((_totalFTEHires / prevNewClientJobs) * 10000) / 10000 };

    // Col 16: Cost Per Discovery Call = Ads Spend / MQLs
    if (_mqlCount > 0)
      updates['Cost Per Discovery Call'] = { col: 16, value: Math.round((_adsSpend / _mqlCount) * 100) / 100 };

    // Col 17: Sales Conversion Rate = New Client Jobs / MQLs
    if (_mqlCount > 0)
      updates['Sales Conversion Rate'] = { col: 17, value: Math.round((_newClientJobs / _mqlCount) * 10000) / 10000 };

    // Col 25: Endorsements Per Recruitment HC
    if (recruitmentHC > 0)
      updates['Endorsements Per Recruitment HC'] = { col: 25, value: Math.round((endorsements / recruitmentHC) * 100) / 100 };

    // Col 27: Staff churned <1m as % of hires
    if (prevTotalHires > 0)
      updates['Staff churned <1m as % of hires'] = { col: 27, value: Math.round((under30FTE / prevTotalHires) * 10000) / 10000 };

    // Col 33: Hires Per Sales/Recruitment HC
    if ((recruitmentHC + salesHC) > 0)
      updates['Hires Per Sales/Recruitment HC'] = { col: 33, value: Math.round((_totalFTEHires / (recruitmentHC + salesHC)) * 100) / 100 };

    // Col 35: Role Churn Rate
    if (startOfPeriodFTE > 0)
      updates['Role Churn Rate'] = { col: 35, value: Math.round((_lostFTEs / startOfPeriodFTE) * 10000) / 10000 };

    // Col 37: Backfill rate
    if (_lostFTEs > 0)
      updates['Backfill rate'] = { col: 37, value: Math.round((rolesToBackfill / _lostFTEs) * 10000) / 10000 };

    // Col 39: Backfill Close Rate
    if (prevChurned > 0)
      updates['Backfill Close Rate'] = { col: 39, value: Math.round((_backfillFTE / prevChurned) * 10000) / 10000 };

    // Col 44: FTE/Client Expansion Rate
    if (existingClients > 0)
      updates['FTE/Client Expansion Rate'] = { col: 44, value: Math.round((_existingClientFTE / existingClients) * 10000) / 10000 };

    // Col 45: Jobs Expansion Rate
    if (prevExistingClients > 0)
      updates['Jobs Expansion Rate'] = { col: 45, value: Math.round((existingClientJobs / prevExistingClients) * 10000) / 10000 };

    // Col 46: FTE Close Rate Existing Client
    if (prevExistingClientJobs > 0)
      updates['FTE Close Rate Existing Client'] = { col: 46, value: Math.round((_existingClientFTE / prevExistingClientJobs) * 10000) / 10000 };

    // Col 49: New Client FTE Conv Rate
    if (prevMQLs > 0)
      updates['New Client FTE Conv Rate'] = { col: 49, value: Math.round((newClientHires / prevMQLs) * 10000) / 10000 };

    // Write updates
    var written = [];
    for (var key in updates) {
      var u = updates[key];
      var cell = KPI_SOURCE_TAB + '!' + colLetter(u.col) + sheetRow;
      await sheetsUpdate(KPI_SOURCE_SHEET_ID, cell, [[u.value]]);
      written.push(key + ' (col ' + u.col + ') = ' + u.value);
    }

    console.log('[KPI Calc Ratios] ' + month + ': wrote ' + written.length + ' ratios');
    kpiCache = null;
    res.json({ success: true, updated: written });
  } catch (e) {
    console.error('[KPI Calc Ratios] Error:', e);
    res.status(500).json({ error: e.message });
  }
});

// Generate KPI data for a specific month from HubSpot + Google Ads
app.post('/api/kpi-history/generate', async function(req, res) {
  try {
    if (!GOOGLE_SA_KEY) {
      return res.status(500).json({ error: 'Google service account key not configured' });
    }
    var month = req.body.month; // e.g. "Mar-25"
    if (!month) {
      return res.status(400).json({ error: 'month is required (e.g. "Mar-25")' });
    }

    var parsed = parseMonthLabel(month);
    if (!parsed) {
      return res.status(400).json({ error: 'Cannot parse month "' + month + '". Expected format: "Mar-25"' });
    }

    // Find the row in the source sheet that matches this month
    var data = await sheetsGet(KPI_SOURCE_SHEET_ID, KPI_SOURCE_TAB + '!A1:AZ');
    var rows = data.values || [];
    if (rows.length < 4) {
      return res.status(500).json({ error: 'Source sheet has no data' });
    }

    var headerRow = rows[2];
    var dataRows = rows.slice(3);

    // Find the row index (in the sheet, 1-indexed; row 0 in dataRows = sheet row 4)
    var targetRowIdx = -1;
    for (var i = 0; i < dataRows.length; i++) {
      if (dataRows[i][0] && dataRows[i][0].trim() === month.trim()) {
        targetRowIdx = i;
        break;
      }
    }

    if (targetRowIdx === -1) {
      return res.status(404).json({ error: 'Month "' + month + '" not found in sheet' });
    }

    var sheetRow = targetRowIdx + 4; // 1-indexed, accounting for 3 header rows
    var updates = {}; // colName -> { col, value }
    var errors = []; // collect non-fatal errors for response

    // ===== Google Ads: Monthly spend + conversions =====
    console.log('[KPI Generate] Fetching Google Ads data for ' + month + ' (' + parsed.start + ' to ' + parsed.end + ')');
    try {
      var adsCsv = await fetchAdsCsv(ADS_CSV_URL);
      var adsRows = parseAdsCsv(adsCsv);
      var adsResult = processAdsData(adsRows);
      // Filter timeseries to target month
      var monthAds = adsResult.timeseries.filter(function(d) {
        return d.day >= parsed.start && d.day <= parsed.end;
      });
      var adsSpend = 0, adsConversions = 0;
      for (var ai = 0; ai < monthAds.length; ai++) {
        adsSpend += monthAds[ai].cost;
        adsConversions += monthAds[ai].conversions;
      }
      adsSpend = Math.round(adsSpend * 100) / 100;
      adsConversions = Math.round(adsConversions * 100) / 100;
      var costPerLead = adsConversions > 0 ? Math.round((adsSpend / adsConversions) * 100) / 100 : 0;

      console.log('[KPI Generate] Ads: $' + adsSpend + ' spend, ' + adsConversions + ' conversions, $' + costPerLead + ' CPL');

      // Map to sheet columns — try common header names
      var adsSpendCol = findCol(headerRow, 'Google Ads Spend');
      if (adsSpendCol === -1) adsSpendCol = findCol(headerRow, 'Ads Spend');
      if (adsSpendCol === -1) adsSpendCol = findCol(headerRow, 'Ad Spend');
      if (adsSpendCol >= 0) updates['Google Ads Spend'] = { col: adsSpendCol, value: adsSpend };

      var adsConvCol = findCol(headerRow, 'Google Ads Conversions');
      if (adsConvCol === -1) adsConvCol = findCol(headerRow, 'Conversions');
      if (adsConvCol >= 0) updates['Google Ads Conversions'] = { col: adsConvCol, value: adsConversions };

      var cplCol = findCol(headerRow, 'Cost Per Lead');
      if (cplCol === -1) cplCol = findCol(headerRow, 'CPL');
      if (cplCol >= 0) updates['Cost Per Lead'] = { col: cplCol, value: costPerLead };
    } catch (adsErr) {
      console.error('[KPI Generate] Ads data fetch failed:', adsErr.message);
    }

    // ===== HubSpot: Hires (onboarding_date in month) =====
    console.log('[KPI Generate] Fetching HubSpot hires for ' + month + '...');
    try {
      var hireStartMs = String(new Date(parsed.start + 'T00:00:00Z').getTime());
      var hireEndMs = String(new Date(parsed.end + 'T23:59:59Z').getTime());
      var hireResults = await fetchAllPagesWithRetry({
        filterGroups: [{
          filters: [
            { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
            { propertyName: 'createdate', operator: 'GTE', value: hireStartMs },
            { propertyName: 'createdate', operator: 'LTE', value: hireEndMs }
          ]
        }],
        properties: ['createdate', 'assignment_type', 'type_of_recruitment', 'job_source', 'hs_pipeline', 'subject'],
        sorts: [{ propertyName: 'createdate', direction: 'ASCENDING' }]
      });
      console.log('[KPI Generate] Hires found (raw): ' + hireResults.length);

      // Exclude tickets where subject contains "BruntWork"
      hireResults = hireResults.filter(function(t) {
        var subj = (t.properties.subject || '').toLowerCase();
        return subj.indexOf('bruntwork') === -1;
      });
      console.log('[KPI Generate] Hires after excluding BruntWork: ' + hireResults.length);

      // Log job_source distribution for debugging
      var jobSrcDist = {};
      for (var hd = 0; hd < hireResults.length; hd++) {
        var src = hireResults[hd].properties.job_source || '(empty)';
        jobSrcDist[src] = (jobSrcDist[src] || 0) + 1;
      }
      console.log('[KPI Generate] Hires job_source distribution: ' + JSON.stringify(jobSrcDist));

      var totalFTEHires = 0;
      var backfillFTEHires = 0, newClientFTEHires = 0, existingClientFTEHires = 0;
      var existingClientHC = 0;
      for (var hi = 0; hi < hireResults.length; hi++) {
        var hp = hireResults[hi].properties;
        var w = fteWeight(hp.assignment_type || 'Unknown');
        totalFTEHires += w;
        var jobSrcLower = (hp.job_source || '').toLowerCase().trim();
        if (jobSrcLower === 'backfill' || jobSrcLower === 'back up') {
          backfillFTEHires += w;
        } else if (jobSrcLower === 'new') {
          newClientFTEHires += w;
        }
        if (jobSrcLower === 'existing') {
          existingClientFTEHires += w;
          existingClientHC++;
        }
      }
      totalFTEHires = Math.round(totalFTEHires * 100) / 100;

      // Col 12: Total FTE Hires (Sales)
      updates['Total FTE Hires (Col 12)'] = { col: 12, value: totalFTEHires };
      // Col 32: Total Hires FTE (Recruitment) — same value
      updates['Total Hires FTE (Col 32)'] = { col: 32, value: totalFTEHires };
      // Col 47: Total FTE Hires (Client Services) — same value
      updates['Total FTE Hires (Col 47)'] = { col: 47, value: totalFTEHires };
      // Col 38: Backfill FTE hires
      updates['Backfill FTE hires'] = { col: 38, value: Math.round(backfillFTEHires * 100) / 100 };
      // Col 43: FTE Hires (Existing Client)
      updates['FTE Hires (Existing Client)'] = { col: 43, value: Math.round(existingClientFTEHires * 100) / 100 };
      // Col 42: Headcount Hires (Existing Client)
      updates['Headcount Hires (Existing Client)'] = { col: 42, value: existingClientHC };
      // Col 48: New Client Hires (from job_source = "New")
      updates['New Client Hires'] = { col: 48, value: Math.round(newClientFTEHires * 100) / 100 };

      console.log('[KPI Generate] FTE Hires: ' + totalFTEHires + ' (backfill=' + backfillFTEHires + ', newClient=' + newClientFTEHires + ', existing=' + existingClientFTEHires + ')');
    } catch (hireErr) {
      console.error('[KPI Generate] Hires fetch failed:', hireErr.message);
    }

    // Delay to avoid HubSpot rate limits between major API calls
    await sleep(1000);

    // ===== HubSpot Companies: Number of Existing Clients (Col 40) =====
    // Count companies where total_active_staff_count > 0
    console.log('[KPI Generate] Counting existing clients (companies with total_active_staff_count > 0)...');
    try {
      var existingClientCount = 0;
      var ecAfter = undefined;
      var ecHasMore = true;
      while (ecHasMore) {
        var ecBody = {
          filterGroups: [{
            filters: [
              { propertyName: 'total_active_staff_count', operator: 'GT', value: '0' }
            ]
          }],
          properties: ['name', 'total_active_staff_count'],
          limit: 200
        };
        if (ecAfter) ecBody.after = ecAfter;
        if (existingClientCount > 0) await sleep(300);
        var ecData;
        try {
          ecData = await hubspotSearchObject('companies', ecBody);
        } catch (ecRateErr) {
          if (ecRateErr.message && ecRateErr.message.indexOf('429') !== -1) {
            console.log('[KPI Generate] Rate limited on companies, waiting 3s...');
            await sleep(3000);
            ecData = await hubspotSearchObject('companies', ecBody);
          } else {
            throw ecRateErr;
          }
        }
        existingClientCount += (ecData.results || []).length;
        if (ecData.paging && ecData.paging.next && ecData.paging.next.after) {
          ecAfter = ecData.paging.next.after;
        } else {
          ecHasMore = false;
        }
      }
      console.log('[KPI Generate] Existing clients (total_active_staff_count > 0): ' + existingClientCount);
      updates['Number of Existing Clients'] = { col: 40, value: existingClientCount };
    } catch (ecErr) {
      console.error('[KPI Generate] Existing clients count failed:', ecErr.message);
      errors.push('Existing clients count failed: ' + ecErr.message);
      // Fallback: preserve existing value from sheet so downstream calcs don't break
      var existingVal = parseSheetNum(dataRows[targetRowIdx][40]);
      if (existingVal > 0) {
        updates['Number of Existing Clients'] = { col: 40, value: existingVal };
        console.log('[KPI Generate] Using existing sheet value for Existing Clients: ' + existingVal);
      }
    }

    await sleep(1000);

    // ===== HubSpot Tickets: Sales Agent Hired FTE =====
    console.log('[KPI Generate] Fetching Sales Agent Hired FTE for ' + month + '...');
    try {
      var saCol = findCol(headerRow, 'Sales Agent Hired FTE');
      if (saCol === -1) saCol = findCol(headerRow, 'Sales Agent Hired');
      if (saCol === -1) saCol = 11; // Column L
      if (saCol >= 0) {
        var saStartMs = String(new Date(parsed.start + 'T00:00:00Z').getTime());
        var saEndMs = String(new Date(parsed.end + 'T23:59:59Z').getTime());
        var saResults = await fetchAllPagesWithRetry({
          filterGroups: [{
            filters: [
              { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
              { propertyName: 'createdate', operator: 'GTE', value: saStartMs },
              { propertyName: 'createdate', operator: 'LTE', value: saEndMs },
              { propertyName: 'assignment_group', operator: 'EQ', value: 'Outsource' },
              { propertyName: 'sales_agent_associated', operator: 'HAS_PROPERTY' }
            ]
          }],
          properties: ['createdate', 'assignment_type', 'assignment_group', 'sales_agent_associated']
        });
        // Filter to known contract types and FTE-weight
        var validTypes = ['output-based', 'full-time', 'part-time-under-20-hours', 'part-time', 'project-based'];
        var saFTE = 0;
        for (var si = 0; si < saResults.length; si++) {
          var saType = (saResults[si].properties.assignment_type || '').toLowerCase();
          if (validTypes.indexOf(saType) !== -1) {
            saFTE += fteWeight(saResults[si].properties.assignment_type || 'Unknown');
          }
        }
        saFTE = Math.round(saFTE * 100) / 100;
        console.log('[KPI Generate] Sales Agent Hired FTE: ' + saFTE + ' (from ' + saResults.length + ' tickets)');
        updates['Sales Agent Hired FTE'] = { col: saCol, value: saFTE };
      } else {
        console.log('[KPI Generate] "Sales Agent Hired FTE" column not found in headers, skipping');
      }
    } catch (saErr) {
      console.error('[KPI Generate] Sales Agent Hired FTE fetch failed:', saErr.message);
    }

    // Delay to avoid HubSpot rate limits between major API calls
    await sleep(1000);

    // ===== HubSpot: Offboardings (offboarding_date in month) =====
    console.log('[KPI Generate] Fetching HubSpot offboardings for ' + month + '...');
    var lostFTEs = 0;
    var under30FTE = 0;
    try {
      var offStartMs = String(new Date(parsed.start + 'T00:00:00Z').getTime());
      var offEndMs = String(new Date(parsed.end + 'T23:59:59Z').getTime());
      var offResults = await fetchAllPagesWithRetry({
        filterGroups: [{
          filters: [
            { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
            { propertyName: 'offboarding_date', operator: 'GTE', value: offStartMs },
            { propertyName: 'offboarding_date', operator: 'LTE', value: offEndMs }
          ]
        }],
        properties: ['offboarding_date', 'assignment_type', 'onboarding_date', 'days_between_onboarding_offboarding', 'type_of_recruitment'],
        sorts: [{ propertyName: 'offboarding_date', direction: 'ASCENDING' }]
      });
      console.log('[KPI Generate] Offboardings found: ' + offResults.length);
      var backfillRequested = 0;
      for (var oi = 0; oi < offResults.length; oi++) {
        var op = offResults[oi].properties;
        var ow = fteWeight(op.assignment_type || 'Unknown');
        lostFTEs += ow;
        // Check <30 day tenure
        var daysBetween = parseFloat(op.days_between_onboarding_offboarding);
        if (isNaN(daysBetween) && op.onboarding_date && op.offboarding_date) {
          daysBetween = (new Date(op.offboarding_date) - new Date(op.onboarding_date)) / (1000*60*60*24);
        }
        if (!isNaN(daysBetween) && daysBetween < 30) under30FTE += ow;
      }
      lostFTEs = Math.round(lostFTEs * 100) / 100;

      // Col 34: Churned Staff (FTE)
      updates['Churned Staff (FTE)'] = { col: 34, value: lostFTEs };

      console.log('[KPI Generate] Lost FTEs: ' + lostFTEs + ', <30 day FTE: ' + under30FTE);
    } catch (offErr) {
      console.error('[KPI Generate] Offboardings fetch failed:', offErr.message);
    }

    // Delay to avoid HubSpot rate limits between major API calls
    await sleep(1000);

    // ===== HubSpot Tickets: Roles to be Backfilled (Col 36) =====
    console.log('[KPI Generate] Fetching roles to be backfilled for ' + month + '...');
    try {
      var bfStartMs = String(new Date(parsed.start + 'T00:00:00Z').getTime());
      var bfEndMs = String(new Date(parsed.end + 'T23:59:59Z').getTime());
      var backfillResults = await fetchAllPagesWithRetry({
        filterGroups: [{
          filters: [
            { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
            { propertyName: 'assignment_group', operator: 'EQ', value: 'Outsource' },
            { propertyName: 'client_role_backfill', operator: 'EQ', value: 'Yes' },
            { propertyName: 'offboarding_date', operator: 'GTE', value: bfStartMs },
            { propertyName: 'offboarding_date', operator: 'LTE', value: bfEndMs }
          ]
        }],
        properties: ['offboarding_date', 'assignment_group', 'client_role_backfill'],
        sorts: [{ propertyName: 'offboarding_date', direction: 'ASCENDING' }]
      });
      console.log('[KPI Generate] Roles to be backfilled: ' + backfillResults.length);
      updates['Roles to be backfilled'] = { col: 36, value: backfillResults.length };
    } catch (bfErr) {
      console.error('[KPI Generate] Backfill roles fetch failed:', bfErr.message);
    }

    // Delay to avoid HubSpot rate limits between major API calls
    await sleep(1000);

    // ===== HubSpot Contacts: MQLs (became MQL in month) — Col 6 =====
    console.log('[KPI Generate] Fetching MQL contacts for ' + month + '...');
    try {
      var mqlStartMs = String(new Date(parsed.start + 'T00:00:00Z').getTime());
      var mqlEndMs = String(new Date(parsed.end + 'T23:59:59Z').getTime());
      var mqlResults = await fetchAllPagesObject('contacts', {
        filterGroups: [{
          filters: [
            { propertyName: 'hs_v2_date_entered_marketingqualifiedlead', operator: 'GTE', value: mqlStartMs },
            { propertyName: 'hs_v2_date_entered_marketingqualifiedlead', operator: 'LTE', value: mqlEndMs }
          ]
        }],
        properties: ['hs_v2_date_entered_marketingqualifiedlead']
      });
      var mqlCount = mqlResults.length;
      console.log('[KPI Generate] MQLs found: ' + mqlCount);
      updates['Discovery Calls / MQLs'] = { col: 6, value: mqlCount };
    } catch (mqlErr) {
      console.error('[KPI Generate] MQL fetch failed:', mqlErr.message);
    }

    // Delay to avoid HubSpot rate limits between major API calls
    await sleep(1000);

    // ===== HubSpot Tickets: BW Admin Staff — currently Active status, excl. Digital Team (Col 5) =====
    console.log('[KPI Generate] Fetching currently active BW Admin Staff...');
    try {
      var bwResults = await fetchAllPagesWithRetry({
        filterGroups: [{
          filters: [
            { propertyName: 'hs_pipeline', operator: 'EQ', value: '16984077' },
            { propertyName: 'staff_status', operator: 'EQ', value: 'Active' },
            { propertyName: 'bw_internal_secondary_team', operator: 'NEQ', value: 'Digital' }
          ]
        }],
        properties: ['staff_status', 'bw_internal_secondary_team']
      });
      console.log('[KPI Generate] BW Admin Staff (Active, excl Digital): ' + bwResults.length);
      updates['BW Admin Staff'] = { col: 5, value: bwResults.length };
    } catch (bwErr) {
      console.error('[KPI Generate] BW Admin Staff fetch failed:', bwErr.message);
    }

    // Delay to avoid HubSpot rate limits between major API calls
    await sleep(1000);

    // ===== HubSpot Tickets: Recruitment Team HC — Active in BW Internal, secondary team = Recruiting OR Sourcing (Col 23) =====
    console.log('[KPI Generate] Fetching Recruitment Team HC...');
    try {
      var recruitResults = await fetchAllPagesWithRetry({
        filterGroups: [
          {
            filters: [
              { propertyName: 'hs_pipeline', operator: 'EQ', value: '16984077' },
              { propertyName: 'staff_status', operator: 'EQ', value: 'Active' },
              { propertyName: 'bw_internal_secondary_team', operator: 'EQ', value: 'Recruitment' }
            ]
          },
          {
            filters: [
              { propertyName: 'hs_pipeline', operator: 'EQ', value: '16984077' },
              { propertyName: 'staff_status', operator: 'EQ', value: 'Active' },
              { propertyName: 'bw_internal_secondary_team', operator: 'EQ', value: 'Sourcing' }
            ]
          }
        ],
        properties: ['staff_status', 'bw_internal_secondary_team']
      });
      console.log('[KPI Generate] Recruitment Team HC: ' + recruitResults.length);
      updates['Recruitment Team HC'] = { col: 23, value: recruitResults.length };
    } catch (recruitErr) {
      console.error('[KPI Generate] Recruitment Team HC fetch failed:', recruitErr.message);
    }

    // Delay to avoid HubSpot rate limits between major API calls
    await sleep(1000);

    // ===== HubSpot Custom Object: Jobs — New Client Jobs Opened (Col 9) =====
    console.log('[KPI Generate] Fetching new client jobs opened for ' + month + '...');
    try {
      var jobsObjectType = await getJobsObjectTypeId();
      // Log properties on first run to discover correct field names
      if (!getJobsObjectTypeId._propsDiscovered) {
        var jobsProps = await hubspotGet('https://api.hubapi.com/crm/v3/properties/' + jobsObjectType);
        var propNames = (jobsProps.results || []).map(function(p) { return p.name + ' (' + p.label + ')'; });
        console.log('[KPI Generate] Jobs object properties: ' + propNames.join(', '));
        getJobsObjectTypeId._propsDiscovered = true;
      }
      var jobStartMs = String(new Date(parsed.start + 'T00:00:00Z').getTime());
      var jobEndMs = String(new Date(parsed.end + 'T23:59:59Z').getTime());
      // Fetch all jobs created in month (no job_source filter) so we can log actual values
      var allJobResults = await fetchAllPagesObject(jobsObjectType, {
        filterGroups: [{
          filters: [
            { propertyName: 'createdate', operator: 'GTE', value: jobStartMs },
            { propertyName: 'createdate', operator: 'LTE', value: jobEndMs }
          ]
        }],
        properties: ['createdate', 'job_source', 'client_billing_name']
      });
      // Log unique job_source values to discover correct filter value
      var jobSourceValues = {};
      for (var ji = 0; ji < allJobResults.length; ji++) {
        var src = allJobResults[ji].properties.job_source || '(empty)';
        jobSourceValues[src] = (jobSourceValues[src] || 0) + 1;
      }
      console.log('[KPI Generate] Jobs created in month: ' + allJobResults.length + ', job_source values: ' + JSON.stringify(jobSourceValues));
      // Filter: job_source = "New Client" (case-insensitive), exclude BruntWork billing
      var newClientJobs = allJobResults.filter(function(j) {
        var src = (j.properties.job_source || '').toLowerCase();
        var billing = (j.properties.client_billing_name || '').toLowerCase();
        return (src === 'new client' || src === 'new_client' || src === 'new') && billing.indexOf('bruntwork') === -1;
      });
      console.log('[KPI Generate] New Client Jobs: ' + newClientJobs.length + ' (excluded BruntWork: ' + (allJobResults.length - newClientJobs.length) + ')');
      updates['New Client Jobs Opened'] = { col: 9, value: newClientJobs.length };

      // Filter: job_source != "New Client" and not empty, exclude BruntWork billing (i.e. existing client jobs)
      var existingClientJobs = allJobResults.filter(function(j) {
        var src = (j.properties.job_source || '').toLowerCase().trim();
        var billing = (j.properties.client_billing_name || '').toLowerCase();
        // Anything that's not "new client" and not empty = existing client
        var isNew = (src === 'new client' || src === 'new_client' || src === 'new');
        return src !== '' && !isNew && billing.indexOf('bruntwork') === -1;
      });
      console.log('[KPI Generate] Existing Client Jobs: ' + existingClientJobs.length + ' (non-new, non-empty job_source)');
      // Log sample of existing client job_source values for debugging
      var existingSources = {};
      for (var ej = 0; ej < existingClientJobs.length; ej++) {
        var esrc = existingClientJobs[ej].properties.job_source || '(empty)';
        existingSources[esrc] = (existingSources[esrc] || 0) + 1;
      }
      if (Object.keys(existingSources).length > 0) console.log('[KPI Generate] Existing client job_source values: ' + JSON.stringify(existingSources));
      updates['Jobs Opened (Existing Clients)'] = { col: 41, value: existingClientJobs.length };
    } catch (jobsErr) {
      console.error('[KPI Generate] Jobs fetch failed:', jobsErr.message);
      errors.push('Jobs: ' + jobsErr.message);
    }

    // Delay to avoid HubSpot rate limits
    await sleep(1000);

    // ===== Col 24: Endorsements (Active Applications count) =====
    try {
      // Use IN with included stages + NOT_CONTAINS_TOKEN to exclude bruntwork
      // Only need the total count, so fetch a single page with limit=1
      var includedStages = [
        '978051969',   // Endorsed to Job Owner
        '977990485',   // Client Interview Scheduled
        '977990486',   // Candidate Rejected by Client
        '977990487',   // Candidate Withdrew
        '1075896997',  // To Be Offered
        '977990488',   // BruntWork Offer Sent
        '977990489',   // Hired
        '1015966551'   // Hired - OEF Created
      ];
      // Two filter groups: one for records with client (exclude bruntwork), one for records without client
      var endData1 = await hubspotSearchObject('2-38227027', {
        filterGroups: [{
          filters: [
            { propertyName: 'hs_pipeline', operator: 'EQ', value: '666493306' },
            { propertyName: 'hs_pipeline_stage', operator: 'IN', values: includedStages },
            { propertyName: 'client__cloned_', operator: 'NOT_CONTAINS_TOKEN', value: 'bruntwork' }
          ]
        },
        {
          filters: [
            { propertyName: 'hs_pipeline', operator: 'EQ', value: '666493306' },
            { propertyName: 'hs_pipeline_stage', operator: 'IN', values: includedStages },
            { propertyName: 'client__cloned_', operator: 'NOT_HAS_PROPERTY' }
          ]
        }],
        properties: ['hs_pipeline_stage'],
        limit: 1
      });
      var endorsementCount = endData1.total || 0;
      console.log('[KPI Generate] Endorsements: ' + endorsementCount);
      updates['Endorsements'] = { col: 24, value: endorsementCount };
    } catch (endErr) {
      console.error('[KPI Generate] Endorsements fetch failed:', endErr.message);
      errors.push('Endorsements: ' + endErr.message);
    }

    // Delay to avoid HubSpot rate limits
    await sleep(1000);

    // ===== Computed metrics (derived from fetched data) =====
    // Gather values for calculations (use updates map or defaults)
    var _adsSpend = updates['Google Ads Spend'] ? updates['Google Ads Spend'].value : 0;
    var _mqlCount = updates['Discovery Calls / MQLs'] ? updates['Discovery Calls / MQLs'].value : 0;
    var _totalFTEHires = updates['Total FTE Hires (Col 12)'] ? updates['Total FTE Hires (Col 12)'].value : 0;
    var _lostFTEs = updates['Churned Staff (FTE)'] ? updates['Churned Staff (FTE)'].value : 0;
    var _backfillFTE = updates['Backfill FTE hires'] ? updates['Backfill FTE hires'].value : 0;
    var _existingClientFTE = updates['FTE Hires (Existing Client)'] ? updates['FTE Hires (Existing Client)'].value : 0;
    var _existingClientHC = updates['Headcount Hires (Existing Client)'] ? updates['Headcount Hires (Existing Client)'].value : 0;

    // Previous month values (for rates that reference prior month)
    var prevRow = targetRowIdx > 0 ? dataRows[targetRowIdx - 1] : [];
    var prevActiveFTE = parseSheetNum(prevRow[4]) || 0;
    var prevMQLs = parseSheetNum(prevRow[6]) || 0;
    var prevTotalHires = parseSheetNum(prevRow[12]) || 0;
    var prevNewClientJobs = parseSheetNum(prevRow[9]) || 0;
    var prevExistingClients = parseSheetNum(prevRow[40]) || 0;
    var prevExistingJobs = parseSheetNum(prevRow[41]) || 0;
    var prevChurned = parseSheetNum(prevRow[34]) || 0;

    // Col 16: Cost Per Discovery Call = Google Ads Spend / MQLs
    if (_mqlCount > 0) {
      updates['Cost Per Discovery Call'] = { col: 16, value: Math.round((_adsSpend / _mqlCount) * 100) / 100 };
    }

    // Col 35: Role Churn Rate = Churned Staff (FTE) / Active Staff Assignments - start of period (FTE) for same month
    var startOfPeriodFTE = parseSheetNum(dataRows[targetRowIdx][4]) || 0;
    if (startOfPeriodFTE > 0) {
      updates['Role Churn Rate'] = { col: 35, value: Math.round((_lostFTEs / startOfPeriodFTE) * 10000) / 10000 };
    }

    // Col 2: Active Staff Headcount Per Admin Staff = Active FTE / BW Admin Staff
    var adminStaff = updates['BW Admin Staff'] ? updates['BW Admin Staff'].value : (parseSheetNum(dataRows[targetRowIdx][5]) || 0);
    var currentActiveFTE = startOfPeriodFTE;
    if (adminStaff > 0 && currentActiveFTE > 0) {
      updates['Active Staff Per Admin'] = { col: 2, value: Math.round((currentActiveFTE / adminStaff) * 100) / 100 };
    }

    // Col 8: Show Rate = Fabius Calls / MQLs (skip if no Fabius data — col 7 is manual)
    var fabiusCalls = parseSheetNum(dataRows[targetRowIdx][7]) || 0;
    if (fabiusCalls > 0 && _mqlCount > 0) {
      updates['Show Rate'] = { col: 8, value: Math.round((fabiusCalls / _mqlCount) * 10000) / 10000 };
    }

    // Col 10: New Client Jobs vs Discovery Calls = New Client Jobs / Fabius Calls
    var _newClientJobs = updates['New Client Jobs Opened'] ? updates['New Client Jobs Opened'].value : (parseSheetNum(dataRows[targetRowIdx][9]) || 0);
    if (fabiusCalls > 0 && _newClientJobs > 0) {
      updates['New Client Jobs vs Discovery Calls'] = { col: 10, value: Math.round((_newClientJobs / fabiusCalls) * 10000) / 10000 };
    }

    // Col 13: FTE Close Rate (1 month window) = Total FTE Hires / prev month MQLs
    if (prevMQLs > 0) {
      updates['FTE Close Rate'] = { col: 13, value: Math.round((_totalFTEHires / prevMQLs) * 10000) / 10000 };
    }

    // Col 14: FTE Conv Close Rate from Job = Total FTE Hires / prev month New Client Jobs
    if (prevNewClientJobs > 0) {
      updates['FTE Conv Close Rate from Job'] = { col: 14, value: Math.round((_totalFTEHires / prevNewClientJobs) * 10000) / 10000 };
    }

    // Col 17: Sales Conversion Rate = New Client Jobs / MQLs
    if (_mqlCount > 0 && _newClientJobs > 0) {
      updates['Sales Conversion Rate'] = { col: 17, value: Math.round((_newClientJobs / _mqlCount) * 10000) / 10000 };
    }

    // Col 25: Endorsements Per Recruitment HC
    var endorsements = updates['Endorsements'] ? updates['Endorsements'].value : (parseSheetNum(dataRows[targetRowIdx][24]) || 0);
    var recruitmentHC = updates['Recruitment Team HC'] ? updates['Recruitment Team HC'].value : (parseSheetNum(dataRows[targetRowIdx][23]) || 0);
    if (recruitmentHC > 0 && endorsements > 0) {
      updates['Endorsements Per Recruitment HC'] = { col: 25, value: Math.round((endorsements / recruitmentHC) * 100) / 100 };
    }

    // Col 27: Staff churned <1 month as % of last month hires
    // Col 26 is <1 month FTE churned (formula in sheet, but we compute here)
    // For now skip 26 as it needs specific ticket-level data already computed above
    if (prevTotalHires > 0 && under30FTE > 0) {
      updates['Staff churned <1m as % of hires'] = { col: 27, value: Math.round((under30FTE / prevTotalHires) * 10000) / 10000 };
    }

    // Col 33: Hires Per Sales/Recruitment HC
    var salesHC = parseSheetNum(dataRows[targetRowIdx][22]) || 0;
    if ((recruitmentHC + salesHC) > 0) {
      updates['Hires Per Sales/Recruitment HC'] = { col: 33, value: Math.round((_totalFTEHires / (recruitmentHC + salesHC)) * 100) / 100 };
    }

    // Col 37: Backfill rate = Roles to be backfilled / total offboardings
    var rolesToBackfill = updates['Roles to be backfilled'] ? updates['Roles to be backfilled'].value : (parseSheetNum(dataRows[targetRowIdx][36]) || 0);
    if (_lostFTEs > 0 && rolesToBackfill > 0) {
      updates['Backfill rate'] = { col: 37, value: Math.round((rolesToBackfill / _lostFTEs) * 10000) / 10000 };
    }

    // Col 39: Backfill Close Rate = Backfill FTE hires / prev month churned
    if (prevChurned > 0 && _backfillFTE > 0) {
      updates['Backfill Close Rate'] = { col: 39, value: Math.round((_backfillFTE / prevChurned) * 10000) / 10000 };
    }

    // Col 44: FTE/Client Expansion Rate = Existing Client FTE / Existing Clients
    // Use freshly fetched value if available, otherwise fall back to sheet
    var existingClients = (updates['Number of Existing Clients'] ? updates['Number of Existing Clients'].value : 0) || parseSheetNum(dataRows[targetRowIdx][40]) || 0;
    if (existingClients > 0 && _existingClientFTE > 0) {
      updates['FTE/Client Expansion Rate'] = { col: 44, value: Math.round((_existingClientFTE / existingClients) * 10000) / 10000 };
    }

    // Col 45: Jobs Expansion Rate = Existing Client Jobs / prev month Existing Clients
    var existingClientJobs = parseSheetNum(dataRows[targetRowIdx][41]) || 0;
    if (prevExistingClients > 0 && existingClientJobs > 0) {
      updates['Jobs Expansion Rate'] = { col: 45, value: Math.round((existingClientJobs / prevExistingClients) * 10000) / 10000 };
    }

    // Col 46: FTE Close Rate Existing Client = Existing Client FTE / prev month Existing Client Jobs
    var prevExistingClientJobs = parseSheetNum(prevRow[41]) || 0;
    if (prevExistingClientJobs > 0 && _existingClientFTE > 0) {
      updates['FTE Close Rate Existing Client'] = { col: 46, value: Math.round((_existingClientFTE / prevExistingClientJobs) * 10000) / 10000 };
    }

    // Col 48: New Client Hires — now set directly from job_source="New" in hire loop above

    // Col 49: New Client FTE Conv Rate = New Client Hires / prev month MQLs
    var newClientHires = updates['New Client Hires'] ? updates['New Client Hires'].value : 0;
    if (prevMQLs > 0 && newClientHires > 0) {
      updates['New Client FTE Conv Rate'] = { col: 49, value: Math.round((newClientHires / prevMQLs) * 10000) / 10000 };
    }

    // ===== Active FTE: First recalculate CURRENT month's Col 4 from prev month, then compute next month =====
    console.log('[KPI Generate] Computing Active FTE for ' + month + ' and next month...');
    try {
      var currentCol4;
      // Step 1: Recalculate current month's Col 4 = prev month Col 4 + prev month net FTE
      if (targetRowIdx > 0) {
        var prevCol4 = parseSheetNum(dataRows[targetRowIdx - 1][4]) || 0;
        var prevHires = parseSheetNum(dataRows[targetRowIdx - 1][12]) || 0;
        var prevChurnedFTE = parseSheetNum(dataRows[targetRowIdx - 1][34]) || 0;
        var prevNetFTE = prevHires - prevChurnedFTE;
        currentCol4 = Math.round((prevCol4 + prevNetFTE) * 100) / 100;
        // Write corrected value to current month's Col 4
        var currentCell = KPI_SOURCE_TAB + '!' + colLetter(4) + sheetRow;
        await sheetsUpdate(KPI_SOURCE_SHEET_ID, currentCell, [[currentCol4]]);
        console.log('[KPI Generate] Recalculated current month Col 4: ' + currentCol4 + ' (prev ' + prevCol4 + ' + prevNet ' + prevNetFTE + ' [hires ' + prevHires + ' - churned ' + prevChurnedFTE + '])');
      } else {
        currentCol4 = parseSheetNum(dataRows[targetRowIdx][4]) || 0;
        console.log('[KPI Generate] First month row, using existing Col 4: ' + currentCol4);
      }

      // Update startOfPeriodFTE and dependent calculations with corrected value
      startOfPeriodFTE = currentCol4;
      currentActiveFTE = currentCol4;
      if (startOfPeriodFTE > 0) {
        updates['Role Churn Rate'] = { col: 35, value: Math.round((_lostFTEs / startOfPeriodFTE) * 10000) / 10000 };
      }
      if (adminStaff > 0 && currentActiveFTE > 0) {
        updates['Active Staff Per Admin'] = { col: 2, value: Math.round((currentActiveFTE / adminStaff) * 100) / 100 };
      }

      // Step 2: Compute next month's Col 4 = current month Col 4 + this month's net FTE
      var netFTE = _totalFTEHires - _lostFTEs;
      var nextActiveFTE = Math.round((currentCol4 + netFTE) * 100) / 100;
      var nextRowIdx = targetRowIdx + 1;
      if (nextRowIdx < dataRows.length) {
        var nextSheetRow = nextRowIdx + 4;
        var nextCell = KPI_SOURCE_TAB + '!' + colLetter(4) + nextSheetRow;
        await sheetsUpdate(KPI_SOURCE_SHEET_ID, nextCell, [[nextActiveFTE]]);
        console.log('[KPI Generate] Wrote next month Active FTE ' + nextActiveFTE + ' to row ' + nextSheetRow + ' (' + currentCol4 + ' + net ' + netFTE + ')');
      }
    } catch (activErr) {
      console.error('[KPI Generate] Active FTE calc failed:', activErr.message);
    }

    // Write updates to the source sheet (current month row)
    var written = [];
    for (var key in updates) {
      var u = updates[key];
      var cell = KPI_SOURCE_TAB + '!' + colLetter(u.col) + sheetRow;
      await sheetsUpdate(KPI_SOURCE_SHEET_ID, cell, [[u.value]]);
      written.push(key + ' = ' + u.value + ' (' + cell + ')');
      console.log('[KPI Generate] Wrote ' + key + ' = ' + u.value + ' to ' + cell);
    }

    // Invalidate cache so next GET reflects changes
    kpiHistoryCache = { data: null, ts: 0 };

    res.json({
      success: true,
      month: month,
      sheetRow: sheetRow,
      updated: written,
      errors: errors.length > 0 ? errors : undefined,
      message: written.length > 0
        ? 'Updated ' + written.length + ' field(s)' + (errors.length > 0 ? ' (' + errors.length + ' error(s))' : '')
        : 'No matching columns found in sheet headers'
    });
  } catch (err) {
    console.error('KPI generate error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/kpi', function(req, res) {
  res.sendFile(path.join(__dirname, 'kpi-history.html'));
});

// ===== Internal Costs =====
var BW_INTERNAL_PIPELINE = '16984077';
var AUD_FX_RATES = {
  PHP: 0.027, ZAR: 0.083, USD: 1.55, KES: 0.012, COP: 0.00037,
  IDR: 0.000095, ARS: 0.0013, SGD: 1.18, GTQ: 0.20, AUD: 1.0,
  NPR: 0.0115, THB: 0.046, EGP: 0.031, TTD: 0.23, MYR: 0.35,
  XAF: 0.0025, INR: 0.019, HNL: 0.061, MXN: 0.077
};
var HOURS_PER_MONTH = 173;
var IC_STAGE_ACTIVE = '43691692';
var IC_STAGES_PREACTIVE = ['43261242', '43261244', '43261245', '45761800'];
var IC_STAGE_LABELS = {
  '43261242': 'New Endorsement',
  '43261244': 'Email Creation, Contact Update, Deputy Creation',
  '43261245': 'NH Invite and NHO Training',
  '45761800': 'Contract Creation',
  '43691692': 'Active Staff',
  '49707899': 'Active Staff'
};
// Employee pipeline: BruntWork client only
var BW_EMPLOYEE_PIPELINE = '20565603';
var IC_EMPLOYEE_STAGE_ACTIVE = '49707899';
var IC_EXCLUDED_NAMES = ['Pamela Larranaga', 'Michelle Kacarovski'];

// ===== Candidate Interviews =====
var APPLICATIONS_OBJECT = '2-38227027';
var CI_OUTCOME_SLOTS = [
  { prop: 'n1st_client_interview_outcome', dateProp: 'n1st_client_interview_date' },
  { prop: 'n2nd_client_interview_outcome', dateProp: 'n2nd_client_interview_date' },
  { prop: 'n3rd_client_interview_outcome', dateProp: 'n3rd_client_interview_date' },
  { prop: 'n4th_client_interview_outcome', dateProp: 'n4th_interview_date_and_time__your_timezone_' },
  { prop: 'n5th_client_interview_outcome', dateProp: 'n5th_interview_date_and_time__your_timezone_' }
];

// Cache job_source options (5 min)
var jobSourceOptionsCache = { options: null, ts: 0 };
app.get('/api/candidate-interviews/job-sources', async function(req, res) {
  try {
    if (jobSourceOptionsCache.options && (Date.now() - jobSourceOptionsCache.ts) < 300000) {
      return res.json({ options: jobSourceOptionsCache.options, cached: true });
    }
    var url = 'https://api.hubapi.com/crm/v3/properties/' + APPLICATIONS_OBJECT + '/job_source';
    var headers = { 'Content-Type': 'application/json' };
    if (HUBSPOT_KEY && HUBSPOT_KEY.startsWith('pat-')) headers['Authorization'] = 'Bearer ' + HUBSPOT_KEY;
    var r = await fetch(url, { headers: headers });
    var data = await r.json();
    var options = (data.options || []).map(function(o) {
      return { value: o.value, label: o.label };
    });
    jobSourceOptionsCache = { options: options, ts: Date.now() };
    res.json({ options: options, cached: false });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Week key helper (ISO week — Monday start, YYYY-Www)
function weekKeyForDate(dateStr) {
  if (!dateStr) return null;
  var d = new Date(dateStr);
  if (isNaN(d.getTime())) return null;
  // Use UTC to avoid TZ drift. Normalise to Monday of that week.
  var utc = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  // Monday-based: getUTCDay: 0=Sun, 1=Mon...6=Sat — shift so Mon=0
  var dow = (utc.getUTCDay() + 6) % 7;
  utc.setUTCDate(utc.getUTCDate() - dow);
  var y = utc.getUTCFullYear();
  var m = String(utc.getUTCMonth() + 1).padStart(2, '0');
  var da = String(utc.getUTCDate()).padStart(2, '0');
  return y + '-' + m + '-' + da; // Monday of that week
}

// GET /api/candidate-interviews?jobSource=xxx&weeks=26
app.get('/api/candidate-interviews', async function(req, res) {
  try {
    var jobSource = (req.query.jobSource || '').trim();
    var weeks = parseInt(req.query.weeks) || 26;
    if (weeks < 1) weeks = 1;
    if (weeks > 104) weeks = 104;

    // Compute from/to window (inclusive of last <weeks> full weeks including current)
    var now = new Date();
    var todayUTC = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    var dow = (todayUTC.getUTCDay() + 6) % 7;
    var thisMon = new Date(todayUTC);
    thisMon.setUTCDate(thisMon.getUTCDate() - dow);
    var startMon = new Date(thisMon);
    startMon.setUTCDate(startMon.getUTCDate() - (weeks - 1) * 7);
    var fromMs = String(startMon.getTime());
    var toMs = String(todayUTC.getTime() + 24*60*60*1000); // include today

    // Build 5 filter groups (OR): each group requires outcome set AND date in range, plus optional job_source
    var filterGroups = CI_OUTCOME_SLOTS.map(function(slot) {
      var filters = [
        { propertyName: slot.prop, operator: 'HAS_PROPERTY' },
        { propertyName: slot.dateProp, operator: 'GTE', value: fromMs },
        { propertyName: slot.dateProp, operator: 'LTE', value: toMs }
      ];
      if (jobSource) filters.push({ propertyName: 'job_source', operator: 'EQ', value: jobSource });
      return { filters: filters };
    });

    // Fetch all pages
    var properties = ['job_source'];
    CI_OUTCOME_SLOTS.forEach(function(s) { properties.push(s.prop); properties.push(s.dateProp); });

    var all = [];
    var after = undefined;
    var pages = 0;
    while (true) {
      var body = { filterGroups: filterGroups, properties: properties, limit: 200 };
      if (after) body.after = after;
      if (pages > 0) await sleep(300);
      var data = await hubspotSearchObject(APPLICATIONS_OBJECT, body);
      all = all.concat(data.results || []);
      if (data.paging && data.paging.next && data.paging.next.after) {
        after = data.paging.next.after;
      } else break;
      pages++;
      if (pages > 50) break; // hard safety cap — 10k applications
    }

    // Aggregate: for each application, for each slot where both outcome+date populated and date in range,
    // emit one event keyed by (week, outcome)
    var weekBuckets = {}; // weekKey -> { outcomeValue -> count }
    var outcomesSet = {};
    var weeksList = {};
    var totalEvents = 0;

    for (var i = 0; i < all.length; i++) {
      var p = all[i].properties || {};
      for (var s = 0; s < CI_OUTCOME_SLOTS.length; s++) {
        var slot = CI_OUTCOME_SLOTS[s];
        var outcome = p[slot.prop];
        var dateVal = p[slot.dateProp];
        if (!outcome || !dateVal) continue;
        var wk = weekKeyForDate(dateVal);
        if (!wk) continue;
        // Ensure within window (date could slightly exceed due to rounding)
        var wMs = new Date(wk).getTime();
        if (wMs < startMon.getTime() || wMs > todayUTC.getTime()) continue;
        outcomesSet[outcome] = true;
        weeksList[wk] = true;
        if (!weekBuckets[wk]) weekBuckets[wk] = {};
        weekBuckets[wk][outcome] = (weekBuckets[wk][outcome] || 0) + 1;
        totalEvents++;
      }
    }

    // Build weeks array (Monday-aligned) covering the full window, even for empty weeks
    var weekKeys = [];
    for (var w = 0; w < weeks; w++) {
      var d = new Date(startMon);
      d.setUTCDate(d.getUTCDate() + w * 7);
      var k = d.getUTCFullYear() + '-' + String(d.getUTCMonth()+1).padStart(2,'0') + '-' + String(d.getUTCDate()).padStart(2,'0');
      weekKeys.push(k);
    }

    var outcomes = Object.keys(outcomesSet).sort();

    res.json({
      weeks: weekKeys,
      outcomes: outcomes,
      buckets: weekBuckets,
      totalEvents: totalEvents,
      totalApplications: all.length,
      window: { from: fromMs, to: toMs, weeks: weeks },
      jobSource: jobSource || null
    });
  } catch (e) {
    console.error('[Candidate Interviews] error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/candidate-interviews', function(req, res) {
  res.sendFile(path.join(__dirname, 'candidate-interviews.html'));
});

app.get('/internal-costs', function(req, res) {
  res.sendFile(path.join(__dirname, 'internal-costs.html'));
});

app.get('/api/internal-costs', async function(req, res) {
  try {
    // Build stage list from query params
    var stageIds = [];
    var includeActive = req.query.active !== '0';
    var includePreActive = req.query.preactive === '1';
    if (includeActive) stageIds.push(IC_STAGE_ACTIVE);
    if (includePreActive) stageIds = stageIds.concat(IC_STAGES_PREACTIVE);
    if (stageIds.length === 0) stageIds.push(IC_STAGE_ACTIVE); // fallback



    var icProps = ['subject', 'bw_internal_secondary_team', 'bw_internal_hourly_rate', 'bw_internal_monthly_rate', 'staff_hourly_monthly_rate_currency', 'hs_pipeline_stage', 'role'];

    var tickets = await fetchAllPagesWithRetry({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'EQ', value: BW_INTERNAL_PIPELINE },
          { propertyName: 'hs_pipeline_stage', operator: 'IN', values: stageIds }
        ]
      }],
      properties: icProps
    });

    // Also fetch BruntWork employees from Employee pipeline (active stage only when active is included)
    if (includeActive) {
      var empTickets = await fetchAllPagesWithRetry({
        filterGroups: [{
          filters: [
            { propertyName: 'hs_pipeline', operator: 'EQ', value: BW_EMPLOYEE_PIPELINE },
            { propertyName: 'client', operator: 'CONTAINS_TOKEN', value: 'BruntWork' },
            { propertyName: 'hs_pipeline_stage', operator: 'EQ', value: IC_EMPLOYEE_STAGE_ACTIVE }
          ]
        }],
        properties: icProps
      });
      tickets = tickets.concat(empTickets);
    }

    var staff = [];
    var teamAgg = {};

    for (var i = 0; i < tickets.length; i++) {
      var p = tickets[i].properties || {};
      var name = (p.subject || '').replace(/, BruntWork.*$/i, '').trim() || 'Unknown';
      if (IC_EXCLUDED_NAMES.some(function(n) { return name.toLowerCase() === n.toLowerCase(); })) continue;
      var team = p.bw_internal_secondary_team || 'Unassigned';
      var hourly = parseFloat(p.bw_internal_hourly_rate) || 0;
      var monthly = parseFloat(p.bw_internal_monthly_rate) || 0;
      var currency = p.staff_hourly_monthly_rate_currency || '';
      var stageId = p.hs_pipeline_stage || '';
      var stageLabel = IC_STAGE_LABELS[stageId] || stageId;
      var role = p.role || '';

      var localMonthly = monthly > 0 ? monthly : (hourly > 0 ? hourly * HOURS_PER_MONTH : 0);
      var rateType = monthly > 0 ? 'monthly' : (hourly > 0 ? 'hourly' : 'none');
      var fxRate = currency ? (AUD_FX_RATES[currency] || 0) : 0;
      var audMonthly = Math.round(localMonthly * fxRate * 100) / 100;

      staff.push({
        id: tickets[i].id,
        name: name,
        team: team,
        currency: currency,
        hourlyRate: hourly,
        monthlyRate: monthly,
        rateType: rateType,
        localMonthly: Math.round(localMonthly * 100) / 100,
        audMonthly: audMonthly,
        stage: stageLabel,
        role: role
      });

      if (!teamAgg[team]) teamAgg[team] = { headcount: 0, totalAud: 0 };
      teamAgg[team].headcount++;
      teamAgg[team].totalAud += audMonthly;
    }

    // Sort staff by team then name
    staff.sort(function(a, b) {
      if (a.team < b.team) return -1;
      if (a.team > b.team) return 1;
      return a.name.localeCompare(b.name);
    });

    // Build sorted team summary
    var teams = Object.keys(teamAgg).map(function(t) {
      return { team: t, headcount: teamAgg[t].headcount, totalAud: Math.round(teamAgg[t].totalAud * 100) / 100 };
    }).sort(function(a, b) { return b.totalAud - a.totalAud; });

    var totalHeadcount = staff.length;
    var totalAud = Math.round(teams.reduce(function(s, t) { return s + t.totalAud; }, 0) * 100) / 100;

    res.json({ staff: staff, teams: teams, totalHeadcount: totalHeadcount, totalAud: totalAud });
  } catch (err) {
    console.error('Internal costs error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ===== Gong API =====
app.get('/sales', function(req, res) {
  res.sendFile(path.join(__dirname, 'gong.html'));
});
app.get('/gong', function(req, res) {
  res.redirect(301, '/sales');
});

async function gongFetch(endpoint, options) {
  var opts = Object.assign({
    headers: {
      'Authorization': 'Basic ' + Buffer.from(GONG_API_KEY + ':' + GONG_API_SECRET).toString('base64'),
      'Content-Type': 'application/json'
    }
  }, options || {});
  var resp = await fetch(GONG_BASE_URL + endpoint, opts);
  if (!resp.ok) throw new Error('Gong API error: ' + resp.status + ' ' + resp.statusText);
  return resp.json();
}

async function gongFetchAllPages(endpoint, options, getRecords) {
  var allRecords = [];
  var cursor = null;
  do {
    var url = endpoint;
    if (cursor) url += (url.includes('?') ? '&' : '?') + 'cursor=' + encodeURIComponent(cursor);
    var data = await gongFetch(url, options);
    var records = getRecords(data);
    allRecords = allRecords.concat(records);
    cursor = data.records && data.records.cursor ? data.records.cursor : null;
  } while (cursor);
  return allRecords;
}

// Cache users for 1 hour
var gongUsersCache = { data: null, ts: 0 };
async function getGongUsers() {
  var now = Date.now();
  if (gongUsersCache.data && (now - gongUsersCache.ts) < 3600000) return gongUsersCache.data;
  var users = await gongFetchAllPages('/users', {}, function(d) { return d.users || []; });
  var map = {};
  for (var i = 0; i < users.length; i++) {
    map[users[i].id] = users[i];
  }
  gongUsersCache = { data: map, ts: now };
  return map;
}

var DISCOVERY_TITLE_PATTERN = /Strategy Call/i;
var MIN_CALL_DURATION = 240; // 4 minutes

function parseAgentFromTitle(title) {
  // Title format: "BruntWork Offshore Staffing Strategy Call w/ [Agent] & [Client]"
  var match = title.match(/w\/\s*([^&]+)/i);
  if (match) return match[1].trim();
  return null;
}

// ===== Gong Cache: persist to KPI source spreadsheet =====
var GONG_CACHE_SHEET = KPI_SOURCE_SHEET_ID; // use the same sheet that KPI history uses
var gongCacheTabReady = false;

async function ensureGongCacheTab() {
  if (gongCacheTabReady) return;
  try {
    var token = await getGoogleAccessToken();
    var metaResp = await fetch(
      'https://sheets.googleapis.com/v4/spreadsheets/' + encodeURIComponent(GONG_CACHE_SHEET) + '?fields=sheets.properties',
      { headers: { 'Authorization': 'Bearer ' + token } }
    );
    var meta = await metaResp.json();
    var existing = (meta.sheets || []).map(function(s) { return s.properties.title; });
    var requests = [];
    if (existing.indexOf('Gong Cache') === -1) {
      requests.push({ addSheet: { properties: { title: 'Gong Cache' } } });
    }
    if (existing.indexOf('Conversion Cache') === -1) {
      requests.push({ addSheet: { properties: { title: 'Conversion Cache' } } });
    }
    if (requests.length > 0) {
      await fetch(
        'https://sheets.googleapis.com/v4/spreadsheets/' + encodeURIComponent(GONG_CACHE_SHEET) + ':batchUpdate',
        { method: 'POST', headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' }, body: JSON.stringify({ requests: requests }) }
      );
      console.log('[Gong Cache] Created cache tabs');
    }
    // Ensure headers
    try {
      var h1 = await sheetsGet(GONG_CACHE_SHEET, 'Gong Cache!A1:C1');
      if (!h1.values || !h1.values[0] || h1.values[0][0] !== 'month') {
        await sheetsUpdate(GONG_CACHE_SHEET, 'Gong Cache!A1:C1', [['month', 'discovery_calls', 'updated_at']]);
      }
    } catch (e) {
      await sheetsUpdate(GONG_CACHE_SHEET, 'Gong Cache!A1:C1', [['month', 'discovery_calls', 'updated_at']]);
    }
    try {
      var h2 = await sheetsGet(GONG_CACHE_SHEET, 'Conversion Cache!A1:C1');
      if (!h2.values || !h2.values[0] || h2.values[0][0] !== 'month') {
        await sheetsUpdate(GONG_CACHE_SHEET, 'Conversion Cache!A1:C1', [['month', 'data_json', 'updated_at']]);
      }
    } catch (e) {
      await sheetsUpdate(GONG_CACHE_SHEET, 'Conversion Cache!A1:C1', [['month', 'data_json', 'updated_at']]);
    }
    gongCacheTabReady = true;
    console.log('[Gong Cache] Tabs ready');
  } catch (e) {
    console.error('[Gong Cache] Tab init error:', e.message);
  }
}

// Save a monthly discovery call count
async function saveGongCountToSheet(month, count) {
  try {
    await ensureGongCacheTab();
    var existing = await sheetsGet(GONG_CACHE_SHEET, 'Gong Cache!A:C');
    var rows = (existing.values || []);
    var rowIdx = -1;
    for (var i = 1; i < rows.length; i++) {
      if (rows[i][0] === month) { rowIdx = i; break; }
    }
    var now = new Date().toISOString();
    if (rowIdx >= 0) {
      await sheetsUpdate(GONG_CACHE_SHEET, 'Gong Cache!A' + (rowIdx + 1) + ':C' + (rowIdx + 1), [[month, count, now]]);
    } else {
      await sheetsAppend(GONG_CACHE_SHEET, 'Gong Cache!A:C', [[month, count, now]]);
    }
    console.log('[Gong Cache] Saved ' + month + ' = ' + count);
  } catch (e) {
    console.error('[Gong Cache] Failed to save:', e.message);
  }
}

// Read all monthly discovery call counts
async function readGongCacheFromSheet() {
  try {
    await ensureGongCacheTab();
    var data = await sheetsGet(GONG_CACHE_SHEET, 'Gong Cache!A:C');
    var rows = (data.values || []);
    var counts = {};
    for (var i = 1; i < rows.length; i++) {
      if (rows[i][0] && rows[i][1] != null) {
        counts[rows[i][0]] = parseInt(rows[i][1]) || 0;
      }
    }
    return counts;
  } catch (e) {
    console.error('[Gong Cache] Failed to read:', e.message);
    return {};
  }
}

// Cache for discovery calls endpoint: keyed by "from|to"
var discoveryCallsCache = {};
var DISCOVERY_CALLS_TTL = Infinity; // never expires — refresh via Load button

app.get('/api/gong/discovery-calls', async function(req, res) {
  try {
    var from = req.query.from;
    var to = req.query.to;
    if (!from || !to) return res.status(400).json({ error: 'from and to date parameters required (YYYY-MM-DD)' });

    var forceRefresh = req.query.refresh === 'true';
    var cacheKey = from + '|' + to;
    var cached = discoveryCallsCache[cacheKey];
    if (!forceRefresh && cached) {
      console.log('[Discovery Calls] Returning cached data for ' + cacheKey);
      return res.json(Object.assign({}, cached.data, { cached: true, cachedAt: new Date(cached.ts).toISOString() }));
    }

    // Fetch all calls for the date range (paginated)
    var allCalls = await gongFetchAllPages(
      '/calls?fromDateTime=' + from + 'T00:00:00Z&toDateTime=' + to + 'T00:00:00Z',
      {},
      function(d) { return d.calls || []; }
    );

    var totalScanned = allCalls.length;

    // Filter: title matches pattern AND duration > 4 minutes
    var discoveryCalls = [];
    for (var i = 0; i < allCalls.length; i++) {
      var call = allCalls[i];
      if (DISCOVERY_TITLE_PATTERN.test(call.title || '') && (call.duration || 0) > MIN_CALL_DURATION) {
        discoveryCalls.push(call);
      }
    }

    // Aggregate by agent
    var agentMap = {};
    var totalDuration = 0;
    for (var j = 0; j < discoveryCalls.length; j++) {
      var c = discoveryCalls[j];
      var agent = parseAgentFromTitle(c.title) || 'Unknown';
      totalDuration += c.duration || 0;
      if (!agentMap[agent]) {
        agentMap[agent] = { agent: agent, calls: 0, totalDuration: 0, shortest: Infinity, longest: 0 };
      }
      agentMap[agent].calls++;
      agentMap[agent].totalDuration += c.duration || 0;
      if (c.duration < agentMap[agent].shortest) agentMap[agent].shortest = c.duration;
      if (c.duration > agentMap[agent].longest) agentMap[agent].longest = c.duration;
    }

    var agents = Object.values(agentMap).map(function(a) {
      return {
        agent: a.agent,
        calls: a.calls,
        avgDuration: Math.round(a.totalDuration / a.calls),
        totalDuration: a.totalDuration,
        shortest: a.shortest === Infinity ? 0 : a.shortest,
        longest: a.longest
      };
    });

    var result = {
      from: from,
      to: to,
      totalCallsScanned: totalScanned,
      totalDiscoveryCalls: discoveryCalls.length,
      avgDuration: discoveryCalls.length > 0 ? Math.round(totalDuration / discoveryCalls.length) : 0,
      agents: agents
    };

    discoveryCallsCache[cacheKey] = { data: result, ts: Date.now() };

    // Persist to Google Sheet if this is a full-month range (from = YYYY-MM-01)
    if (/^\d{4}-\d{2}-01$/.test(from)) {
      var monthKey = from.substring(0, 7); // YYYY-MM
      try { await saveGongCountToSheet(monthKey, discoveryCalls.length); } catch(e) { console.error('[Gong Cache] Save error:', e.message); }
    }

    res.json(result);
  } catch (err) {
    console.error('Gong discovery calls error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ===== Gong: Client Interviews =====
var INTERVIEW_TITLE_PATTERN = /^Candidate Interview with BruntWork \|/i;
var interviewCache = {}; // keyed by "from|to"

// Save/read client interview cache to Google Sheet
async function ensureInterviewCacheTab() {
  try {
    var meta = await sheetsGet(GONG_CACHE_SHEET, '');
    var sheets = (meta.sheets || []).map(function(s) { return s.properties.title; });
    if (sheets.indexOf('Interview Cache') === -1) {
      await fetch('https://sheets.googleapis.com/v4/spreadsheets/' + GONG_CACHE_SHEET + ':batchUpdate', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + (await getAccessToken()), 'Content-Type': 'application/json' },
        body: JSON.stringify({ requests: [{ addSheet: { properties: { title: 'Interview Cache' } } }] })
      });
      await sheetsUpdate(GONG_CACHE_SHEET, 'Interview Cache!A1:C1', [['Month', 'Data', 'Updated']]);
    }
  } catch (e) { console.error('[Interview Cache] ensureTab error:', e.message); }
}

async function saveInterviewToSheet(month, data) {
  try {
    await ensureInterviewCacheTab();
    var existing = await sheetsGet(GONG_CACHE_SHEET, 'Interview Cache!A:C');
    var rows = (existing.values || []);
    var rowIdx = -1;
    for (var i = 1; i < rows.length; i++) {
      if (rows[i][0] === month) { rowIdx = i; break; }
    }
    var now = new Date().toISOString();
    var json = JSON.stringify(data);
    if (rowIdx >= 0) {
      await sheetsUpdate(GONG_CACHE_SHEET, 'Interview Cache!A' + (rowIdx + 1) + ':C' + (rowIdx + 1), [[month, json, now]]);
    } else {
      await sheetsAppend(GONG_CACHE_SHEET, 'Interview Cache!A:C', [[month, json, now]]);
    }
    console.log('[Interview Cache] Saved ' + month);
  } catch (e) { console.error('[Interview Cache] Save error:', e.message); }
}

async function readInterviewFromSheet(month) {
  try {
    await ensureInterviewCacheTab();
    var data = await sheetsGet(GONG_CACHE_SHEET, 'Interview Cache!A:C');
    var rows = (data.values || []);
    for (var i = 1; i < rows.length; i++) {
      if (rows[i][0] === month && rows[i][1]) {
        return { data: JSON.parse(rows[i][1]), cachedAt: rows[i][2] };
      }
    }
    return null;
  } catch (e) { console.error('[Interview Cache] Read error:', e.message); return null; }
}

app.get('/api/gong/client-interviews', async function(req, res) {
  try {
    var from = req.query.from;
    var to = req.query.to;
    if (!from || !to) return res.status(400).json({ error: 'from and to date parameters required' });

    var forceRefresh = req.query.refresh === 'true';
    var cacheKey = from + '|' + to;
    var monthKey = from.substring(0, 7);

    // 1. In-memory cache
    if (!forceRefresh && interviewCache[cacheKey]) {
      return res.json(Object.assign({}, interviewCache[cacheKey].data, { cached: true, cachedAt: new Date(interviewCache[cacheKey].ts).toISOString() }));
    }

    // 2. Google Sheet cache
    if (!forceRefresh) {
      var sheetData = await readInterviewFromSheet(monthKey);
      if (sheetData) {
        interviewCache[cacheKey] = { data: sheetData.data, ts: new Date(sheetData.cachedAt).getTime() };
        return res.json(Object.assign({}, sheetData.data, { cached: true, cachedAt: sheetData.cachedAt }));
      }
    }

    // 3. Live fetch from Gong
    var users = await getGongUsers();
    var allCalls = await gongFetchAllPages(
      '/calls?fromDateTime=' + from + 'T00:00:00Z&toDateTime=' + to + 'T00:00:00Z',
      {},
      function(d) { return d.calls || []; }
    );

    var totalScanned = allCalls.length;
    var interviews = [];
    for (var i = 0; i < allCalls.length; i++) {
      var call = allCalls[i];
      if (INTERVIEW_TITLE_PATTERN.test(call.title || '')) {
        interviews.push(call);
      }
    }

    // Aggregate by host (ownerId -> user name)
    var hostMap = {};
    var totalDuration = 0;
    for (var j = 0; j < interviews.length; j++) {
      var c = interviews[j];
      var ownerId = c.ownerId || 'unknown';
      var user = users[ownerId];
      var hostName = user ? ((user.firstName || '') + ' ' + (user.lastName || '')).trim() : 'Unknown';
      totalDuration += c.duration || 0;
      if (!hostMap[hostName]) {
        hostMap[hostName] = { host: hostName, calls: 0, totalDuration: 0, shortest: Infinity, longest: 0 };
      }
      hostMap[hostName].calls++;
      hostMap[hostName].totalDuration += c.duration || 0;
      if (c.duration < hostMap[hostName].shortest) hostMap[hostName].shortest = c.duration;
      if (c.duration > hostMap[hostName].longest) hostMap[hostName].longest = c.duration;
    }

    var hosts = Object.values(hostMap).map(function(h) {
      return {
        host: h.host,
        calls: h.calls,
        avgDuration: Math.round(h.totalDuration / h.calls),
        totalDuration: h.totalDuration,
        shortest: h.shortest === Infinity ? 0 : h.shortest,
        longest: h.longest
      };
    });

    var result = {
      from: from,
      to: to,
      totalCallsScanned: totalScanned,
      totalInterviews: interviews.length,
      avgDuration: interviews.length > 0 ? Math.round(totalDuration / interviews.length) : 0,
      hosts: hosts
    };

    interviewCache[cacheKey] = { data: result, ts: Date.now() };

    // Persist to sheet
    if (/^\d{4}-\d{2}-01$/.test(from)) {
      try { await saveInterviewToSheet(monthKey, result); } catch(e) { console.error('[Interview Cache] Save error:', e.message); }
    }

    res.json(result);
  } catch (err) {
    console.error('Gong client interviews error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/gong/client-interviews-cached', async function(req, res) {
  try {
    var month = req.query.month;
    if (!month) return res.status(400).json({ error: 'month required' });
    var sheetData = await readInterviewFromSheet(month);
    if (sheetData) return res.json(Object.assign({}, sheetData.data, { cached: true, cachedAt: sheetData.cachedAt }));
    res.json({ empty: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ===== Gong: Read persisted discovery call counts from sheet =====
app.get('/api/gong/sheet-counts', async function(req, res) {
  try {
    var counts = await readGongCacheFromSheet();
    res.json({ counts: counts });
  } catch (err) {
    console.error('Gong sheet counts error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ===== Gong: Monthly Discovery Call Counts (cached, batch) =====
var discoveryCountCache = {}; // { 'YYYY-MM': { count, ts } }
var DISCOVERY_COUNT_TTL = Infinity; // never expires — refresh via Load button

async function getDiscoveryCountForMonth(month) {
  var cached = discoveryCountCache[month];
  if (cached && (Date.now() - cached.ts) < DISCOVERY_COUNT_TTL) {
    return cached.count;
  }

  var parts = month.split('-');
  var y = parseInt(parts[0]), m = parseInt(parts[1]);
  var from = month + '-01';
  var nextM = m === 12 ? (y + 1) + '-01' : y + '-' + String(m + 1).padStart(2, '0');
  var to = nextM + '-01';

  var allCalls = await gongFetchAllPages(
    '/calls?fromDateTime=' + from + 'T00:00:00Z&toDateTime=' + to + 'T00:00:00Z',
    {},
    function(d) { return d.calls || []; }
  );

  var count = 0;
  for (var i = 0; i < allCalls.length; i++) {
    var call = allCalls[i];
    if (DISCOVERY_TITLE_PATTERN.test(call.title || '') && (call.duration || 0) > MIN_CALL_DURATION) {
      count++;
    }
  }

  discoveryCountCache[month] = { count: count, ts: Date.now() };
  return count;
}

// Single month endpoint
app.get('/api/gong/monthly-discovery-count', async function(req, res) {
  try {
    var month = req.query.month;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) return res.status(400).json({ error: 'month param required (YYYY-MM)' });
    var count = await getDiscoveryCountForMonth(month);
    res.json({ month: month, totalDiscoveryCalls: count });
  } catch (err) {
    console.error('Gong monthly discovery count error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Batch endpoint: ?months=2026-01,2026-02,2026-03
app.get('/api/gong/monthly-discovery-counts', async function(req, res) {
  try {
    var monthsParam = req.query.months;
    if (!monthsParam) return res.status(400).json({ error: 'months param required (comma-separated YYYY-MM)' });
    var months = monthsParam.split(',').filter(function(m) { return /^\d{4}-\d{2}$/.test(m); });
    if (months.length === 0) return res.status(400).json({ error: 'no valid months provided' });

    // Fetch months sequentially to avoid Gong rate limits
    var results = {};
    for (var i = 0; i < months.length; i++) {
      try {
        results[months[i]] = await getDiscoveryCountForMonth(months[i]);
      } catch (e) {
        console.error('Error fetching Gong count for ' + months[i] + ':', e.message);
        results[months[i]] = null;
      }
    }

    res.json({ counts: results });
  } catch (err) {
    console.error('Gong batch discovery counts error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ===== Gong + HubSpot: Sales Agent Conversion Rates =====
var CLOSE_PIPELINES = ['4483329', '3857063', '20565603'];

async function hubspotGet(url) {
  var resp = await fetch(url, {
    headers: { Authorization: 'Bearer ' + HUBSPOT_KEY, 'Content-Type': 'application/json' }
  });
  if (!resp.ok) {
    var body = await resp.text();
    throw new Error('HubSpot GET error ' + resp.status + ': ' + body.slice(0, 200));
  }
  return resp.json();
}

async function hubspotPost(url, body) {
  var resp = await fetch(url, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + HUBSPOT_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!resp.ok) {
    var text = await resp.text();
    throw new Error('HubSpot POST error ' + resp.status + ': ' + text.slice(0, 200));
  }
  return resp.json();
}

// Get deals associated with a batch of ticket IDs
async function getDealsForTickets(ticketIds) {
  var dealMap = {}; // ticketId -> [dealIds]
  // Batch API allows up to 100 at a time
  for (var i = 0; i < ticketIds.length; i += 100) {
    var batch = ticketIds.slice(i, i + 100);
    var inputs = batch.map(function(id) { return { id: String(id) }; });
    if (i > 0) await sleep(300);
    try {
      var data = await hubspotPost(
        'https://api.hubapi.com/crm/v4/associations/tickets/deals/batch/read',
        { inputs: inputs }
      );
      var results = data.results || [];
      for (var r = 0; r < results.length; r++) {
        var from = results[r].from ? results[r].from.id : null;
        var to = results[r].to || [];
        if (from && to.length > 0) {
          dealMap[from] = to.map(function(t) { return t.toObjectId; });
        }
      }
    } catch (err) {
      console.error('[Conversion] Associations batch error:', err.message);
    }
  }
  return dealMap;
}

// Get sales_agent for a batch of deal IDs
async function getSalesAgentForDeals(dealIds) {
  var agentMap = {}; // dealId -> sales_agent
  var unique = [...new Set(dealIds)];
  for (var i = 0; i < unique.length; i += 100) {
    var batch = unique.slice(i, i + 100);
    if (i > 0) await sleep(300);
    try {
      var data = await hubspotPost(
        'https://api.hubapi.com/crm/v3/objects/deals/batch/read',
        { inputs: batch.map(function(id) { return { id: String(id) }; }), properties: ['sales_agent', 'dealname'] }
      );
      var results = data.results || [];
      for (var r = 0; r < results.length; r++) {
        var deal = results[r];
        if (deal.properties && deal.properties.sales_agent) {
          agentMap[deal.id] = deal.properties.sales_agent;
        }
      }
    } catch (err) {
      console.error('[Conversion] Deals batch read error:', err.message);
    }
  }
  return agentMap;
}

// ===== Conversion Cache Sheet persistence =====
async function saveConversionToSheet(month, data) {
  try {
    await ensureGongCacheTab();
    var existing = await sheetsGet(GONG_CACHE_SHEET, 'Conversion Cache!A:A');
    var rows = (existing.values || []);
    var rowIdx = -1;
    for (var i = 1; i < rows.length; i++) {
      if (rows[i][0] === month) { rowIdx = i; break; }
    }
    var now = new Date().toISOString();
    var json = JSON.stringify(data);
    if (rowIdx >= 0) {
      await sheetsUpdate(GONG_CACHE_SHEET, 'Conversion Cache!A' + (rowIdx + 1) + ':C' + (rowIdx + 1), [[month, json, now]]);
    } else {
      await sheetsAppend(GONG_CACHE_SHEET, 'Conversion Cache!A:C', [[month, json, now]]);
    }
    console.log('[Conversion Cache] Saved ' + month + ' (json length: ' + json.length + ')');
  } catch (e) {
    console.error('[Conversion Cache] Failed to save:', e.message);
  }
}

async function readConversionFromSheet(month) {
  try {
    await ensureGongCacheTab();
    var data = await sheetsGet(GONG_CACHE_SHEET, 'Conversion Cache!A:C');
    var rows = (data.values || []);
    for (var i = 1; i < rows.length; i++) {
      if (rows[i][0] === month && rows[i][1]) {
        return { data: JSON.parse(rows[i][1]), updatedAt: rows[i][2] || null };
      }
    }
    return null;
  } catch (e) {
    console.error('[Conversion Cache] Failed to read:', e.message);
    return null;
  }
}

// Endpoint to read cached conversion data from sheet (no Gong/HubSpot calls)
app.get('/api/gong/conversion-cached', async function(req, res) {
  try {
    var month = req.query.month;
    if (!month) return res.status(400).json({ error: 'month parameter required (YYYY-MM)' });
    var cached = await readConversionFromSheet(month);
    if (cached) {
      res.json(Object.assign({}, cached.data, { cached: true, cachedAt: cached.updatedAt }));
    } else {
      res.json({ empty: true, month: month });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Cache conversion data per month for 24 hours
var conversionCache = {}; // { 'YYYY-MM': { data, ts } }
var CONVERSION_CACHE_TTL = Infinity; // never expires — refresh via Load button

app.get('/api/gong/conversion', async function(req, res) {
  try {
    var month = req.query.month;
    if (!month) return res.status(400).json({ error: 'month parameter required (YYYY-MM)' });

    // Check cache (skip if refresh=true)
    var forceRefresh = req.query.refresh === 'true';
    var cached = conversionCache[month];
    if (!forceRefresh && cached) {
      console.log('[Conversion] Returning in-memory cached data for ' + month);
      return res.json(Object.assign({}, cached.data, { cached: true, cachedAt: new Date(cached.ts).toISOString() }));
    }

    // If not in memory and not forcing refresh, try loading from sheet
    if (!forceRefresh) {
      var sheetCached = await readConversionFromSheet(month);
      if (sheetCached) {
        console.log('[Conversion] Loaded from sheet cache for ' + month);
        conversionCache[month] = { data: sheetCached.data, ts: Date.now() };
        return res.json(Object.assign({}, sheetCached.data, { cached: true, cachedAt: sheetCached.updatedAt }));
      }
    }

    var parts = month.split('-');
    var y = parseInt(parts[0]), m = parseInt(parts[1]);

    // Date range for ticket creation (the "close" month)
    var closeFrom = new Date(Date.UTC(y, m - 1, 1));
    var closeTo = new Date(Date.UTC(m === 12 ? y + 1 : y, m === 12 ? 0 : m, 1));
    var closeFromMs = String(closeFrom.getTime());
    var closeToMs = String(closeTo.getTime());

    // Prior month for Gong calls
    var prevM = m === 1 ? 12 : m - 1;
    var prevY = m === 1 ? y - 1 : y;
    var gongFrom = prevY + '-' + String(prevM).padStart(2, '0') + '-01';
    var gongTo = y + '-' + String(m).padStart(2, '0') + '-01';

    console.log('[Conversion] Tickets: ' + closeFrom.toISOString().slice(0, 10) + ' to ' + closeTo.toISOString().slice(0, 10));
    console.log('[Conversion] Gong calls: ' + gongFrom + ' to ' + gongTo);

    // 1. Fetch tickets created in close month from the 3 pipelines
    var tickets = await fetchAllPages({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: CLOSE_PIPELINES },
          { propertyName: 'createdate', operator: 'GTE', value: closeFromMs },
          { propertyName: 'createdate', operator: 'LT', value: closeToMs }
        ]
      }],
      properties: ['createdate', 'hs_pipeline', 'subject', 'assignment_type'],
      sorts: [{ propertyName: 'createdate', direction: 'DESCENDING' }]
    });
    console.log('[Conversion] Tickets found: ' + tickets.length);

    // FTE weighting by assignment type
    function convFteWeight(type) {
      if (type === 'Full-Time') return 1;
      if (type === 'Part-Time') return 0.5;
      return 0.25; // PT-Under-20, Project-Based, Output-Based
    }

    // Build ticket FTE weight map
    var ticketFteMap = {};
    for (var tw = 0; tw < tickets.length; tw++) {
      ticketFteMap[String(tickets[tw].id)] = convFteWeight(tickets[tw].properties.assignment_type || '');
    }

    // 2. Get associated deals for each ticket
    var ticketIds = tickets.map(function(t) { return t.id; });
    var ticketDealMap = await getDealsForTickets(ticketIds);

    // 3. Collect all deal IDs and fetch sales_agent
    var allDealIds = [];
    Object.values(ticketDealMap).forEach(function(ids) { allDealIds = allDealIds.concat(ids); });
    var dealAgentMap = await getSalesAgentForDeals(allDealIds);

    // 4. Aggregate closures (FTE-weighted) per sales agent
    var closuresByAgent = {};
    for (var ti = 0; ti < ticketIds.length; ti++) {
      var tid = String(ticketIds[ti]);
      var fte = ticketFteMap[tid] || 1;
      var dealIds = ticketDealMap[tid] || [];
      for (var di = 0; di < dealIds.length; di++) {
        var agent = dealAgentMap[String(dealIds[di])];
        if (agent) {
          closuresByAgent[agent] = (closuresByAgent[agent] || 0) + fte;
        }
      }
    }

    // 5. Fetch Gong discovery calls from the prior month
    var allCalls = await gongFetchAllPages(
      '/calls?fromDateTime=' + gongFrom + 'T00:00:00Z&toDateTime=' + gongTo + 'T00:00:00Z',
      {},
      function(d) { return d.calls || []; }
    );
    var discoveryCalls = allCalls.filter(function(c) {
      return DISCOVERY_TITLE_PATTERN.test(c.title || '') && (c.duration || 0) > MIN_CALL_DURATION;
    });

    // Aggregate Gong calls by agent
    var gongByAgent = {};
    for (var gi = 0; gi < discoveryCalls.length; gi++) {
      var gAgent = parseAgentFromTitle(discoveryCalls[gi].title) || 'Unknown';
      gongByAgent[gAgent] = (gongByAgent[gAgent] || 0) + 1;
    }

    // 6. Build name-matching map (Gong first names -> HubSpot full names)
    // Gong titles use first names (e.g. "Ace"), HubSpot uses full names (e.g. "Ace Barcelona")
    // Nickname aliases: Gong name -> canonical first name used in HubSpot
    var NAME_ALIASES = {
      'anthony': 'tony'
    };
    var hsAgentNames = Object.keys(closuresByAgent);
    var gongAgentNames = Object.keys(gongByAgent);

    function matchGongToHS(gongName) {
      var gLower = gongName.toLowerCase().trim();
      var gFirst = gLower.split(' ')[0];
      var gFirstAliased = NAME_ALIASES[gFirst] || gFirst;
      // Try exact match first
      for (var h = 0; h < hsAgentNames.length; h++) {
        if (hsAgentNames[h].toLowerCase() === gLower) return hsAgentNames[h];
      }
      // Try first-name match (including aliases)
      for (var h2 = 0; h2 < hsAgentNames.length; h2++) {
        var hFirst = hsAgentNames[h2].split(' ')[0].toLowerCase();
        if (hFirst === gFirst || hFirst === gFirstAliased) return hsAgentNames[h2];
      }
      return null;
    }

    // 7. Build combined results
    var allAgents = new Set([...hsAgentNames, ...gongAgentNames]);
    var agents = [];
    var MONTH_NAMES = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    allAgents.forEach(function(name) {
      var gongCalls = gongByAgent[name] || 0;
      var closures = closuresByAgent[name] || 0;

      // Try matching if one side is zero
      if (gongCalls === 0 && closures > 0) {
        // This is an HS name, find matching Gong name
        for (var g = 0; g < gongAgentNames.length; g++) {
          if (matchGongToHS(gongAgentNames[g]) === name) {
            gongCalls = gongByAgent[gongAgentNames[g]];
            break;
          }
        }
      }
      if (closures === 0 && gongCalls > 0) {
        var hsMatch = matchGongToHS(name);
        if (hsMatch) closures = closuresByAgent[hsMatch] || 0;
      }

      // Skip if this is a duplicate (Gong name that matched an HS name already in the set)
      if (gongByAgent[name] && matchGongToHS(name) && matchGongToHS(name) !== name && allAgents.has(matchGongToHS(name))) {
        return; // skip duplicate
      }

      var closuresRound = Math.round(closures * 10) / 10;
      agents.push({
        agent: name,
        gongCalls: gongCalls,
        closures: closuresRound,
        conversionRate: gongCalls > 0 ? Math.round((closures / gongCalls) * 1000) / 10 : null
      });
    });

    // Sort by closures desc
    agents.sort(function(a, b) { return b.closures - a.closures; });

    var totalGong = discoveryCalls.length;
    var totalClosures = Math.round(Object.values(closuresByAgent).reduce(function(s, v) { return s + v; }, 0) * 10) / 10;

    var result = {
      month: month,
      closeMonth: MONTH_NAMES[m - 1] + ' ' + y,
      callsMonth: MONTH_NAMES[prevM - 1] + ' ' + prevY,
      totalTickets: tickets.length,
      totalClosures: totalClosures,
      totalGongCalls: totalGong,
      overallConversion: totalGong > 0 ? Math.round((totalClosures / totalGong) * 1000) / 10 : null,
      agents: agents
    };

    // Cache the result in memory and persist to Google Sheet
    conversionCache[month] = { data: result, ts: Date.now() };
    try { await saveConversionToSheet(month, result); } catch(e) { console.error('[Conversion Cache] Save error:', e.message); }
    console.log('[Conversion] Cached result for ' + month);

    res.json(Object.assign({}, result, { cached: false }));
  } catch (err) {
    console.error('Conversion rate error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Backfill missing months in Gong Cache sheet (runs once on startup)
async function backfillGongCache() {
  try {
    var existing = await readGongCacheFromSheet();
    // Build list of months from 2025-03 to previous month
    var now = new Date();
    var endY = now.getFullYear(), endM = now.getMonth(); // current month (0-indexed), we want up to prev month
    if (endM === 0) { endY--; endM = 12; } // prev month
    var months = [];
    var y = 2025, m = 3;
    while (y < endY || (y === endY && m <= endM)) {
      var key = y + '-' + String(m).padStart(2, '0');
      if (existing[key] == null) months.push(key);
      m++;
      if (m > 12) { m = 1; y++; }
    }
    if (months.length === 0) { console.log('[Gong Backfill] All months cached, nothing to do.'); return; }
    console.log('[Gong Backfill] Missing months: ' + months.join(', '));
    for (var i = 0; i < months.length; i++) {
      var mk = months[i];
      var parts = mk.split('-');
      var yr = parseInt(parts[0]), mn = parseInt(parts[1]);
      var from = mk + '-01';
      var nextMn = mn === 12 ? 1 : mn + 1;
      var nextY = mn === 12 ? yr + 1 : yr;
      var to = nextY + '-' + String(nextMn).padStart(2, '0') + '-01';
      console.log('[Gong Backfill] Fetching ' + mk + '...');
      try {
        var allCalls = await gongFetchAllPages(
          '/calls?fromDateTime=' + from + 'T00:00:00Z&toDateTime=' + to + 'T00:00:00Z',
          {},
          function(d) { return d.calls || []; }
        );
        var count = 0;
        for (var j = 0; j < allCalls.length; j++) {
          if (DISCOVERY_TITLE_PATTERN.test(allCalls[j].title || '') && (allCalls[j].duration || 0) > MIN_CALL_DURATION) count++;
        }
        await saveGongCountToSheet(mk, count);
        console.log('[Gong Backfill] ' + mk + ' = ' + count + ' discovery calls');
      } catch (e) {
        console.error('[Gong Backfill] Failed for ' + mk + ':', e.message);
      }
    }
    console.log('[Gong Backfill] Done.');
  } catch (e) {
    console.error('[Gong Backfill] Error:', e.message);
  }
}

initOIDC().then(function() {
  app.listen(PORT, function() {
    console.log('FTE Dashboard server running at http://localhost:' + PORT);
    scheduleGChatPosts();
    backfillGongCache();
  });
}).catch(function(err) {
  console.error('Failed to initialise OIDC:', err.message);
  process.exit(1);
});

// ===== Google Ads Spend API =====
var ADS_SHEET_ID = '13vyeLZXZnw4jPjlVS6Q2z2299ZT_76D1ne0jVknh2kc';
var ADS_SHEET_GID = '243603327';
var ADS_CSV_URL = 'https://docs.google.com/spreadsheets/d/' + ADS_SHEET_ID + '/export?format=csv&gid=' + ADS_SHEET_GID;
var adsCache = { data: null, ts: 0 };
var ADS_CACHE_TTL = 5 * 60 * 1000;

function fetchAdsCsv(url) {
  return new Promise(function(resolve, reject) {
    var https = require('https');
    var get = function(u) {
      https.get(u, function(res) {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          get(res.headers.location);
          return;
        }
        if (res.statusCode !== 200) { reject(new Error('HTTP ' + res.statusCode)); return; }
        var body = '';
        res.on('data', function(chunk) { body += chunk; });
        res.on('end', function() { resolve(body); });
        res.on('error', reject);
      }).on('error', reject);
    };
    get(url);
  });
}

function parseAdsCsv(text) {
  var lines = [];
  var current = '';
  var inQuotes = false;
  for (var i = 0; i < text.length; i++) {
    var ch = text[i];
    if (ch === '"') {
      if (inQuotes && text[i + 1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
    } else if (ch === '\n' && !inQuotes) { lines.push(current); current = ''; }
    else if (ch === '\r' && !inQuotes) { /* skip */ }
    else current += ch;
  }
  if (current) lines.push(current);
  var rows = [];
  for (var li = 0; li < lines.length; li++) {
    var line = lines[li];
    if (!line.trim()) continue;
    var fields = []; var field = ''; var q = false;
    for (var j = 0; j < line.length; j++) {
      var c = line[j];
      if (c === '"') { if (q && line[j + 1] === '"') { field += '"'; j++; } else q = !q; }
      else if (c === ',' && !q) { fields.push(field); field = ''; }
      else field += c;
    }
    fields.push(field);
    rows.push(fields);
  }
  return rows;
}

function processAdsData(rows) {
  var campaignHeaderIdx = -1;
  var accountTotalIdx = -1;
  var firstHeaderIdx = -1;
  for (var i = 0; i < rows.length; i++) {
    if (rows[i][0] === 'Day') {
      if (firstHeaderIdx === -1) firstHeaderIdx = i;
      else { campaignHeaderIdx = i; break; }
    }
    if (rows[i][0] === '' && rows[i][1] === 'Total: Account') accountTotalIdx = i;
  }
  var accountTotal = null;
  if (accountTotalIdx >= 0 && firstHeaderIdx >= 0) {
    var aHeaders = rows[firstHeaderIdx]; var aValues = rows[accountTotalIdx];
    accountTotal = {};
    for (var ai = 0; ai < aHeaders.length; ai++) accountTotal[aHeaders[ai]] = aValues[ai] || '';
  }
  var campaigns = [];
  if (campaignHeaderIdx >= 0) {
    var cHeaders = rows[campaignHeaderIdx];
    for (var ci = campaignHeaderIdx + 1; ci < rows.length; ci++) {
      var row = rows[ci];
      if (!row[0] || row[0] === '') continue;
      var obj = {};
      for (var cj = 0; cj < cHeaders.length; cj++) obj[cHeaders[cj]] = row[cj] || '';
      campaigns.push(obj);
    }
  }
  var dailyMap = {};
  for (var di = 0; di < campaigns.length; di++) {
    var camp = campaigns[di];
    var day = camp['Day'];
    if (!day) continue;
    if (!dailyMap[day]) dailyMap[day] = { day: day, clicks: 0, impressions: 0, cost: 0, conversions: 0 };
    dailyMap[day].clicks += parseFloat(camp['Clicks']) || 0;
    dailyMap[day].impressions += parseFloat(camp['Impr.']) || 0;
    dailyMap[day].cost += parseFloat(camp['Cost']) || 0;
    dailyMap[day].conversions += parseFloat(camp['Conversions']) || 0;
  }
  var timeseries = Object.values(dailyMap).sort(function(a, b) { return a.day.localeCompare(b.day); });
  for (var ti = 0; ti < timeseries.length; ti++) {
    var d = timeseries[ti];
    d.ctr = d.impressions > 0 ? (d.clicks / d.impressions) : 0;
    d.avg_cpc = d.clicks > 0 ? (d.cost / d.clicks) : 0;
    d.cost = Math.round(d.cost * 100) / 100;
    d.conversions = Math.round(d.conversions * 100) / 100;
    d.ctr = Math.round(d.ctr * 10000) / 100;
    d.avg_cpc = Math.round(d.avg_cpc * 100) / 100;
  }
  var campaignMap = {};
  for (var si = 0; si < campaigns.length; si++) {
    var c2 = campaigns[si]; var name = c2['Campaign'];
    if (!name) continue;
    if (!campaignMap[name]) campaignMap[name] = { campaign: name, status: c2['Campaign status'], clicks: 0, impressions: 0, cost: 0, conversions: 0 };
    campaignMap[name].clicks += parseFloat(c2['Clicks']) || 0;
    campaignMap[name].impressions += parseFloat(c2['Impr.']) || 0;
    campaignMap[name].cost += parseFloat(c2['Cost']) || 0;
    campaignMap[name].conversions += parseFloat(c2['Conversions']) || 0;
  }
  var campaignSummary = Object.values(campaignMap).map(function(c3) {
    return {
      campaign: c3.campaign, status: c3.status,
      clicks: c3.clicks, impressions: c3.impressions,
      cost: Math.round(c3.cost * 100) / 100, conversions: Math.round(c3.conversions * 100) / 100,
      ctr: c3.impressions > 0 ? Math.round((c3.clicks / c3.impressions) * 10000) / 100 : 0,
      avg_cpc: c3.clicks > 0 ? Math.round((c3.cost / c3.clicks) * 100) / 100 : 0
    };
  }).sort(function(a, b) { return b.cost - a.cost; });
  var totalClicks = 0, totalImpressions = 0, totalCost = 0, totalConversions = 0;
  for (var xi = 0; xi < timeseries.length; xi++) {
    totalClicks += timeseries[xi].clicks;
    totalImpressions += timeseries[xi].impressions;
    totalCost += timeseries[xi].cost;
    totalConversions += timeseries[xi].conversions;
  }
  return {
    summary: {
      total_clicks: totalClicks, total_impressions: totalImpressions,
      total_cost: Math.round(totalCost * 100) / 100,
      total_conversions: Math.round(totalConversions * 100) / 100,
      avg_ctr: totalImpressions > 0 ? Math.round((totalClicks / totalImpressions) * 10000) / 100 : 0,
      avg_cpc: totalClicks > 0 ? Math.round((totalCost / totalClicks) * 100) / 100 : 0,
      cost_per_conversion: totalConversions > 0 ? Math.round((totalCost / totalConversions) * 100) / 100 : 0,
      date_range: { start: timeseries.length ? timeseries[0].day : null, end: timeseries.length ? timeseries[timeseries.length - 1].day : null },
      total_campaigns: campaignSummary.length, total_days: timeseries.length
    },
    campaigns: campaignSummary, timeseries: timeseries, raw_rows: campaigns.length
  };
}

app.get('/api/ads', async function(req, res) {
  try {
    var now = Date.now();
    if (adsCache.data && (now - adsCache.ts) < ADS_CACHE_TTL) {
      return res.json(Object.assign({}, adsCache.data, { cached: true }));
    }
    var csv = await fetchAdsCsv(ADS_CSV_URL);
    var rows = parseAdsCsv(csv);
    var result = processAdsData(rows);
    adsCache = { data: result, ts: now };
    res.json(Object.assign({}, result, { cached: false }));
  } catch (err) {
    console.error('Ads API error:', err.message);
    if (adsCache.data) return res.json(Object.assign({}, adsCache.data, { cached: true, stale: true }));
    res.status(500).json({ error: err.message });
  }
});

// Serve /adspend as a clean URL
app.get('/adspend', function(req, res) {
  res.sendFile(path.join(__dirname, 'adspend.html'));
});
