require('dotenv').config();
const express = require('express');
const path = require('path');
const crypto = require('crypto');
const session = require('express-session');
const { Issuer, generators } = require('openid-client');

const app = express();
const PORT = process.env.PORT || 3000;
const HUBSPOT_KEY = process.env.HUBSPOT_TOKEN;
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
  cookie: {
    httpOnly: true,
    sameSite: 'lax',
    secure: APP_URL.startsWith('https'),
    maxAge: 8 * 60 * 60 * 1000 // 8 hours
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
    req.session.user = {
      sub: claims.sub,
      name: claims.name || claims.preferred_username,
      email: claims.email
    };
    req.session.idToken = tokenSet.id_token;
    delete req.session.oidcState;
    delete req.session.oidcNonce;
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
var PUBLIC_PATHS = ['/auth/login', '/auth/callback', '/login', '/robots.txt'];
app.use(function(req, res, next) {
  if (PUBLIC_PATHS.indexOf(req.path) !== -1) return next();
  if (!req.session.user) {
    if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Unauthorized' });
    return res.redirect('/auth/login');
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

async function fetchAllPages(baseBody) {
  var results = [];
  var after = undefined;
  var hasMore = true;
  while (hasMore) {
    var body = Object.assign({}, baseBody, { limit: 200 });
    if (after) body.after = after;
    var data = await hubspotSearch(body);
    results = results.concat(data.results || []);
    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
    } else {
      hasMore = false;
    }
  }
  return results;
}

app.get('/api/tickets', async function(req, res) {
  try {
    var cutoff = Date.now() - (120 * 24 * 60 * 60 * 1000);
    var cutoffStr = String(cutoff);

    var createdResults = await fetchAllPages({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'createdate', operator: 'GTE', value: cutoffStr }
        ]
      }],
      properties: ['createdate', 'assignment_type', 'hs_pipeline'],
      sorts: [{ propertyName: 'createdate', direction: 'DESCENDING' }]
    });

    var offboardResults = await fetchAllPages({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'offboarding_date', operator: 'GTE', value: cutoffStr }
        ]
      }],
      properties: ['offboarding_date', 'assignment_type', 'hs_pipeline']
    });

    var raw = createdResults.map(function(r) {
      return {
        d: r.properties.createdate,
        t: r.properties.assignment_type || 'Unknown',
        p: r.properties.hs_pipeline
      };
    });

    var offboard = offboardResults.map(function(r) {
      return {
        o: r.properties.offboarding_date,
        t: r.properties.assignment_type || 'Unknown'
      };
    });

    var now = new Date();
    var pht = new Date(now.getTime() + (8 * 60 * 60 * 1000));
    var months = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    var h = pht.getUTCHours();
    var ampm = h >= 12 ? 'PM' : 'AM';
    var h12 = h % 12 || 12;
    var timestamp = months[pht.getUTCMonth()] + ' ' + pht.getUTCDate() + ', ' + pht.getUTCFullYear() + ' ' + h12 + ':' + String(pht.getUTCMinutes()).padStart(2, '0') + ' ' + ampm + ' (GMT+8)';

    res.json({
      raw: raw,
      offboard: offboard,
      timestamp: timestamp,
      counts: { created: raw.length, offboarded: offboard.length }
    });
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

async function sheetsGet(spreadsheetId, range) {
  var token = await getGoogleAccessToken();
  var url = 'https://sheets.googleapis.com/v4/spreadsheets/' + encodeURIComponent(spreadsheetId)
    + '/values/' + encodeURIComponent(range);
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

    console.log('[Report] Generating automated data for ' + month);
    var parts = month.split('-');
    var yr = parseInt(parts[0]);
    var mo = parseInt(parts[1]);
    // Month start/end in YYYY-MM-DD format
    var monthStart = month + '-01';
    var lastDay = new Date(yr, mo, 0).getDate();
    var monthEnd = month + '-' + String(lastDay).padStart(2, '0');
    // Timestamps for HubSpot filters (milliseconds)
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
          { propertyName: 'onboarding_date', operator: 'GTE', value: monthStart },
          { propertyName: 'onboarding_date', operator: 'LTE', value: monthEnd }
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
          { propertyName: 'offboarding_date', operator: 'GTE', value: monthStart },
          { propertyName: 'offboarding_date', operator: 'LTE', value: monthEnd }
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
            { propertyName: 'onboarding_date', operator: 'LTE', value: monthEnd },
            { propertyName: 'offboarding_date', operator: 'NOT_HAS_PROPERTY' }
          ]
        },
        {
          filters: [
            { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
            { propertyName: 'onboarding_date', operator: 'LTE', value: monthEnd },
            { propertyName: 'offboarding_date', operator: 'GT', value: monthEnd }
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

// Serve report pages
app.get('/reports', function(req, res) {
  res.sendFile(path.join(__dirname, 'reports.html'));
});
app.get('/report', function(req, res) {
  res.sendFile(path.join(__dirname, 'report.html'));
});

initOIDC().then(function() {
  app.listen(PORT, function() {
    console.log('FTE Dashboard server running at http://localhost:' + PORT);
    scheduleGChatPosts();
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
