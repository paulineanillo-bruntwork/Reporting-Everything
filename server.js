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
