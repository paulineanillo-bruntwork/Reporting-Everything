require('dotenv').config();
const express = require('express');
const path = require('path');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;
const HUBSPOT_KEY = process.env.HUBSPOT_TOKEN;
const HUBSPOT_API = 'https://api.hubapi.com/crm/v3/objects/tickets/search';
const PIPELINES = ['4483329', '3857063', '20565603'];
const APP_PASSWORD = process.env.APP_PASSWORD || '1234';

// Parse form data and JSON
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// Simple cookie-based session tokens
var validTokens = {};

function generateToken() {
  return crypto.randomBytes(32).toString('hex');
}

function parseCookies(req) {
  var cookies = {};
  var header = req.headers.cookie || '';
  header.split(';').forEach(function(c) {
    var parts = c.trim().split('=');
    if (parts.length === 2) cookies[parts[0]] = parts[1];
  });
  return cookies;
}

function isAuthenticated(req) {
  var cookies = parseCookies(req);
  var token = cookies.session;
  return token && validTokens[token];
}

// Block search engines
app.get('/robots.txt', function(req, res) {
  res.type('text/plain');
  res.send('User-agent: *\nDisallow: /\n');
});

// Login page
var LOGIN_HTML = '<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="robots" content="noindex, nofollow, noarchive, nosnippet">' +
  '<meta name="viewport" content="width=device-width, initial-scale=1.0">' +
  '<title>FTE Dashboard - Login</title>' +
  '<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f0f2f5;display:flex;justify-content:center;align-items:center;min-height:100vh}' +
  '.login-box{background:#fff;border-radius:12px;padding:40px;box-shadow:0 4px 12px rgba(0,0,0,0.1);width:100%;max-width:400px;text-align:center}' +
  'h1{font-size:22px;margin-bottom:8px;color:#1a1a2e}p{color:#666;font-size:14px;margin-bottom:24px}' +
  'input[type=password]{width:100%;padding:12px 16px;border:2px solid #e5e7eb;border-radius:8px;font-size:16px;outline:none;transition:border-color 0.2s}' +
  'input[type=password]:focus{border-color:#2563eb}' +
  'button{width:100%;padding:12px;background:#2563eb;color:#fff;border:none;border-radius:8px;font-size:16px;font-weight:600;cursor:pointer;margin-top:16px;transition:background 0.2s}' +
  'button:hover{background:#1d4ed8}' +
  '.error{color:#dc2626;font-size:13px;margin-top:12px}</style></head><body>' +
  '<div class="login-box"><h1>FTE Hires Dashboard</h1><p>Enter password to access</p>' +
  '<form method="POST" action="/login"><input type="password" name="password" placeholder="Password" autofocus required>' +
  '<button type="submit">Login</button>' +
  '<ERRORMSG></form></div></body></html>';

app.get('/login', function(req, res) {
  if (isAuthenticated(req)) return res.redirect('/');
  res.send(LOGIN_HTML.replace('<ERRORMSG>', ''));
});

app.post('/login', function(req, res) {
  if (req.body.password === APP_PASSWORD) {
    var token = generateToken();
    validTokens[token] = { created: Date.now() };
    res.setHeader('Set-Cookie', 'session=' + token + '; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400');
    return res.redirect('/');
  }
  res.send(LOGIN_HTML.replace('<ERRORMSG>', '<div class="error">Incorrect password</div>'));
});

app.get('/logout', function(req, res) {
  var cookies = parseCookies(req);
  if (cookies.session) delete validTokens[cookies.session];
  res.setHeader('Set-Cookie', 'session=; Path=/; HttpOnly; Max-Age=0');
  res.redirect('/login');
});

// Auth middleware - protect everything except login and robots.txt
app.use(function(req, res, next) {
  if (req.path === '/login' || req.path === '/robots.txt') return next();
  if (!isAuthenticated(req)) return res.redirect('/login');
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

app.listen(PORT, function() {
  console.log('FTE Dashboard server running at http://localhost:' + PORT);
});
// deployed Mon Mar 23 08:33:54 MPST 2026
