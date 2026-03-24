/**
 * Fetches Running Update data from the dashboard API and posts a
 * formatted text card to Google Chat. No screenshot/Puppeteer needed.
 *
 * Env vars required:
 *   DASHBOARD_URL  - e.g. https://fte-dashboard-production.up.railway.app
 *   APP_PASSWORD   - dashboard login password
 *   GCHAT_WEBHOOK  - Google Chat space webhook URL
 */

var https = require('https');
var http = require('http');

var DASHBOARD_URL = process.env.DASHBOARD_URL || 'https://fte-dashboard-production.up.railway.app';
var APP_PASSWORD = process.env.APP_PASSWORD || '1234';
var GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

if (!GCHAT_WEBHOOK) {
  console.error('ERROR: GCHAT_WEBHOOK env var is required');
  process.exit(1);
}

// FTE weight logic (must match app.js)
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

function httpRequest(url, options, body) {
  return new Promise(function(resolve, reject) {
    var urlObj = new URL(url);
    var mod = urlObj.protocol === 'https:' ? https : http;
    var opts = Object.assign({
      hostname: urlObj.hostname,
      port: urlObj.port,
      path: urlObj.pathname + urlObj.search,
      method: 'GET'
    }, options);

    var req = mod.request(opts, function(res) {
      var data = '';
      res.on('data', function(chunk) { data += chunk; });
      res.on('end', function() { resolve({ status: res.statusCode, body: data, headers: res.headers }); });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function login() {
  // POST login to get session cookie
  var loginBody = 'password=' + encodeURIComponent(APP_PASSWORD);
  var res = await httpRequest(DASHBOARD_URL + '/login', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(loginBody)
    }
  }, loginBody);

  // Extract session cookie from Set-Cookie header (302 redirect means success)
  var cookies = res.headers['set-cookie'];
  if (!cookies) throw new Error('No session cookie received. Login may have failed (status ' + res.status + ')');
  var sessionCookie = '';
  (Array.isArray(cookies) ? cookies : [cookies]).forEach(function(c) {
    var match = c.match(/session=([^;]+)/);
    if (match) sessionCookie = 'session=' + match[1];
  });
  if (!sessionCookie) throw new Error('Session cookie not found in response');
  console.log('Logged in successfully');
  return sessionCookie;
}

async function fetchTicketData(cookie) {
  var res = await httpRequest(DASHBOARD_URL + '/api/tickets', {
    headers: { 'Cookie': cookie }
  });
  if (res.status !== 200) throw new Error('API returned ' + res.status + ': ' + res.body.substring(0, 200));
  return JSON.parse(res.body);
}

function buildMonthSummary(data, monthKey) {
  var types = ['Full-Time', 'Part-Time', 'Part-Time-Under-20-Hours', 'Output-Based', 'Project-Based', 'Trial'];
  var counts = { hire: {}, churn: {} };
  types.forEach(function(t) { counts.hire[t] = 0; counts.churn[t] = 0; });
  counts.hire._total = 0; counts.churn._total = 0;
  counts.hire._fte = 0; counts.churn._fte = 0;

  data.raw.forEach(function(r) {
    var k = getMonthKey(r.d);
    if (k !== monthKey) return;
    var type = r.t || 'Unknown';
    if (counts.hire[type] === undefined) counts.hire[type] = 0;
    counts.hire[type]++;
    counts.hire._total++;
    counts.hire._fte += getFTEWeight(type);
  });

  data.offboard.forEach(function(r) {
    var k = getMonthKey(r.o + 'T00:00:00Z');
    if (k !== monthKey) return;
    var type = r.t || 'Unknown';
    if (counts.churn[type] === undefined) counts.churn[type] = 0;
    counts.churn[type]++;
    counts.churn._total++;
    counts.churn._fte += getFTEWeight(type);
  });

  return counts;
}

function postToGoogleChat(message) {
  return new Promise(function(resolve, reject) {
    var body = JSON.stringify(message);
    var urlObj = new URL(GCHAT_WEBHOOK);
    var req = https.request({
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
    }, function(res) {
      var data = '';
      res.on('data', function(chunk) { data += chunk; });
      res.on('end', function() {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          console.log('Posted to Google Chat successfully!');
          resolve(data);
        } else {
          reject(new Error('Google Chat error ' + res.statusCode + ': ' + data));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function main() {
  try {
    console.log('=== FTE Dashboard → Google Chat ===');
    console.log('Dashboard: ' + DASHBOARD_URL);

    // Login and fetch data
    var cookie = await login();
    var data = await fetchTicketData(cookie);
    console.log('Fetched: ' + data.counts.created + ' created, ' + data.counts.offboarded + ' offboarded');

    // Build current month summary
    var now = new Date();
    var gmt8 = new Date(now.getTime() + (8 * 60 * 60 * 1000));
    var curMonthKey = gmt8.getUTCFullYear() + '-' + String(gmt8.getUTCMonth() + 1).padStart(2, '0');
    var monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    var monthLabel = monthNames[gmt8.getUTCMonth()] + ' ' + gmt8.getUTCFullYear();

    var c = buildMonthSummary(data, curMonthKey);
    var netFTE = Math.round((c.hire._fte - c.churn._fte) * 100) / 100;

    // Format time
    var h = gmt8.getUTCHours();
    var ampm = h >= 12 ? 'PM' : 'AM';
    var h12 = h % 12 || 12;
    var timeStr = monthNames[gmt8.getUTCMonth()] + ' ' + gmt8.getUTCDate() + ', ' + gmt8.getUTCFullYear() + ' ' + h12 + ':' + String(gmt8.getUTCMinutes()).padStart(2, '0') + ' ' + ampm + ' (GMT+8)';

    // Build contract type breakdown lines
    var types = ['Full-Time', 'Part-Time', 'Part-Time-Under-20-Hours', 'Output-Based', 'Project-Based', 'Trial'];
    var typeLabels = {
      'Full-Time': 'Full Time',
      'Part-Time': 'Part Time',
      'Part-Time-Under-20-Hours': 'PT Under 20hrs',
      'Output-Based': 'Output-Based',
      'Project-Based': 'Project-Based',
      'Trial': 'Trial'
    };

    var breakdownLines = [];
    types.forEach(function(t) {
      var hc = c.hire[t] || 0;
      var ch = c.churn[t] || 0;
      if (hc === 0 && ch === 0) return;
      var label = typeLabels[t] || t;
      breakdownLines.push('    ' + label + ':  ' + hc + '  |  ' + ch);
    });

    // Net FTE color indicator
    var netPrefix = netFTE >= 0 ? '+' : '';
    var netEmoji = netFTE > 0 ? '📈' : (netFTE < 0 ? '📉' : '➖');

    // Build Google Chat card (Cards V2 format for better formatting)
    var message = {
      cardsV2: [{
        cardId: 'fte-update',
        card: {
          header: {
            title: 'FTE Running Update',
            subtitle: 'Current Month — ' + monthLabel,
            imageUrl: 'https://fonts.gstatic.com/s/i/short-term/release/googlesymbols/analytics/default/48px.svg',
            imageType: 'CIRCLE'
          },
          sections: [
            {
              header: '<b>SUMMARY</b>',
              widgets: [
                {
                  decoratedText: {
                    topLabel: 'NEW HIRES (Headcount → FTE)',
                    text: '<b>' + c.hire._total + ' HC  →  ' + fmtFTE(Math.round(c.hire._fte * 100) / 100) + ' FTE</b>',
                    startIcon: { knownIcon: 'PERSON' }
                  }
                },
                {
                  decoratedText: {
                    topLabel: 'CHURN (Headcount → FTE)',
                    text: '<b>' + c.churn._total + ' HC  →  ' + fmtFTE(Math.round(c.churn._fte * 100) / 100) + ' FTE</b>',
                    startIcon: { knownIcon: 'MEMBERSHIP' }
                  }
                },
                {
                  decoratedText: {
                    topLabel: 'NET FTE ' + netEmoji,
                    text: '<b><font color="' + (netFTE >= 0 ? '#16a34a' : '#dc2626') + '">' + netPrefix + fmtFTE(netFTE) + '</font></b>',
                    startIcon: { knownIcon: 'BOOKMARK' }
                  }
                }
              ]
            },
            {
              header: '<b>BY CONTRACT TYPE</b>  (Hires | Churn)',
              widgets: (function() {
                var widgets = [];
                types.forEach(function(t) {
                  var hc = c.hire[t] || 0;
                  var ch = c.churn[t] || 0;
                  if (hc === 0 && ch === 0) return;
                  widgets.push({
                    decoratedText: {
                      topLabel: (typeLabels[t] || t).toUpperCase(),
                      text: '<b>' + hc + '</b>  |  <b>' + ch + '</b>'
                    }
                  });
                });
                if (widgets.length === 0) {
                  widgets.push({ decoratedText: { text: 'No data for this month yet' } });
                }
                return widgets;
              })()
            },
            {
              widgets: [
                {
                  decoratedText: {
                    topLabel: 'Data as of',
                    text: timeStr
                  }
                },
                {
                  buttonList: {
                    buttons: [{
                      text: 'OPEN DASHBOARD',
                      onClick: { openLink: { url: DASHBOARD_URL } },
                      color: { red: 0.15, green: 0.39, blue: 0.92, alpha: 1 }
                    }]
                  }
                }
              ]
            }
          ]
        }
      }]
    };

    await postToGoogleChat(message);
    console.log('Done!');
    process.exit(0);
  } catch (err) {
    console.error('FAILED:', err.message);
    process.exit(1);
  }
}

main();
