/**
 * Fetches Running Update data directly from HubSpot API and posts a
 * formatted text card to Google Chat.
 *
 * Env vars required:
 *   HUBSPOT_TOKEN  - HubSpot API key (hapikey or pat- token)
 *   GCHAT_WEBHOOK  - Google Chat space webhook URL
 *   DASHBOARD_URL  - (optional) link to dashboard for the button
 */

var https = require('https');

var HUBSPOT_TOKEN = process.env.HUBSPOT_TOKEN;
var GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;
var DASHBOARD_URL = process.env.DASHBOARD_URL || 'https://fte-dashboard-production.up.railway.app';
var HUBSPOT_API = 'https://api.hubapi.com/crm/v3/objects/tickets/search';
var PIPELINES = ['4483329', '3857063', '20565603'];

if (!GCHAT_WEBHOOK) {
  console.error('ERROR: GCHAT_WEBHOOK env var is required');
  process.exit(1);
}
if (!HUBSPOT_TOKEN) {
  console.error('ERROR: HUBSPOT_TOKEN env var is required');
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

function hubspotFetch(body) {
  return new Promise(function(resolve, reject) {
    var url = new URL(HUBSPOT_API);
    var headers = { 'Content-Type': 'application/json' };

    if (HUBSPOT_TOKEN.startsWith('pat-')) {
      headers['Authorization'] = 'Bearer ' + HUBSPOT_TOKEN;
    } else {
      url.searchParams.set('hapikey', HUBSPOT_TOKEN);
    }

    var bodyStr = JSON.stringify(body);
    headers['Content-Length'] = Buffer.byteLength(bodyStr);

    var req = https.request({
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: 'POST',
      headers: headers
    }, function(res) {
      var data = '';
      res.on('data', function(chunk) { data += chunk; });
      res.on('end', function() {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(JSON.parse(data));
        } else {
          reject(new Error('HubSpot API error ' + res.statusCode + ': ' + data.substring(0, 300)));
        }
      });
    });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
}

async function fetchAllPages(baseBody) {
  var results = [];
  var after = undefined;
  var hasMore = true;
  while (hasMore) {
    var body = Object.assign({}, baseBody, { limit: 200 });
    if (after) body.after = after;
    var data = await hubspotFetch(body);
    results = results.concat(data.results || []);
    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
    } else {
      hasMore = false;
    }
  }
  return results;
}

function buildMonthSummary(raw, offboard, monthKey) {
  var types = ['Full-Time', 'Part-Time', 'Part-Time-Under-20-Hours', 'Output-Based', 'Project-Based', 'Trial'];
  var counts = { hire: {}, churn: {} };
  types.forEach(function(t) { counts.hire[t] = 0; counts.churn[t] = 0; });
  counts.hire._total = 0; counts.churn._total = 0;
  counts.hire._fte = 0; counts.churn._fte = 0;

  raw.forEach(function(r) {
    var k = getMonthKey(r.properties.createdate);
    if (k !== monthKey) return;
    var type = r.properties.assignment_type || 'Unknown';
    if (counts.hire[type] === undefined) counts.hire[type] = 0;
    counts.hire[type]++;
    counts.hire._total++;
    counts.hire._fte += getFTEWeight(type);
  });

  offboard.forEach(function(r) {
    var oDate = r.properties.offboarding_date;
    if (!oDate) return;
    var k = getMonthKey(oDate + 'T00:00:00Z');
    if (k !== monthKey) return;
    var type = r.properties.assignment_type || 'Unknown';
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

    // Fetch data directly from HubSpot
    var cutoff = Date.now() - (120 * 24 * 60 * 60 * 1000);
    var cutoffStr = String(cutoff);

    console.log('Fetching created tickets from HubSpot...');
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

    console.log('Fetching offboarded tickets from HubSpot...');
    var offboardResults = await fetchAllPages({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'offboarding_date', operator: 'GTE', value: cutoffStr }
        ]
      }],
      properties: ['offboarding_date', 'assignment_type', 'hs_pipeline']
    });

    console.log('Fetched: ' + createdResults.length + ' created, ' + offboardResults.length + ' offboarded');

    // Build current month summary
    var now = new Date();
    var gmt8 = new Date(now.getTime() + (8 * 60 * 60 * 1000));
    var curMonthKey = gmt8.getUTCFullYear() + '-' + String(gmt8.getUTCMonth() + 1).padStart(2, '0');
    var monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    var monthLabel = monthNames[gmt8.getUTCMonth()] + ' ' + gmt8.getUTCFullYear();

    var c = buildMonthSummary(createdResults, offboardResults, curMonthKey);
    var netFTE = Math.round((c.hire._fte - c.churn._fte) * 100) / 100;

    // Format time
    var h = gmt8.getUTCHours();
    var ampm = h >= 12 ? 'PM' : 'AM';
    var h12 = h % 12 || 12;
    var timeStr = monthNames[gmt8.getUTCMonth()] + ' ' + gmt8.getUTCDate() + ', ' + gmt8.getUTCFullYear() + ' ' + h12 + ':' + String(gmt8.getUTCMinutes()).padStart(2, '0') + ' ' + ampm + ' (GMT+8)';

    // Build contract type breakdown
    var types = ['Full-Time', 'Part-Time', 'Part-Time-Under-20-Hours', 'Output-Based', 'Project-Based', 'Trial'];
    var typeLabels = {
      'Full-Time': 'Full Time',
      'Part-Time': 'Part Time',
      'Part-Time-Under-20-Hours': 'PT Under 20hrs',
      'Output-Based': 'Output-Based',
      'Project-Based': 'Project-Based',
      'Trial': 'Trial'
    };

    // Net FTE color indicator
    var netPrefix = netFTE >= 0 ? '+' : '';
    var netEmoji = netFTE > 0 ? '\u{1F4C8}' : (netFTE < 0 ? '\u{1F4C9}' : '\u{2796}');

    // Build Google Chat card (Cards V2 format)
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
                {
                  decoratedText: {
                    topLabel: 'NEW HIRES (Headcount \u2192 FTE)',
                    text: '<b>' + c.hire._total + ' HC  \u2192  ' + fmtFTE(Math.round(c.hire._fte * 100) / 100) + ' FTE</b>',
                    startIcon: { knownIcon: 'PERSON' }
                  }
                },
                {
                  decoratedText: {
                    topLabel: 'CHURN (Headcount \u2192 FTE)',
                    text: '<b>' + c.churn._total + ' HC  \u2192  ' + fmtFTE(Math.round(c.churn._fte * 100) / 100) + ' FTE</b>',
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
