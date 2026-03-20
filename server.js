require('dotenv').config();
const express = require('express');
const path = require('path');

const app = express();
const PORT = 3000;
const HUBSPOT_KEY = process.env.HUBSPOT_TOKEN;
const HUBSPOT_API = 'https://api.hubapi.com/crm/v3/objects/tickets/search';
const PIPELINES = ['4483329', '3857063', '20565603'];

app.use(express.static(path.join(__dirname)));

async function hubspotSearch(body) {
  // Try Bearer token first, fall back to hapikey query param for legacy apps
  var url = HUBSPOT_API;
  var headers = { 'Content-Type': 'application/json' };

  // Use hapikey for legacy API keys, Bearer for private app tokens
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
    var cutoffDate = new Date(cutoff).toISOString().slice(0, 10);

    // Fetch created tickets (last 120 days, all 3 pipelines)
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

    // Fetch offboard data (offboarding_date >= 120 days ago or future)
    var offboardResults = await fetchAllPages({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'offboarding_date', operator: 'GTE', value: cutoffStr }
        ]
      }],
      properties: ['offboarding_date', 'assignment_type', 'hs_pipeline']
    });

    // Transform to the format the frontend expects
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

    // Timestamp in GMT+8
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
