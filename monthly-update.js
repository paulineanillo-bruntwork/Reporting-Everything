/**
 * Monthly Historical Data Update Script
 * Runs on the 1st of each month at 5AM GMT+8
 * Fetches the previous month's final data from HubSpot and appends to HIST_DATA in index.html
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');

const HUBSPOT_TOKEN = process.env.HUBSPOT_TOKEN;
const HUBSPOT_API = 'https://api.hubapi.com/crm/v3/objects/tickets/search';
const PIPELINES = ['4483329', '3857063', '20565603'];
const INDEX_FILE = path.join(__dirname, 'index.html');

// FTE weights
function getFTEWeight(type) {
  if (type === 'Full-Time') return 1;
  if (type === 'Part-Time-Under-20-Hours') return 0.25;
  return 0.5;
}

async function hubspotSearch(body) {
  const res = await fetch(HUBSPOT_API, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + HUBSPOT_TOKEN
    },
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
  while (true) {
    var body = Object.assign({}, baseBody, { limit: 200 });
    if (after) body.after = after;
    var data = await hubspotSearch(body);
    results = results.concat(data.results || []);
    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
    } else {
      break;
    }
  }
  return results;
}

function countByType(tickets, dateField) {
  var counts = { ft: 0, pt: 0, pt20: 0, ob: 0, pb: 0 };
  var fte = 0;
  tickets.forEach(function(t) {
    var type = t.properties.assignment_type || 'Unknown';
    var weight = getFTEWeight(type);
    fte += weight;
    if (type === 'Full-Time') counts.ft++;
    else if (type === 'Part-Time') counts.pt++;
    else if (type === 'Part-Time-Under-20-Hours') counts.pt20++;
    else if (type === 'Output-Based') counts.ob++;
    else if (type === 'Project-Based') counts.pb++;
    else counts.pb++; // Trial and other types go to pb
  });
  var hc = counts.ft + counts.pt + counts.pt20 + counts.ob + counts.pb;
  return { ft: counts.ft, pt: counts.pt, pt20: counts.pt20, ob: counts.ob, pb: counts.pb, hc: hc, fte: fte };
}

async function run() {
  // Determine previous month
  var now = new Date();
  var prevMonth = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 1, 1));
  var year = prevMonth.getUTCFullYear();
  var month = prevMonth.getUTCMonth(); // 0-indexed
  var monthKey = year + '-' + String(month + 1).padStart(2, '0');

  // Month start/end in epoch ms
  var monthStart = new Date(Date.UTC(year, month, 1)).getTime();
  var monthEnd = new Date(Date.UTC(year, month + 1, 1)).getTime(); // first ms of next month

  console.log('Fetching data for month:', monthKey);
  console.log('Range:', new Date(monthStart).toISOString(), 'to', new Date(monthEnd).toISOString());

  // Check if this month already exists in HIST_DATA
  var html = fs.readFileSync(INDEX_FILE, 'utf8');
  if (html.indexOf('"' + monthKey + '"') !== -1) {
    console.log('Month ' + monthKey + ' already exists in HIST_DATA. Skipping.');
    return;
  }

  // Fetch HIRES: tickets created in prev month across all 3 pipelines
  console.log('Fetching hires (created tickets)...');
  var hires = await fetchAllPages({
    filterGroups: [{
      filters: [
        { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
        { propertyName: 'createdate', operator: 'GTE', value: String(monthStart) },
        { propertyName: 'createdate', operator: 'LT', value: String(monthEnd) }
      ]
    }],
    properties: ['createdate', 'assignment_type', 'hs_pipeline']
  });
  console.log('  Found', hires.length, 'hire tickets');

  // Fetch CHURN: tickets with offboarding_date in prev month
  console.log('Fetching churn (offboarded tickets)...');
  var churnStart = new Date(Date.UTC(year, month, 1)).toISOString().slice(0, 10);
  var churnEnd = new Date(Date.UTC(year, month + 1, 1)).toISOString().slice(0, 10);

  // HubSpot date properties need epoch ms for GTE/LT
  var churn = await fetchAllPages({
    filterGroups: [{
      filters: [
        { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
        { propertyName: 'offboarding_date', operator: 'GTE', value: String(monthStart) },
        { propertyName: 'offboarding_date', operator: 'LT', value: String(monthEnd) }
      ]
    }],
    properties: ['offboarding_date', 'assignment_type', 'hs_pipeline']
  });
  console.log('  Found', churn.length, 'churn tickets');

  // Calculate counts
  var h = countByType(hires);
  var c = countByType(churn);
  var net = Math.round((h.fte - c.fte) * 100) / 100;

  console.log('Hires:', JSON.stringify(h));
  console.log('Churn:', JSON.stringify(c));
  console.log('Net FTE:', net);

  // Build the new HIST_DATA line
  var newEntry = '  {m:"' + monthKey + '",' +
    'h:{ft:' + h.ft + ',pt:' + h.pt + ',pt20:' + h.pt20 + ',ob:' + h.ob + ',pb:' + h.pb + ',hc:' + h.hc + ',fte:' + h.fte + '},' +
    'c:{ft:' + c.ft + ',pt:' + c.pt + ',pt20:' + c.pt20 + ',ob:' + c.ob + ',pb:' + c.pb + ',hc:' + c.hc + ',fte:' + c.fte + '},' +
    'net:' + net + '}';

  console.log('New entry:', newEntry);

  // Insert into HIST_DATA array (before the closing ];)
  // Find the last entry line and append after it
  var lastEntryPattern = /(\{m:"2026-\d{2}"[^}]+\}[^}]+\}[^}]+\},[^\n]*net:[^\n]+\})\s*\n\];/;
  // More robust: find the "];" that closes HIST_DATA
  var histDataEnd = html.indexOf('];\n\nvar histRendered');
  if (histDataEnd === -1) {
    histDataEnd = html.indexOf('];\r\nvar histRendered');
  }
  if (histDataEnd === -1) {
    // Try finding the pattern more loosely
    var match = html.match(/net:\d+\.?\d*\}\s*\n\];/);
    if (match) {
      histDataEnd = html.indexOf(match[0]) + match[0].indexOf('\n];') + 1;
    }
  }

  if (histDataEnd === -1) {
    console.error('Could not find HIST_DATA closing bracket in index.html');
    process.exit(1);
  }

  // Find the last entry before ];
  var beforeEnd = html.substring(0, histDataEnd);
  var lastNewline = beforeEnd.lastIndexOf('\n');
  var lastLine = beforeEnd.substring(lastNewline + 1).trim();

  // If last line doesn't end with comma, we need to add one
  var updatedHtml;
  if (lastLine.endsWith('}')) {
    // Add comma to last line, then add new entry
    updatedHtml = html.substring(0, histDataEnd).replace(/(\}\s*)$/, '},\n') + newEntry + '\n' + html.substring(histDataEnd);
  } else {
    // Last line already has comma
    updatedHtml = html.substring(0, histDataEnd) + newEntry + ',\n' + html.substring(histDataEnd);
  }

  // Wait, simpler approach: just replace the last entry line to add comma if needed, then insert
  // Actually, looking at the data, the last line does NOT have a trailing comma
  // Let's just find the exact position

  // Find "net:110.5}" (last entry) followed by newline and "];"
  var closingMatch = html.match(/(net:[\d.]+\})\s*\n\]/);
  if (!closingMatch) {
    console.error('Could not find HIST_DATA closing pattern');
    process.exit(1);
  }

  var insertPoint = html.indexOf(closingMatch[0]);
  var replacement = closingMatch[1] + ',\n' + newEntry + '\n]';
  updatedHtml = html.substring(0, insertPoint) + replacement + html.substring(insertPoint + closingMatch[0].length);

  fs.writeFileSync(INDEX_FILE, updatedHtml, 'utf8');
  console.log('\nSuccessfully added ' + monthKey + ' to HIST_DATA!');

  // Log timestamp
  var ts = new Date(Date.now() + 8 * 60 * 60 * 1000);
  console.log('Updated at:', ts.toISOString().replace('T', ' ').substring(0, 19), '(GMT+8)');
}

run().catch(function(err) {
  console.error('Error:', err.message);
  process.exit(1);
});
