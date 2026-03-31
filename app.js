
function getFTEWeight(contractType) {
  if (contractType === "Full-Time") return 1;
  if (contractType === "Part-Time-Under-20-Hours") return 0.25;
  return 0.5;
}

function getSundayOfWeek(dateStr) {
  var d = new Date(dateStr);
  var day = d.getUTCDay();
  d.setUTCDate(d.getUTCDate() - day);
  d.setUTCHours(0, 0, 0, 0);
  return d.toISOString().slice(0, 10);
}

function getMonthKey(dateStr) {
  var d = new Date(dateStr);
  return d.getUTCFullYear() + '-' + String(d.getUTCMonth() + 1).padStart(2, '0');
}

function formatWeek(dateStr) {
  var d = new Date(dateStr + 'T00:00:00Z');
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
}

function formatMonth(monthKey) {
  var parts = monthKey.split('-');
  var d = new Date(Date.UTC(parseInt(parts[0]), parseInt(parts[1]) - 1, 1));
  return d.toLocaleDateString('en-US', { month: 'long', year: 'numeric', timeZone: 'UTC' });
}

var CONTRACT_TYPES = ['Full-Time', 'Part-Time', 'Part-Time-Under-20-Hours', 'Other'];

function classifyContract(t) {
  if (t === 'Full-Time' || t === 'Part-Time' || t === 'Part-Time-Under-20-Hours') return t;
  return 'Other';
}

function emptyBucket() {
  return {
    created: 0, offboarded: 0,
    c_ft: 0, c_pt: 0, c_pt20: 0, c_other: 0,
    o_ft: 0, o_pt: 0, o_pt20: 0, o_other: 0,
    c_count: 0, o_count: 0
  };
}

function buildGroupedData(numPeriods, groupBy) {
  var buckets = {};

  RAW_DATA.forEach(function(r) {
    var key = groupBy === 'month' ? getMonthKey(r.d) : getSundayOfWeek(r.d);
    if (!buckets[key]) buckets[key] = emptyBucket();
    var w = getFTEWeight(r.t);
    buckets[key].created += w;
    buckets[key].c_count++;
    var ct = classifyContract(r.t);
    if (ct === 'Full-Time') buckets[key].c_ft += w;
    else if (ct === 'Part-Time') buckets[key].c_pt += w;
    else if (ct === 'Part-Time-Under-20-Hours') buckets[key].c_pt20 += w;
    else buckets[key].c_other += w;
  });

  OFFBOARD_DATA.forEach(function(r) {
    var key = groupBy === 'month' ? getMonthKey(r.o + 'T00:00:00Z') : getSundayOfWeek(r.o + 'T00:00:00Z');
    if (!buckets[key]) buckets[key] = emptyBucket();
    var w = getFTEWeight(r.t);
    buckets[key].offboarded += w;
    buckets[key].o_count++;
    var ct = classifyContract(r.t);
    if (ct === 'Full-Time') buckets[key].o_ft += w;
    else if (ct === 'Part-Time') buckets[key].o_pt += w;
    else if (ct === 'Part-Time-Under-20-Hours') buckets[key].o_pt20 += w;
    else buckets[key].o_other += w;
  });

  var sortedKeys = Object.keys(buckets).sort().reverse();
  if (numPeriods > 0) sortedKeys = sortedKeys.slice(0, numPeriods);
  sortedKeys.reverse();

  var r2 = function(v) { return Math.round(v * 100) / 100; };
  return sortedKeys.map(function(k) {
    var b = buckets[k];
    return {
      key: k,
      label: groupBy === 'month' ? formatMonth(k) : formatWeek(k),
      created: r2(b.created), offboarded: r2(b.offboarded),
      net: r2(b.created - b.offboarded),
      c_ft: r2(b.c_ft), c_pt: r2(b.c_pt), c_pt20: r2(b.c_pt20), c_other: r2(b.c_other),
      o_ft: r2(b.o_ft), o_pt: r2(b.o_pt), o_pt20: r2(b.o_pt20), o_other: r2(b.o_other),
      c_count: b.c_count, o_count: b.o_count
    };
  });
}

function fmtFTE(val) {
  if (val % 1 === 0) return val.toFixed(0);
  if ((val * 4) % 1 === 0 && val % 0.5 !== 0) return val.toFixed(2);
  return val.toFixed(1);
}

function buildMonthSummary(monthKey) {
  var counts = { hire: {}, churn: {} };
  ['Full-Time','Part-Time','Part-Time-Under-20-Hours','Output-Based','Project-Based','Trial'].forEach(function(t) {
    counts.hire[t] = 0; counts.churn[t] = 0;
  });
  counts.hire._total = 0; counts.churn._total = 0;
  counts.hire._fte = 0; counts.churn._fte = 0;

  RAW_DATA.forEach(function(r) {
    var k = getMonthKey(r.d);
    if (k !== monthKey) return;
    var type = r.t || 'Unknown';
    if (counts.hire[type] === undefined) counts.hire[type] = 0;
    counts.hire[type]++;
    counts.hire._total++;
    counts.hire._fte += getFTEWeight(type);
  });

  OFFBOARD_DATA.forEach(function(r) {
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

function renderRunningUpdate() {
  var sel = document.getElementById('ruMonth');
  var monthKey = sel.value;
  var c = buildMonthSummary(monthKey);

  var types = ['Full-Time','Part-Time','Part-Time-Under-20-Hours','Output-Based','Project-Based','Trial'];
  var typeLabels = {'Full-Time':'Full Time','Part-Time':'Part Time','Part-Time-Under-20-Hours':'Part Time under 20 hrs','Output-Based':'Output-Based','Project-Based':'Project-Based','Trial':'Trial'};

  var netFTE = Math.round((c.hire._fte - c.churn._fte) * 100) / 100;

  var html = '<thead><tr><th></th><th class="ru-header-hire">New Hires</th><th class="ru-header-churn">Churn</th></tr></thead><tbody>';
  html += '<tr class="ru-hc-row"><td><strong>Headcount</strong></td><td><strong>' + c.hire._total + '</strong></td><td><strong>' + c.churn._total + '</strong></td></tr>';

  types.forEach(function(t) {
    var h = c.hire[t] || 0;
    var ch = c.churn[t] || 0;
    if (h === 0 && ch === 0) return;
    html += '<tr><td style="padding-left:24px">' + (typeLabels[t] || t) + '</td><td>' + (h || '') + '</td><td>' + (ch || '') + '</td></tr>';
  });

  html += '<tr class="ru-fte-row"><td><strong>FTE Equivalent</strong></td><td><strong>' + fmtFTE(Math.round(c.hire._fte * 100) / 100) + '</strong></td><td><strong>' + fmtFTE(Math.round(c.churn._fte * 100) / 100) + '</strong></td></tr>';
  html += '<tr class="ru-net-row"><td><strong>Net FTE</strong></td><td colspan="2" style="text-align:center"><strong>' + fmtFTE(netFTE) + '</strong></td></tr>';
  html += '</tbody>';

  document.getElementById('ruTable').innerHTML = html;
}

// Populate month dropdown (using GMT+8)
(function initRUDropdown() {
  var now = new Date();
  var gmt8 = new Date(now.getTime() + (8 * 60 * 60 * 1000));
  var curMonth = gmt8.getUTCFullYear() + '-' + String(gmt8.getUTCMonth() + 1).padStart(2, '0');
  var prev = new Date(Date.UTC(gmt8.getUTCFullYear(), gmt8.getUTCMonth() - 1, 1));
  var prevMonth = prev.getUTCFullYear() + '-' + String(prev.getUTCMonth() + 1).padStart(2, '0');

  var sel = document.getElementById('ruMonth');
  var curLabel = formatMonth(curMonth);
  var prevLabel = formatMonth(prevMonth);
  var monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  var curParts = curMonth.split('-');
  var prevParts = prevMonth.split('-');
  var curDisplay = curParts[0] + ' ' + monthNames[parseInt(curParts[1]) - 1];
  var prevDisplay = prevParts[0] + ' ' + monthNames[parseInt(prevParts[1]) - 1];
  sel.innerHTML = '<option value="' + curMonth + '">Current Month - ' + curDisplay + '</option><option value="' + prevMonth + '">Previous Month - ' + prevDisplay + '</option>';
  sel.addEventListener('change', renderRunningUpdate);
})();

var fteChart, netChart;

function render() {
  var numPeriods = parseInt(document.getElementById('periodCount').value);
  var groupBy = document.getElementById('groupBy').value;
  var data = buildGroupedData(numPeriods, groupBy);

  var periodLabel = groupBy === 'month' ? 'Monthly' : 'Weekly';
  document.getElementById('chartTitle').textContent = periodLabel + ' FTE - Created vs Offboarded';
  document.getElementById('netChartTitle').textContent = periodLabel + ' Net FTE (Created - Offboarded)';
  document.getElementById('periodHeader').textContent = groupBy === 'month' ? 'Month' : 'Week Starting';

  var today = new Date();
  var currentKey = groupBy === 'month' ? getMonthKey(today.toISOString()) : getSundayOfWeek(today.toISOString());

  var totalCreated = data.reduce(function(s, d) { return s + d.created; }, 0);
  var totalOffboarded = data.reduce(function(s, d) { return s + d.offboarded; }, 0);
  var totalNet = data.reduce(function(s, d) { return s + d.net; }, 0);

  document.getElementById('summaryCards').innerHTML =
    '<div class="card onboard"><div class="label">Created (by create date)</div><div class="value">' + fmtFTE(totalCreated) + '</div></div>' +
    '<div class="card offboard"><div class="label">Offboarded (by offboarding date)</div><div class="value">' + fmtFTE(totalOffboarded) + '</div></div>' +
    '<div class="card net"><div class="label">Net FTE</div><div class="value">' + (totalNet >= 0 ? '+' : '') + fmtFTE(totalNet) + '</div></div>';

  var tbody = document.getElementById('tableBody');
  tbody.innerHTML = data.map(function(d) {
    return '<tr class="' + (d.key === currentKey ? 'week-current' : '') + '">' +
      '<td>' + d.label + (d.key === currentKey ? ' (current)' : '') + '</td>' +
      '<td><span class="tag tag-onboard">' + fmtFTE(d.created) + '</span></td>' +
      '<td><span class="tag tag-offboard">' + fmtFTE(d.offboarded) + '</span></td>' +
      '<td style="color:' + (d.net >= 0 ? '#059669' : '#dc2626') + '; font-weight:600">' + (d.net >= 0 ? '+' : '') + fmtFTE(d.net) + '</td></tr>';
  }).join('');

  var labels = data.map(function(d) { return d.label; });

  var stackTotalsPlugin = {
    id: 'stackTotals',
    afterDraw: function(chart) {
      var ctx = chart.ctx;
      var meta = {};
      chart.data.datasets.forEach(function(ds, i) {
        var m = chart.getDatasetMeta(i);
        if (!m.hidden) {
          m.data.forEach(function(bar, j) {
            var stack = ds.stack;
            if (!meta[stack]) meta[stack] = {};
            if (!meta[stack][j]) meta[stack][j] = { maxY: Infinity, total: 0 };
            meta[stack][j].total += ds.data[j] || 0;
            if (bar.y < meta[stack][j].maxY) meta[stack][j].maxY = bar.y;
            meta[stack][j].x = bar.x;
          });
        }
      });
      ctx.save();
      ctx.textAlign = 'center';
      Object.keys(meta).forEach(function(stack) {
        var isCreated = stack === 'created';
        Object.keys(meta[stack]).forEach(function(j) {
          var idx = parseInt(j);
          var info = meta[stack][j];
          var fte = info.total;
          var hc = isCreated ? data[idx].c_count : data[idx].o_count;
          if (fte === 0 && hc === 0) return;
          ctx.font = 'bold 11px -apple-system, sans-serif';
          ctx.fillStyle = '#1a1a2e';
          ctx.fillText(fmtFTE(fte) + ' FTE', info.x, info.maxY - 14);
          ctx.font = '10px -apple-system, sans-serif';
          ctx.fillStyle = '#666';
          ctx.fillText(hc + ' HC', info.x, info.maxY - 3);
        });
      });
      ctx.restore();
    }
  };

  if (fteChart) fteChart.destroy();
  fteChart = new Chart(document.getElementById('fteChart'), {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [
        { label: 'Created - Full-Time (1.0)', data: data.map(function(d){return d.c_ft;}), backgroundColor: '#2563eb', stack: 'created' },
        { label: 'Created - Part-Time (0.5)', data: data.map(function(d){return d.c_pt;}), backgroundColor: '#60a5fa', stack: 'created' },
        { label: 'Created - PT <20hrs (0.25)', data: data.map(function(d){return d.c_pt20;}), backgroundColor: '#93c5fd', stack: 'created' },
        { label: 'Created - Other (0.5)', data: data.map(function(d){return d.c_other;}), backgroundColor: '#bfdbfe', stack: 'created' },
        { label: 'Offboarded - Full-Time (1.0)', data: data.map(function(d){return d.o_ft;}), backgroundColor: '#dc2626', stack: 'offboarded' },
        { label: 'Offboarded - Part-Time (0.5)', data: data.map(function(d){return d.o_pt;}), backgroundColor: '#f87171', stack: 'offboarded' },
        { label: 'Offboarded - PT <20hrs (0.25)', data: data.map(function(d){return d.o_pt20;}), backgroundColor: '#fca5a5', stack: 'offboarded' },
        { label: 'Offboarded - Other (0.5)', data: data.map(function(d){return d.o_other;}), backgroundColor: '#fecaca', stack: 'offboarded' }
      ]
    },
    plugins: [stackTotalsPlugin],
    options: {
      responsive: true,
      layout: { padding: { top: 25 } },
      plugins: { legend: { position: 'top', labels: { boxWidth: 12, font: { size: 11 } } } },
      scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true, title: { display: true, text: 'FTE Hires' } } }
    }
  });

  var netTotalsPlugin = {
    id: 'netTotals',
    afterDraw: function(chart) {
      var ctx = chart.ctx;
      var meta = chart.getDatasetMeta(0);
      meta.data.forEach(function(bar, i) {
        var val = chart.data.datasets[0].data[i];
        if (val === 0) return;
        var fmt = fmtFTE(val);
        ctx.save();
        ctx.font = 'bold 11px -apple-system, sans-serif';
        ctx.textAlign = 'center';
        ctx.fillStyle = '#1a1a2e';
        var yPos = val >= 0 ? bar.y - 6 : bar.y + 14;
        ctx.fillText(fmt, bar.x, yPos);
        ctx.restore();
      });
    }
  };

  if (netChart) netChart.destroy();
  netChart = new Chart(document.getElementById('netChart'), {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Net FTE',
        data: data.map(function(d){return d.net;}),
        backgroundColor: data.map(function(d){return d.net >= 0 ? '#10b981' : '#ef4444';}),
        borderRadius: 4
      }]
    },
    plugins: [netTotalsPlugin],
    options: {
      responsive: true,
      layout: { padding: { top: 20 } },
      plugins: { legend: { display: false } },
      scales: { y: { title: { display: true, text: 'Net FTE (Created - Offboarded by Date)' } } }
    }
  });
}

document.getElementById('periodCount').addEventListener('change', render);
document.getElementById('groupBy').addEventListener('change', render);

// === LIVE DATA LOADING (auto-refresh every 5 minutes) ===
var REFRESH_INTERVAL = 5 * 60 * 1000; // 5 minutes
var refreshTimer = null;

function loadLiveData() {
  fetch('/api/tickets')
    .then(function(res) { return res.json(); })
    .then(function(data) {
      if (data.error) {
        document.getElementById('loadingOverlay').innerHTML = '<div style="color:#dc2626;">Error: ' + data.error + '<br><small>Retrying in 30 seconds...</small></div>';
        setTimeout(loadLiveData, 30000);
        return;
      }
      RAW_DATA = data.raw;
      OFFBOARD_DATA = data.offboard;
      document.getElementById('timestampLine').textContent = 'Data last extracted: ' + data.timestamp + ' | ' + data.counts.created + ' created, ' + data.counts.offboarded + ' offboarded tickets (auto-refreshes every 5 min)';
      document.getElementById('loadingOverlay').style.display = 'none';
      document.getElementById('liveContent').style.display = 'block';
      renderRunningUpdate();
      render();
      // Schedule next refresh
      if (refreshTimer) clearTimeout(refreshTimer);
      refreshTimer = setTimeout(loadLiveData, REFRESH_INTERVAL);
    })
    .catch(function(err) {
      document.getElementById('loadingOverlay').innerHTML = '<div style="color:#dc2626;">Failed to load data: ' + err.message + '<br><small>Retrying in 30 seconds...</small></div>';
      setTimeout(loadLiveData, 30000);
    });
}

loadLiveData();

// === TAB SWITCHING ===
function switchTab(tab) {
  document.querySelectorAll('.tab-content').forEach(function(el) { el.classList.remove('active'); });
  document.querySelectorAll('.tab-btn').forEach(function(el) { el.classList.remove('active'); });
  document.getElementById('tab-' + tab).classList.add('active');
  event.target.classList.add('active');
  if (tab === 'history' && !histRendered) renderHistory();
}

// === HISTORICAL DATA ===
var HIST_DATA = [
  {m:"2024-01",h:{ft:211,pt:108,pt20:0,ob:5,pb:8,hc:332,fte:271.5},c:{ft:134,pt:80,pt20:0,ob:1,pb:2,hc:217,fte:175.5},net:96},
  {m:"2024-02",h:{ft:264,pt:124,pt20:0,ob:4,pb:5,hc:397,fte:330.5},c:{ft:164,pt:102,pt20:0,ob:0,pb:3,hc:269,fte:216.5},net:114},
  {m:"2024-03",h:{ft:291,pt:105,pt20:0,ob:2,pb:4,hc:402,fte:346.5},c:{ft:163,pt:64,pt20:0,ob:2,pb:9,hc:238,fte:200.5},net:146},
  {m:"2024-04",h:{ft:297,pt:110,pt20:0,ob:4,pb:11,hc:422,fte:359.5},c:{ft:185,pt:67,pt20:0,ob:1,pb:5,hc:258,fte:221.5},net:138},
  {m:"2024-05",h:{ft:349,pt:139,pt20:0,ob:0,pb:3,hc:491,fte:420},c:{ft:189,pt:108,pt20:0,ob:3,pb:3,hc:303,fte:246},net:174},
  {m:"2024-06",h:{ft:287,pt:113,pt20:0,ob:2,pb:0,hc:402,fte:344.5},c:{ft:184,pt:84,pt20:0,ob:0,pb:3,hc:271,fte:227.5},net:117},
  {m:"2024-07",h:{ft:353,pt:147,pt20:0,ob:0,pb:1,hc:501,fte:427},c:{ft:226,pt:106,pt20:0,ob:0,pb:3,hc:335,fte:280.5},net:146.5},
  {m:"2024-08",h:{ft:377,pt:168,pt20:0,ob:1,pb:9,hc:555,fte:466},c:{ft:245,pt:105,pt20:0,ob:0,pb:7,hc:357,fte:301},net:165},
  {m:"2024-09",h:{ft:341,pt:165,pt20:0,ob:2,pb:2,hc:510,fte:425.5},c:{ft:229,pt:97,pt20:0,ob:2,pb:1,hc:329,fte:279},net:146.5},
  {m:"2024-10",h:{ft:341,pt:200,pt20:0,ob:0,pb:8,hc:549,fte:445},c:{ft:238,pt:113,pt20:0,ob:0,pb:4,hc:352,fte:296.5},net:148.5},
  {m:"2024-11",h:{ft:371,pt:182,pt20:0,ob:1,pb:3,hc:557,fte:464},c:{ft:212,pt:122,pt20:0,ob:2,pb:3,hc:339,fte:275.5},net:188.5},
  {m:"2024-12",h:{ft:284,pt:113,pt20:0,ob:3,pb:2,hc:402,fte:343},c:{ft:233,pt:137,pt20:0,ob:4,pb:0,hc:374,fte:303.5},net:39.5},
  {m:"2025-01",h:{ft:339,pt:143,pt20:0,ob:0,pb:4,hc:486,fte:412.5},c:{ft:228,pt:139,pt20:0,ob:0,pb:4,hc:371,fte:299.5},net:113},
  {m:"2025-02",h:{ft:428,pt:189,pt20:0,ob:1,pb:0,hc:618,fte:523},c:{ft:255,pt:126,pt20:0,ob:0,pb:5,hc:386,fte:320.5},net:202.5},
  {m:"2025-03",h:{ft:405,pt:165,pt20:1,ob:0,pb:2,hc:573,fte:488.75},c:{ft:282,pt:140,pt20:0,ob:1,pb:5,hc:428,fte:355},net:133.75},
  {m:"2025-04",h:{ft:416,pt:205,pt20:29,ob:0,pb:0,hc:650,fte:525.75},c:{ft:252,pt:120,pt20:3,ob:0,pb:4,hc:379,fte:314.75},net:211},
  {m:"2025-05",h:{ft:515,pt:175,pt20:36,ob:0,pb:5,hc:731,fte:614},c:{ft:277,pt:155,pt20:3,ob:4,pb:2,hc:441,fte:358.25},net:255.75},
  {m:"2025-06",h:{ft:368,pt:178,pt20:43,ob:0,pb:5,hc:594,fte:470.25},c:{ft:264,pt:122,pt20:10,ob:1,pb:1,hc:398,fte:328.5},net:141.75},
  {m:"2025-07",h:{ft:424,pt:168,pt20:62,ob:0,pb:3,hc:657,fte:525},c:{ft:268,pt:137,pt20:17,ob:0,pb:3,hc:425,fte:342.25},net:182.75},
  {m:"2025-08",h:{ft:441,pt:200,pt20:29,ob:0,pb:3,hc:673,fte:549.75},c:{ft:260,pt:142,pt20:21,ob:0,pb:0,hc:423,fte:336.25},net:213.5},
  {m:"2025-09",h:{ft:379,pt:193,pt20:22,ob:1,pb:1,hc:596,fte:482},c:{ft:294,pt:145,pt20:18,ob:1,pb:2,hc:460,fte:372.5},net:109.5},
  {m:"2025-10",h:{ft:423,pt:195,pt20:16,ob:0,pb:5,hc:639,fte:527},c:{ft:312,pt:182,pt20:17,ob:0,pb:1,hc:512,fte:407.75},net:119.25},
  {m:"2025-11",h:{ft:363,pt:186,pt20:11,ob:0,pb:3,hc:563,fte:460.25},c:{ft:269,pt:137,pt20:15,ob:0,pb:1,hc:422,fte:341.75},net:118.5},
  {m:"2025-12",h:{ft:312,pt:132,pt20:13,ob:0,pb:1,hc:458,fte:381.75},c:{ft:261,pt:166,pt20:10,ob:0,pb:2,hc:439,fte:347.5},net:34.25},
  {m:"2026-01",h:{ft:331,pt:157,pt20:8,ob:0,pb:2,hc:498,fte:412.5},c:{ft:274,pt:134,pt20:16,ob:0,pb:1,hc:425,fte:345.5},net:67},
  {m:"2026-02",h:{ft:379,pt:177,pt20:16,ob:0,pb:1,hc:573,fte:472},c:{ft:281,pt:146,pt20:22,ob:1,pb:3,hc:453,fte:361.5},net:110.5}
];

var histRendered = false;
var histChart = null, histNetChart = null;
var activeYears = {};

function getHistYears() {
  var years = {};
  HIST_DATA.forEach(function(d) { years[d.m.slice(0,4)] = true; });
  return Object.keys(years).sort();
}

function renderHistory() {
  histRendered = true;
  var years = getHistYears();
  var btnContainer = document.getElementById('yearBtns');
  btnContainer.innerHTML = '';
  years.forEach(function(y) {
    activeYears[y] = true;
    var btn = document.createElement('button');
    btn.className = 'year-btn active';
    btn.textContent = y;
    btn.onclick = function() {
      activeYears[y] = !activeYears[y];
      btn.classList.toggle('active');
      updateHistCharts();
      updateHistTable();
    };
    btnContainer.appendChild(btn);
  });
  updateHistCharts();
  updateHistTable();
}

function getFilteredHist() {
  return HIST_DATA.filter(function(d) { return activeYears[d.m.slice(0,4)]; });
}

function updateHistCharts() {
  var data = getFilteredHist();
  var monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  var labels = data.map(function(d) { var p = d.m.split('-'); return monthNames[parseInt(p[1])-1] + ' ' + p[0]; });

  var histNetPlugin = {
    id: 'histNetTotals',
    afterDraw: function(chart) {
      var ctx = chart.ctx;
      var meta = chart.getDatasetMeta(0);
      meta.data.forEach(function(bar, i) {
        var val = chart.data.datasets[0].data[i];
        if (val === 0) return;
        ctx.save();
        ctx.font = 'bold 10px -apple-system, sans-serif';
        ctx.textAlign = 'center';
        ctx.fillStyle = '#1a1a2e';
        ctx.fillText(fmtFTE(val), bar.x, val >= 0 ? bar.y - 5 : bar.y + 12);
        ctx.restore();
      });
    }
  };

  var stackPlugin = {
    id: 'histStackTotals',
    afterDraw: function(chart) {
      var ctx = chart.ctx;
      var meta = {};
      chart.data.datasets.forEach(function(ds, i) {
        var m = chart.getDatasetMeta(i);
        if (!m.hidden) {
          m.data.forEach(function(bar, j) {
            var stack = ds.stack;
            if (!meta[stack]) meta[stack] = {};
            if (!meta[stack][j]) meta[stack][j] = { maxY: Infinity, total: 0 };
            meta[stack][j].total += ds.data[j] || 0;
            if (bar.y < meta[stack][j].maxY) meta[stack][j].maxY = bar.y;
            meta[stack][j].x = bar.x;
          });
        }
      });
      ctx.save();
      ctx.textAlign = 'center';
      ctx.font = 'bold 9px -apple-system, sans-serif';
      ctx.fillStyle = '#1a1a2e';
      Object.keys(meta).forEach(function(stack) {
        Object.keys(meta[stack]).forEach(function(j) {
          var info = meta[stack][j];
          if (info.total === 0) return;
          ctx.fillText(fmtFTE(info.total), info.x, info.maxY - 4);
        });
      });
      ctx.restore();
    }
  };

  if (histChart) histChart.destroy();
  histChart = new Chart(document.getElementById('histChart'), {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [
        { label: 'Hire - Full-Time', data: data.map(function(d){return d.h.ft;}), backgroundColor: '#2563eb', stack: 'hire' },
        { label: 'Hire - Part-Time', data: data.map(function(d){return d.h.pt*0.5;}), backgroundColor: '#60a5fa', stack: 'hire' },
        { label: 'Hire - PT<20hrs', data: data.map(function(d){return d.h.pt20*0.25;}), backgroundColor: '#93c5fd', stack: 'hire' },
        { label: 'Hire - Other', data: data.map(function(d){return (d.h.ob+d.h.pb)*0.5;}), backgroundColor: '#bfdbfe', stack: 'hire' },
        { label: 'Churn - Full-Time', data: data.map(function(d){return d.c.ft;}), backgroundColor: '#dc2626', stack: 'churn' },
        { label: 'Churn - Part-Time', data: data.map(function(d){return d.c.pt*0.5;}), backgroundColor: '#f87171', stack: 'churn' },
        { label: 'Churn - PT<20hrs', data: data.map(function(d){return d.c.pt20*0.25;}), backgroundColor: '#fca5a5', stack: 'churn' },
        { label: 'Churn - Other', data: data.map(function(d){return (d.c.ob+d.c.pb)*0.5;}), backgroundColor: '#fecaca', stack: 'churn' }
      ]
    },
    plugins: [stackPlugin],
    options: {
      responsive: true,
      layout: { padding: { top: 20 } },
      plugins: { legend: { position: 'top', labels: { boxWidth: 12, font: { size: 10 } } } },
      scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true, title: { display: true, text: 'FTE' } } }
    }
  });

  if (histNetChart) histNetChart.destroy();
  histNetChart = new Chart(document.getElementById('histNetChart'), {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Net FTE',
        data: data.map(function(d){return d.net;}),
        backgroundColor: data.map(function(d){return d.net >= 0 ? '#10b981' : '#ef4444';}),
        borderRadius: 4
      }]
    },
    plugins: [histNetPlugin],
    options: {
      responsive: true,
      layout: { padding: { top: 20 } },
      plugins: { legend: { display: false } },
      scales: { y: { title: { display: true, text: 'Net FTE' } } }
    }
  });
}

function updateHistTable() {
  var data = getFilteredHist();
  var monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  var container = document.getElementById('histTableContainer');

  var html = '<table class="hist-table"><thead><tr><th></th>';
  data.forEach(function(d) {
    var p = d.m.split('-');
    html += '<th colspan="2" class="month-hdr">' + monthNames[parseInt(p[1])-1] + ' ' + p[0] + '</th>';
  });
  html += '</tr><tr><th></th>';
  data.forEach(function() { html += '<th class="sub-hdr-hire">New Hires</th><th class="sub-hdr-churn">Churn</th>'; });
  html += '</tr></thead><tbody>';

  html += '<tr class="hc-row"><td class="row-label"><strong>Headcount</strong></td>';
  data.forEach(function(d) { html += '<td><strong>' + d.h.hc + '</strong></td><td><strong>' + d.c.hc + '</strong></td>'; });
  html += '</tr>';

  var types = [
    {key:'ft', label:'Full Time'},
    {key:'pt', label:'Part Time'},
    {key:'pt20', label:'Part Time under 20 hrs'},
    {key:'ob', label:'Output-Based'},
    {key:'pb', label:'Project-Based'}
  ];
  types.forEach(function(t) {
    html += '<tr><td class="row-label" style="padding-left:24px">' + t.label + '</td>';
    data.forEach(function(d) {
      html += '<td>' + (d.h[t.key] || '') + '</td><td>' + (d.c[t.key] || '') + '</td>';
    });
    html += '</tr>';
  });

  html += '<tr class="fte-row"><td class="row-label"><strong>FTE Equivalent</strong></td>';
  data.forEach(function(d) { html += '<td><strong>' + fmtFTE(d.h.fte) + '</strong></td><td><strong>' + fmtFTE(d.c.fte) + '</strong></td>'; });
  html += '</tr>';

  html += '<tr class="net-row"><td class="row-label"><strong>Net FTE</strong></td>';
  data.forEach(function(d) { html += '<td colspan="2"><strong>' + fmtFTE(d.net) + '</strong></td>'; });
  html += '</tr>';

  html += '</tbody></table>';
  container.innerHTML = html;
}
