// report-app.js — Monthly KPI Report client-side logic

var MONTH_NAMES = ['January','February','March','April','May','June','July','August','September','October','November','December'];

function getMonthParam() {
  var params = new URLSearchParams(window.location.search);
  var m = params.get('month');
  if (m && /^\d{4}-\d{2}$/.test(m)) return m;
  // Default to previous month
  var now = new Date();
  var yr = now.getFullYear();
  var mo = now.getMonth(); // 0-indexed, so this is already "previous month"
  if (mo === 0) { yr--; mo = 12; }
  return yr + '-' + String(mo).padStart(2, '0');
}

function monthLabel(m) {
  if (!m) return '';
  var parts = m.split('-');
  return MONTH_NAMES[parseInt(parts[1]) - 1] + ' ' + parts[0];
}

function prevMonth(m) {
  var parts = m.split('-');
  var yr = parseInt(parts[0]);
  var mo = parseInt(parts[1]);
  if (mo === 1) { yr--; mo = 12; } else mo--;
  return yr + '-' + String(mo).padStart(2, '0');
}

function nextMonth(m) {
  var parts = m.split('-');
  var yr = parseInt(parts[0]);
  var mo = parseInt(parts[1]);
  if (mo === 12) { yr++; mo = 1; } else mo++;
  return yr + '-' + String(mo).padStart(2, '0');
}

function fmt(n) {
  if (n === null || n === undefined || n === '') return '—';
  return Number(n).toLocaleString('en-US');
}

function fmtD(n, decimals) {
  if (n === null || n === undefined || n === '') return '—';
  return Number(n).toLocaleString('en-US', { minimumFractionDigits: decimals || 0, maximumFractionDigits: decimals || 0 });
}

function fm(n) {
  if (n === null || n === undefined || n === '') return '—';
  return '$' + Number(n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function pct(n) {
  if (n === null || n === undefined || n === '') return '—';
  return Number(n).toFixed(1) + '%';
}

function changeCell(cur, prev, format, invertColor) {
  if (cur === null || cur === undefined || cur === '' || prev === null || prev === undefined || prev === '') {
    return '<td class="ar change-zero">—</td>';
  }
  var c = Number(cur);
  var p = Number(prev);
  if (p === 0 && c === 0) return '<td class="ar change-zero">—</td>';
  var diff, label;
  if (p === 0) {
    label = c > 0 ? '+100%' : '0%';
    diff = c;
  } else {
    var pctChange = ((c - p) / Math.abs(p)) * 100;
    label = (pctChange >= 0 ? '+' : '') + pctChange.toFixed(1) + '%';
    diff = c - p;
  }
  var isPositive = diff > 0;
  // For some metrics (like churn, offboardings), positive change is bad
  var cls;
  if (diff === 0) cls = 'change-zero';
  else if (invertColor) cls = isPositive ? 'change-neg' : 'change-pos';
  else cls = isPositive ? 'change-pos' : 'change-neg';
  return '<td class="ar ' + cls + '">' + label + '</td>';
}

function formatValue(val, format) {
  if (format === '$') return fm(val);
  if (format === '%') return pct(val);
  if (format === 'f1') return fmtD(val, 1);
  if (format === 'f2') return fmtD(val, 2);
  return fmt(val);
}

function renderKPITable(tableId, sections, current, previous) {
  var tb = document.querySelector('#' + tableId + ' tbody');
  var h = '';
  for (var s = 0; s < sections.length; s++) {
    var sec = sections[s];
    h += '<tr class="cat-header"><td colspan="4">' + sec.label + '</td></tr>';
    for (var r = 0; r < sec.rows.length; r++) {
      var row = sec.rows[r];
      var curVal = current ? current[row.key] : null;
      var prevVal = previous ? previous[row.key] : null;
      h += '<tr>';
      h += '<td>' + row.label + '</td>';
      h += '<td class="ar" style="font-weight:600">' + formatValue(curVal, row.format) + '</td>';
      h += '<td class="ar">' + formatValue(prevVal, row.format) + '</td>';
      h += changeCell(curVal, prevVal, row.format, row.invertColor);
      h += '</tr>';
    }
  }
  tb.innerHTML = h;
}

// Acquisition KPI definitions
var acquisitionSections = [
  { label: 'Company', rows: [
    { key: 'monthly_contract_value', label: 'Monthly Contract Value (MCV)', format: '$' },
    { key: 'monthly_contract_margin_pct', label: 'Monthly Contract Margin %', format: '%' },
    { key: 'active_outsource_fte', label: 'Active Outsource FTE', format: 'f1' },
    { key: '_fte_per_internal', label: 'FTE Per Internal Staff', format: 'f1' }
  ]},
  { label: 'Marketing', rows: [
    { key: 'google_ads_spend', label: 'Google Ads Spend', format: '$' },
    { key: 'marketing_qualified_leads', label: 'Marketing Qualified Leads (MQLs)', format: '' },
    { key: 'cost_per_lead', label: 'Cost Per Lead', format: '$' }
  ]},
  { label: 'Sales', rows: [
    { key: 'new_jobs_new_client', label: 'New Client Jobs Created', format: '' },
    { key: 'lead_to_new_job_conv_rate', label: 'Lead to New Job Conv. Rate', format: '%' },
    { key: 'lead_to_closed_fte_conv_rate', label: 'Lead to Closed FTE Conv. Rate', format: '%' }
  ]},
  { label: 'Recruitment', rows: [
    { key: 'time_to_first_candidate_submission', label: 'Time to 1st Candidate Submission (Median Days)', format: 'f1' },
    { key: 'candidate_endorsements_per_recruitment_hc', label: 'Candidate Endorsements Per Recruitment HC', format: 'f1' }
  ]},
  { label: 'Combined', rows: [
    { key: 'total_fte_hires', label: 'Total FTE Hires', format: 'f1' },
    { key: '_hires_per_recruiter', label: 'Hires Per Recruiter HC', format: 'f1' },
    { key: '_hires_per_sales', label: 'Hires Per Sales HC', format: 'f1' }
  ]}
];

// Retention & Expansion KPI definitions
var retentionSections = [
  { label: 'Client Success', rows: [
    { key: 'lost_ftes', label: 'Lost FTEs', format: 'f1', invertColor: true },
    { key: 'fte_churn_rate', label: 'FTE Churn Rate', format: '%', invertColor: true },
    { key: 'pct_offboardings_under_30_days', label: '% Offboardings <30 Days', format: '%', invertColor: true },
    { key: 'backfill_ftes_hired', label: 'Backfill FTEs Hired', format: 'f1' }
  ]},
  { label: 'Expansion', rows: [
    { key: 'existing_client_hires', label: 'Existing Client Hires (FTE)', format: 'f1' },
    { key: 'expansion_rate', label: 'Expansion Rate', format: '%' }
  ]}
];

// Compute derived fields
function enrichData(d) {
  if (!d) return d;
  var r = Object.assign({}, d);
  // FTE per internal staff
  if (r.active_outsource_fte && r.internal_staff_headcount) {
    r._fte_per_internal = Math.round((r.active_outsource_fte / r.internal_staff_headcount) * 10) / 10;
  }
  // Hires per recruiter/sales
  if (r.total_fte_hires && r.recruitment_team_hc) {
    r._hires_per_recruiter = Math.round((r.total_fte_hires / r.recruitment_team_hc) * 10) / 10;
  }
  if (r.total_fte_hires && r.sales_team_hc) {
    r._hires_per_sales = Math.round((r.total_fte_hires / r.sales_team_hc) * 10) / 10;
  }
  return r;
}

// Render trend charts using all available report data
var trendCharts = {};

function renderTrendCharts(allReports, currentMonth) {
  // Sort reports by month and take last 14
  var sorted = allReports.slice().sort(function(a, b) { return a.month.localeCompare(b.month); });
  // Filter to only months up to current
  sorted = sorted.filter(function(r) { return r.month <= currentMonth; });
  var last14 = sorted.slice(-14);
  var labels = last14.map(function(r) { return r.month; });

  // Chart 1: Google Ads Spend (bar)
  renderBarChart('chartAdsSpend', labels, [
    { label: 'Ads Spend', data: last14.map(function(r) { return r.google_ads_spend || 0; }), color: '#2563eb' }
  ], '$');

  // Chart 2: Hires by type (stacked bar)
  renderStackedBar('chartHires', labels, [
    { label: 'Full-Time', data: last14.map(function(r) { return r.hires_full_time || 0; }), color: '#2563eb' },
    { label: 'Part-Time', data: last14.map(function(r) { return r.hires_part_time || 0; }), color: '#60a5fa' },
    { label: 'PT <20hrs', data: last14.map(function(r) { return r.hires_pt_under_20 || 0; }), color: '#93c5fd' },
    { label: 'Project', data: last14.map(function(r) { return r.hires_project_based || 0; }), color: '#c084fc' },
    { label: 'Output', data: last14.map(function(r) { return r.hires_output_based || 0; }), color: '#a78bfa' }
  ]);

  // Chart 3: New Jobs by source (stacked bar)
  renderStackedBar('chartJobs', labels, [
    { label: 'New Client', data: last14.map(function(r) { return r.new_jobs_new_client || 0; }), color: '#059669' },
    { label: 'Existing Client', data: last14.map(function(r) { return r.new_jobs_existing_client || 0; }), color: '#34d399' },
    { label: 'Backfill', data: last14.map(function(r) { return r.new_jobs_backfill || 0; }), color: '#6ee7b7' }
  ]);

  // Chart 4: Offboardings by type (stacked bar)
  renderStackedBar('chartOffboardings', labels, [
    { label: 'Full-Time', data: last14.map(function(r) { return r.offboardings_full_time || 0; }), color: '#dc2626' },
    { label: 'Part-Time', data: last14.map(function(r) { return r.offboardings_part_time || 0; }), color: '#f87171' },
    { label: 'PT <20hrs', data: last14.map(function(r) { return r.offboardings_pt_under_20 || 0; }), color: '#fca5a5' },
    { label: 'Project', data: last14.map(function(r) { return r.offboardings_project_based || 0; }), color: '#fb923c' },
    { label: 'Output', data: last14.map(function(r) { return r.offboardings_output_based || 0; }), color: '#fdba74' }
  ]);
}

function renderBarChart(canvasId, labels, series, prefix) {
  var ctx = document.getElementById(canvasId).getContext('2d');
  if (trendCharts[canvasId]) trendCharts[canvasId].destroy();
  trendCharts[canvasId] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: series.map(function(s) {
        return { label: s.label, data: s.data, backgroundColor: s.color, borderRadius: 3 };
      })
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: series.length > 1, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } } },
      scales: {
        x: { ticks: { font: { size: 10 } }, grid: { display: false } },
        y: { ticks: { callback: function(v) { return (prefix || '') + v; } }, grid: { color: '#f1f5f9' } }
      }
    }
  });
}

function renderStackedBar(canvasId, labels, series) {
  var ctx = document.getElementById(canvasId).getContext('2d');
  if (trendCharts[canvasId]) trendCharts[canvasId].destroy();
  trendCharts[canvasId] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: series.map(function(s) {
        return { label: s.label, data: s.data, backgroundColor: s.color, borderRadius: 0 };
      })
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top', labels: { boxWidth: 12, font: { size: 11 } } } },
      scales: {
        x: { stacked: true, ticks: { font: { size: 10 } }, grid: { display: false } },
        y: { stacked: true, grid: { color: '#f1f5f9' } }
      }
    }
  });
}

// Project status editor
var projectRows = [];

function renderProjectEditor() {
  var el = document.getElementById('projectEditor');
  var pillarOptions = '<option value="">Select...</option><option>Easy to do business with</option><option>FTE Growth</option><option>Profitability</option><option>Other</option>';
  var statusOptions = '<option value="">Select...</option><option>On Track</option><option>At Risk</option><option>Delayed</option><option>Complete</option><option>Not Started</option>';
  var h = '';
  for (var i = 0; i < projectRows.length; i++) {
    var p = projectRows[i];
    h += '<div class="form-grid" style="grid-template-columns:1fr 1.5fr 1fr 2fr auto;margin-bottom:8px" data-pidx="' + i + '">';
    h += '<select class="proj-pillar" style="padding:8px 12px;border:1px solid var(--border);border-radius:8px;font-size:13px">' + pillarOptions.replace('>' + p.pillar + '<', ' selected>' + p.pillar + '<') + '</select>';
    h += '<input class="proj-project" placeholder="Project name" value="' + (p.project || '').replace(/"/g, '&quot;') + '" style="padding:8px 12px;border:1px solid var(--border);border-radius:8px;font-size:13px">';
    h += '<select class="proj-status" style="padding:8px 12px;border:1px solid var(--border);border-radius:8px;font-size:13px">' + statusOptions.replace('>' + p.status + '<', ' selected>' + p.status + '<') + '</select>';
    h += '<input class="proj-desc" placeholder="Description" value="' + (p.description || '').replace(/"/g, '&quot;') + '" style="padding:8px 12px;border:1px solid var(--border);border-radius:8px;font-size:13px">';
    h += '<button class="btn btn-secondary proj-remove" style="padding:6px 10px;font-size:16px" data-idx="' + i + '">&times;</button>';
    h += '</div>';
  }
  el.innerHTML = h;

  // Bind remove buttons
  el.querySelectorAll('.proj-remove').forEach(function(btn) {
    btn.addEventListener('click', function() {
      projectRows.splice(parseInt(btn.getAttribute('data-idx')), 1);
      renderProjectEditor();
    });
  });
}

function collectProjectRows() {
  var el = document.getElementById('projectEditor');
  var grids = el.querySelectorAll('[data-pidx]');
  var results = [];
  grids.forEach(function(g) {
    var pillar = g.querySelector('.proj-pillar').value;
    var project = g.querySelector('.proj-project').value;
    var status = g.querySelector('.proj-status').value;
    var desc = g.querySelector('.proj-desc').value;
    if (pillar || project) {
      results.push({ pillar: pillar, project: project, status: status, description: desc });
    }
  });
  return results;
}

function renderProjectsTable(projects) {
  var tb = document.querySelector('#tblProjects tbody');
  if (!projects || projects.length === 0) {
    tb.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text-muted);padding:20px">No project updates for this month</td></tr>';
    return;
  }
  var h = '';
  for (var i = 0; i < projects.length; i++) {
    var p = projects[i];
    var sc = (p.status || '').toLowerCase().replace(/\s+/g, '-');
    h += '<tr>';
    h += '<td style="font-weight:500">' + (p.pillar || '') + '</td>';
    h += '<td>' + (p.project || '') + '</td>';
    h += '<td><span class="badge badge-' + sc + '">' + (p.status || '—') + '</span></td>';
    h += '<td>' + (p.description || '') + '</td>';
    h += '</tr>';
  }
  tb.innerHTML = h;
}

// Populate manual form from data
function populateManualForm(data) {
  if (!data) return;
  var form = document.getElementById('manualForm');
  form.querySelectorAll('input').forEach(function(input) {
    var key = input.getAttribute('name');
    if (data[key] !== null && data[key] !== undefined && data[key] !== '') {
      input.value = data[key];
    }
  });
}

function collectManualForm() {
  var form = document.getElementById('manualForm');
  var data = {};
  form.querySelectorAll('input').forEach(function(input) {
    var key = input.getAttribute('name');
    var val = input.value.trim();
    if (val !== '') data[key] = parseFloat(val);
  });
  return data;
}

// Month navigation
function renderMonthNav(month) {
  var nav = document.getElementById('monthNav');
  var prev = prevMonth(month);
  var next = nextMonth(month);
  nav.innerHTML =
    '<a href="/report?month=' + prev + '">&larr; ' + monthLabel(prev) + '</a>' +
    '<a class="current">' + monthLabel(month) + '</a>' +
    '<a href="/report?month=' + next + '">' + monthLabel(next) + ' &rarr;</a>';
}

// Main init
var currentMonthKey = getMonthParam();

document.getElementById('reportTitle').textContent = 'Monthly Management KPI Report';
document.getElementById('reportSubtitle').textContent = monthLabel(currentMonthKey);
renderMonthNav(currentMonthKey);

// Toggle manual panel
document.getElementById('manualToggle').addEventListener('click', function() {
  document.getElementById('manualBody').classList.toggle('open');
});

// Generate automated data
document.getElementById('btnGenerate').addEventListener('click', function() {
  var btn = document.getElementById('btnGenerate');
  var status = document.getElementById('generateStatus');
  btn.disabled = true;
  status.textContent = 'Generating...';
  fetch('/api/report/generate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ month: currentMonthKey })
  }).then(function(r) { return r.json(); }).then(function(data) {
    if (data.error) { status.textContent = 'Error: ' + data.error; }
    else { status.textContent = 'Generated! Reloading...'; window.location.reload(); }
    btn.disabled = false;
  }).catch(function(err) {
    status.textContent = 'Error: ' + err.message;
    btn.disabled = false;
  });
});

// Save manual data
document.getElementById('btnSave').addEventListener('click', function() {
  var btn = document.getElementById('btnSave');
  var status = document.getElementById('saveStatus');
  btn.disabled = true;
  status.textContent = 'Saving...';
  var manualData = collectManualForm();
  manualData.month = currentMonthKey;
  manualData.projects = collectProjectRows();
  fetch('/api/report', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(manualData)
  }).then(function(r) { return r.json(); }).then(function(data) {
    if (data.error) { status.textContent = 'Error: ' + data.error; }
    else { status.textContent = 'Saved successfully!'; setTimeout(function() { status.textContent = ''; }, 3000); }
    btn.disabled = false;
  }).catch(function(err) {
    status.textContent = 'Error: ' + err.message;
    btn.disabled = false;
  });
});

// Add project row
document.getElementById('btnAddProject').addEventListener('click', function() {
  projectRows.push({ pillar: '', project: '', status: '', description: '' });
  renderProjectEditor();
});

// Load data
Promise.all([
  fetch('/api/report?month=' + currentMonthKey).then(function(r) { return r.json(); }),
  fetch('/api/reports').then(function(r) { return r.json(); })
]).then(function(results) {
  var reportData = results[0];
  var allReports = results[1];

  document.getElementById('loadingReport').style.display = 'none';
  document.getElementById('reportContent').style.display = 'block';

  if (reportData.error) {
    document.getElementById('reportContent').innerHTML =
      '<div class="not-configured"><h2>Reports Not Configured</h2><p>' + reportData.error + '</p>' +
      '<p style="margin-top:12px">Set <code>REPORT_SHEET_ID</code> and <code>GOOGLE_SERVICE_ACCOUNT_KEY</code> environment variables on Railway.</p></div>';
    return;
  }

  var current = enrichData(reportData.current);
  var previous = enrichData(reportData.previous);

  // Update headers with month labels
  document.getElementById('curMonthHeader').textContent = monthLabel(currentMonthKey);
  document.getElementById('prevMonthHeader').textContent = monthLabel(reportData.previous_month);

  if (current && current.generated_at) {
    document.getElementById('reportTimestamp').textContent = 'Generated: ' + new Date(current.generated_at).toLocaleString();
  }

  // Render KPI tables
  renderKPITable('tblAcquisition', acquisitionSections, current, previous);
  renderKPITable('tblRetention', retentionSections, current, previous);

  // Render project status
  var monthProjects = reportData.projects || [];
  renderProjectsTable(monthProjects);

  // Load projects into editor
  projectRows = monthProjects.map(function(p) {
    return { pillar: p.pillar || '', project: p.project || '', status: p.status || '', description: p.description || '' };
  });
  renderProjectEditor();

  // Populate manual form
  populateManualForm(current);

  // Render trend charts with all historical data
  if (allReports.months && allReports.months.length > 0) {
    // We need full data for all months — fetch them all
    // For now, use the summary data from /api/reports (limited fields)
    // TODO: In Phase 3+, add a bulk endpoint or fetch all months
    // For now, fetch all individual months in parallel
    var monthKeys = allReports.months.map(function(m) { return m.month; });
    Promise.all(monthKeys.map(function(mk) {
      return fetch('/api/report?month=' + mk).then(function(r) { return r.json(); }).then(function(d) { return d.current; });
    })).then(function(allData) {
      var validData = allData.filter(function(d) { return d !== null; });
      if (validData.length > 0) {
        renderTrendCharts(validData, currentMonthKey);
      }
    });
  }

}).catch(function(err) {
  document.getElementById('loadingReport').innerHTML =
    '<div style="color:var(--red)"><strong>Failed to load report</strong><br>' + err.message + '</div>';
});
