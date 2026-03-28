var spC=null,clC=null;

function switchTab(t,b){
  document.querySelectorAll(".tab-content").forEach(function(e){e.classList.remove("active")});
  document.querySelectorAll(".tab-btn").forEach(function(e){e.classList.remove("active")});
  document.getElementById("tab-"+t).classList.add("active");
  b.classList.add("active");
}

function fmt(n){return n.toLocaleString("en-US")}
function fm(n){return "$"+n.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}

function renderSummary(s){
  var el=document.getElementById("summaryCards");
  el.innerHTML=
    "<div class=card><div class=label>Total Spend</div><div class=value style=color:var(--blue)>"+fm(s.total_cost)+"</div><div class=sub>"+s.total_days+" days, "+s.total_campaigns+" campaigns</div></div>"+
    "<div class=card><div class=label>Total Clicks</div><div class=value>"+fmt(s.total_clicks)+"</div><div class=sub>Avg CPC: "+fm(s.avg_cpc)+"</div></div>"+
    "<div class=card><div class=label>Total Impressions</div><div class=value>"+fmt(s.total_impressions)+"</div><div class=sub>CTR: "+s.avg_ctr+"%</div></div>"+
    "<div class=card><div class=label>Conversions</div><div class=value>"+fmt(s.total_conversions)+"</div><div class=sub>Cost/conv: "+fm(s.cost_per_conversion)+"</div></div>";
  if(s.date_range.start&&s.date_range.end) document.getElementById("dateRange").textContent=s.date_range.start+" to "+s.date_range.end;
}

function renderSpendChart(ts){
  var ctx=document.getElementById("chartSpend").getContext("2d");
  if(spC)spC.destroy();
  spC=new Chart(ctx,{
    type:"bar",
    data:{labels:ts.map(function(d){return d.day}),datasets:[{label:"Daily Spend ($)",data:ts.map(function(d){return d.cost}),backgroundColor:"rgba(37,99,235,0.7)",borderRadius:3}]},
    options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{maxTicksLimit:20,font:{size:10}},grid:{display:false}},y:{ticks:{callback:function(v){return "$"+v}},grid:{color:"#f1f5f9"}}}}
  });
}

function renderClicksChart(ts){
  var ctx=document.getElementById("chartClicks").getContext("2d");
  if(clC)clC.destroy();
  clC=new Chart(ctx,{
    type:"line",
    data:{labels:ts.map(function(d){return d.day}),datasets:[
      {label:"Clicks",data:ts.map(function(d){return d.clicks}),borderColor:"#2563eb",backgroundColor:"rgba(37,99,235,0.1)",fill:true,tension:0.3,pointRadius:1,yAxisID:"y"},
      {label:"Conversions",data:ts.map(function(d){return d.conversions}),borderColor:"#059669",backgroundColor:"rgba(5,150,105,0.1)",fill:true,tension:0.3,pointRadius:1,yAxisID:"y1"}
    ]},
    options:{responsive:true,maintainAspectRatio:false,interaction:{mode:"index",intersect:false},scales:{x:{ticks:{maxTicksLimit:20,font:{size:10}},grid:{display:false}},y:{position:"left",grid:{color:"#f1f5f9"}},y1:{position:"right",grid:{drawOnChartArea:false}}}}
  });
}

function renderCampTable(camps){
  var tb=document.querySelector("#tblCamp tbody"),h="";
  for(var i=0;i<camps.length;i++){
    var c=camps[i];
    var sc=(c.status||"").toLowerCase().replace(/[^a-z]/g,"");
    h+="<tr><td style=\"font-weight:500;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">"+c.campaign+"</td>";
    h+="<td><span class=\"badge badge-"+sc+"\">"+(c.status||"Unknown")+"</span></td>";
    h+="<td class=ar>"+fmt(c.clicks)+"</td><td class=ar>"+fmt(c.impressions)+"</td>";
    h+="<td class=ar>"+c.ctr+"%</td><td class=ar>"+fm(c.avg_cpc)+"</td>";
    h+="<td class=ar style=\"font-weight:600\">"+fm(c.cost)+"</td><td class=ar>"+c.conversions+"</td></tr>";
  }
  tb.innerHTML=h;
}

function renderDailyTable(ts){
  var tb=document.querySelector("#tblDaily tbody"),h="",rev=ts.slice().reverse();
  for(var i=0;i<rev.length;i++){
    var d=rev[i];
    h+="<tr><td style=\"font-weight:500\">"+d.day+"</td><td class=ar>"+fmt(d.clicks)+"</td>";
    h+="<td class=ar>"+fmt(d.impressions)+"</td><td class=ar>"+d.ctr+"%</td>";
    h+="<td class=ar>"+fm(d.avg_cpc)+"</td><td class=ar style=\"font-weight:600\">"+fm(d.cost)+"</td>";
    h+="<td class=ar>"+d.conversions+"</td></tr>";
  }
  tb.innerHTML=h;
}

fetch("/api/ads").then(function(r){return r.json()}).then(function(data){
  document.getElementById("loadingAds").style.display="none";
  document.getElementById("tab-overview").classList.add("active");
  renderSummary(data.summary);
  renderSpendChart(data.timeseries);
  renderClicksChart(data.timeseries);
  renderCampTable(data.campaigns);
  renderDailyTable(data.timeseries);
}).catch(function(err){
  document.getElementById("loadingAds").innerHTML="<div style=\"color:var(--red)\"><strong>Failed to load</strong><br>"+err.message+"</div>";
});
