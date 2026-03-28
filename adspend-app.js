var spC=null,clC=null;
var rawData=null;
var currentGroup="day";
var brkGroup="day";

// Tab switching via data attributes
document.querySelectorAll(".tab-btn").forEach(function(btn){
  btn.addEventListener("click",function(){
    document.querySelectorAll(".tab-content").forEach(function(e){e.classList.remove("active")});
    document.querySelectorAll(".tab-btn").forEach(function(e){e.classList.remove("active")});
    document.getElementById("tab-"+btn.getAttribute("data-tab")).classList.add("active");
    btn.classList.add("active");
  });
});

// Toggle group buttons
document.querySelectorAll("#groupToggle .toggle-btn").forEach(function(btn){
  btn.addEventListener("click",function(){
    document.querySelectorAll("#groupToggle .toggle-btn").forEach(function(e){e.classList.remove("active")});
    btn.classList.add("active");
    currentGroup=btn.getAttribute("data-group");
    applyFilters();
  });
});
document.querySelectorAll("#brkGroupToggle .toggle-btn").forEach(function(btn){
  btn.addEventListener("click",function(){
    document.querySelectorAll("#brkGroupToggle .toggle-btn").forEach(function(e){e.classList.remove("active")});
    btn.classList.add("active");
    brkGroup=btn.getAttribute("data-group");
    applyFilters();
  });
});

// Period filter change listeners
document.getElementById("periodFilter").addEventListener("change",applyFilters);
document.getElementById("campPeriodFilter").addEventListener("change",applyFilters);
document.getElementById("brkPeriodFilter").addEventListener("change",applyFilters);

function fmt(n){return n.toLocaleString("en-US")}
function fm(n){return "$"+n.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2})}

function filterByDays(ts,days){
  if(days==="all") return ts;
  var cutoff=new Date();
  cutoff.setDate(cutoff.getDate()-parseInt(days));
  var cutStr=cutoff.toISOString().slice(0,10);
  return ts.filter(function(d){return d.day>=cutStr});
}

function getWeekKey(dateStr){
  var d=new Date(dateStr+"T00:00:00");
  var day=d.getDay();
  var diff=d.getDate()-day+(day===0?-6:1);
  var monday=new Date(d);
  monday.setDate(diff);
  return monday.toISOString().slice(0,10);
}

function getMonthKey(dateStr){
  return dateStr.slice(0,7);
}

function groupTimeseries(ts,mode){
  if(mode==="day") return ts;
  var map={};
  for(var i=0;i<ts.length;i++){
    var d=ts[i];
    var key=mode==="week"?getWeekKey(d.day):getMonthKey(d.day);
    if(!map[key]) map[key]={day:key,clicks:0,impressions:0,cost:0,conversions:0};
    map[key].clicks+=d.clicks;
    map[key].impressions+=d.impressions;
    map[key].cost+=d.cost;
    map[key].conversions+=d.conversions;
  }
  var result=Object.values(map).sort(function(a,b){return a.day.localeCompare(b.day)});
  for(var j=0;j<result.length;j++){
    var r=result[j];
    r.cost=Math.round(r.cost*100)/100;
    r.conversions=Math.round(r.conversions*100)/100;
    r.ctr=r.impressions>0?Math.round((r.clicks/r.impressions)*10000)/100:0;
    r.avg_cpc=r.clicks>0?Math.round((r.cost/r.clicks)*100)/100:0;
  }
  return result;
}

function computeSummary(ts,campaigns){
  var totalClicks=0,totalImpr=0,totalCost=0,totalConv=0;
  for(var i=0;i<ts.length;i++){
    totalClicks+=ts[i].clicks;
    totalImpr+=ts[i].impressions;
    totalCost+=ts[i].cost;
    totalConv+=ts[i].conversions;
  }
  return {
    total_clicks:totalClicks,
    total_impressions:totalImpr,
    total_cost:Math.round(totalCost*100)/100,
    total_conversions:Math.round(totalConv*100)/100,
    avg_ctr:totalImpr>0?Math.round((totalClicks/totalImpr)*10000)/100:0,
    avg_cpc:totalClicks>0?Math.round((totalCost/totalClicks)*100)/100:0,
    cost_per_conversion:totalConv>0?Math.round((totalCost/totalConv)*100)/100:0,
    total_days:ts.length,
    total_campaigns:campaigns,
    date_range:{
      start:ts.length?ts[0].day:null,
      end:ts.length?ts[ts.length-1].day:null
    }
  };
}

function filterCampaigns(allCamps,rawTs,days){
  if(days==="all") return allCamps;
  var filtered=filterByDays(rawTs,days);
  var dateSet={};
  for(var i=0;i<filtered.length;i++) dateSet[filtered[i].day]=true;
  // We need to recompute from raw campaign rows, but we only have aggregated data
  // So we return the same campaigns (they cover the full range) - the summary cards use timeseries filtering
  return allCamps;
}

function groupLabel(mode){
  if(mode==="week") return "Weekly";
  if(mode==="month") return "Monthly";
  return "Daily";
}

function applyFilters(){
  if(!rawData) return;

  // Overview
  var ovPeriod=document.getElementById("periodFilter").value;
  var ovTs=filterByDays(rawData.timeseries,ovPeriod);
  var ovGrouped=groupTimeseries(ovTs,currentGroup);
  var ovSummary=computeSummary(ovTs,rawData.campaigns.length);
  renderSummary(ovSummary);
  renderSpendChart(ovGrouped);
  renderClicksChart(ovGrouped);
  document.getElementById("spendChartTitle").textContent=groupLabel(currentGroup)+" Spend";
  document.getElementById("clicksChartTitle").textContent=groupLabel(currentGroup)+" Clicks & Conversions";

  // Campaigns
  var campPeriod=document.getElementById("campPeriodFilter").value;
  renderCampTable(rawData.campaigns);

  // Breakdown
  var brkPeriod=document.getElementById("brkPeriodFilter").value;
  var brkTs=filterByDays(rawData.timeseries,brkPeriod);
  var brkGrouped=groupTimeseries(brkTs,brkGroup);
  renderBreakdownTable(brkGrouped);
  document.getElementById("brkTableTitle").textContent=groupLabel(brkGroup)+" Performance";
  document.getElementById("brkDateHeader").textContent=brkGroup==="day"?"Date":brkGroup==="week"?"Week Starting":"Month";
}

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
    data:{labels:ts.map(function(d){return d.day}),datasets:[{label:"Spend ($)",data:ts.map(function(d){return d.cost}),backgroundColor:"rgba(37,99,235,0.7)",borderRadius:3}]},
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

function renderBreakdownTable(ts){
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
  rawData=data;
  document.getElementById("loadingAds").style.display="none";
  document.getElementById("tab-overview").classList.add("active");
  applyFilters();
}).catch(function(err){
  document.getElementById("loadingAds").innerHTML="<div style=\"color:var(--red)\"><strong>Failed to load</strong><br>"+err.message+"</div>";
});
