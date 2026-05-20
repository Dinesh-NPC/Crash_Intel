// ── Dark mode ─────────────────────────────────────────────────────────────
function toggleDark() {
  const html = document.documentElement;
  const isDark = html.getAttribute('data-theme') === 'dark';
  html.setAttribute('data-theme', isDark ? 'light' : 'dark');
  document.getElementById('darkIcon').textContent = isDark ? '🌙' : '☀️';
  localStorage.setItem('theme', isDark ? 'light' : 'dark');
}
(function() {
  const saved = localStorage.getItem('theme') || 'light';
  document.documentElement.setAttribute('data-theme', saved);
  const icon = document.getElementById('darkIcon');
  if (icon) icon.textContent = saved === 'dark' ? '☀️' : '🌙';
})();

// ── Navbar ────────────────────────────────────────────────────────────────
window.addEventListener('scroll', () => {
  const nav = document.getElementById('navbar');
  if (nav) nav.style.boxShadow = window.scrollY > 40 ? '0 2px 20px rgba(15,35,84,0.10)' : 'none';
});
function toggleMenu() {
  const m = document.getElementById('mobileMenu');
  if (m) m.classList.toggle('open');
}

// ── Colors ────────────────────────────────────────────────────────────────
const COLORS = { coral:'#e8392a', navy:'#0f2354', teal:'#0bbf9f', gold:'#f5a623', purple:'#7c3aed', blue:'#0ea5e9' };
const CHART_COLORS = [COLORS.coral, COLORS.navy, COLORS.teal, COLORS.gold, COLORS.purple, COLORS.blue];

if (typeof Chart !== 'undefined') {
  Chart.defaults.font.family = "'Plus Jakarta Sans', sans-serif";
  Chart.defaults.plugins.legend.labels.usePointStyle = true;
  Chart.defaults.plugins.tooltip.cornerRadius = 10;
  Chart.defaults.plugins.tooltip.padding      = 12;
  Chart.defaults.plugins.tooltip.boxPadding   = 4;
}

function tooltipTheme() {
  const dark = document.documentElement.getAttribute('data-theme') === 'dark';
  return {
    backgroundColor: dark ? '#1a1d2e' : '#ffffff',
    titleColor:      dark ? '#e8eaf0' : '#0f2354',
    bodyColor:       dark ? '#a0aab8' : '#4a5568',
    borderColor:     dark ? '#2a2d3e' : '#e2e8f0',
    borderWidth: 1,
  };
}

// ── Chart factories ───────────────────────────────────────────────────────
function makeBarChart(ctx, labels, data, colors, horizontal=false) {
  return new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets: [{ data, backgroundColor: colors||CHART_COLORS.slice(0,labels.length), borderRadius:6, borderSkipped:false, barPercentage:0.72, categoryPercentage:0.8 }] },
    options: {
      indexAxis: horizontal ? 'y' : 'x',
      responsive:true, maintainAspectRatio:true,
      plugins:{ legend:{display:false}, tooltip:{...tooltipTheme()} },
      scales:{
        x:{ grid:{display:horizontal,color:'rgba(0,0,0,.04)'}, ticks:{color:'#8898aa',font:{size:10},maxRotation:30}, border:{display:false} },
        y:{ grid:{display:!horizontal,color:'rgba(0,0,0,.04)'}, ticks:{color:'#8898aa',font:{size:10}}, border:{display:false}, beginAtZero:true }
      },
      layout:{ padding:{ top:8, right:8 } }
    }
  });
}

function makeLineChart(ctx, labels, data, color, fill=true) {
  return new Chart(ctx, {
    type:'line',
    data:{ labels, datasets:[{ data, borderColor:color||COLORS.coral, backgroundColor:fill?hexToRgba(color||COLORS.coral,.10):'transparent', borderWidth:2.5, pointRadius:3, pointHoverRadius:5, pointBackgroundColor:color||COLORS.coral, tension:.4, fill }] },
    options:{
      responsive:true, maintainAspectRatio:true,
      plugins:{ legend:{display:false}, tooltip:{...tooltipTheme()} },
      scales:{
        x:{ grid:{display:false}, ticks:{color:'#8898aa',font:{size:10},maxRotation:0}, border:{display:false} },
        y:{ grid:{color:'rgba(0,0,0,.04)'}, ticks:{color:'#8898aa',font:{size:10}}, border:{display:false}, beginAtZero:true }
      },
      layout:{ padding:{ top:8, right:8 } }
    }
  });
}

function makeDoughnutChart(ctx, labels, data, colors) {
  return new Chart(ctx, {
    type:'doughnut',
    data:{ labels, datasets:[{ data, backgroundColor:colors||CHART_COLORS.slice(0,labels.length), borderWidth:2, borderColor:'var(--card-bg)', hoverOffset:8 }] },
    options:{
      responsive:true, maintainAspectRatio:true, cutout:'60%',
      plugins:{
        legend:{position:'bottom',labels:{color:'#8898aa',font:{size:11},padding:12,usePointStyle:true,pointStyleWidth:8}},
        tooltip:{...tooltipTheme()}
      },
      layout:{ padding:8 }
    }
  });
}

function hexToRgba(hex, alpha) {
  const r=parseInt(hex.slice(1,3),16), g=parseInt(hex.slice(3,5),16), b=parseInt(hex.slice(5,7),16);
  return `rgba(${r},${g},${b},${alpha})`;
}

// ── localStorage helpers ──────────────────────────────────────────────────
function saveUploadData(d)  { try{localStorage.setItem('crashintel_data',JSON.stringify(d));}catch(e){} }
function loadUploadData()   { try{const d=localStorage.getItem('crashintel_data');return d?JSON.parse(d):null;}catch(e){return null;} }
function saveFileName(n)    { try{localStorage.setItem('crashintel_file',n);}catch(e){} }
function loadFileName()     { try{return localStorage.getItem('crashintel_file')||null;}catch(e){return null;} }
function saveSummary(d)     { try{localStorage.setItem('crashintel_summary',JSON.stringify(d));}catch(e){} }
function loadSummary()      { try{const d=localStorage.getItem('crashintel_summary');return d?JSON.parse(d):null;}catch(e){return null;} }
function saveMetrics(d)     { try{localStorage.setItem('crashintel_metrics',JSON.stringify(d));}catch(e){} }
function loadMetrics()      { try{const d=localStorage.getItem('crashintel_metrics');return d?JSON.parse(d):null;}catch(e){return null;} }

// ── Scroll Reveal Observer ────────────────────────────────────────────────
(function(){
  if(!window.IntersectionObserver)return;
  var io=new IntersectionObserver(function(entries){
    entries.forEach(function(e){
      if(e.isIntersecting){ e.target.classList.add('visible'); io.unobserve(e.target); }
    });
  },{threshold:0.12});
  function observe(){
    document.querySelectorAll('.sr,.sr-left,.sr-right,.sr-scale').forEach(function(el){io.observe(el);});
  }
  if(document.readyState==='loading'){ document.addEventListener('DOMContentLoaded',observe); }
  else{ observe(); setTimeout(observe,400); }
  document.addEventListener('DOMContentLoaded',function(){ setTimeout(observe,200); });
})();

// ── Navbar badge ──────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  const fname = loadFileName();
  const badge = document.getElementById('fileBadge');
  if (fname && badge) { badge.textContent='✓ '+fname; badge.style.display='inline-block'; }
});

// ── Export PDF ────────────────────────────────────────────────────────────
async function exportDashboardPDF() {
  const btn = document.getElementById('exportPdfBtn');
  if (btn) { btn.textContent='⏳ Generating...'; btn.disabled=true; }
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF({ orientation:'landscape', unit:'mm', format:'a4' });
  doc.setFont('helvetica','bold');
  doc.setFontSize(20); doc.setTextColor(14,35,84);
  doc.text('CrashIntel — Road Accident Analytics Report', 15, 18);
  doc.setFontSize(10); doc.setTextColor(136,152,170);
  doc.text('Generated: '+new Date().toLocaleString(), 15, 26);
  const charts = document.querySelectorAll('canvas');
  let y=35, x=15, col=0;
  const W=125, H=70;
  for (const canvas of charts) {
    try {
      const img = canvas.toDataURL('image/png');
      doc.addImage(img,'PNG',x,y,W,H);
      col++;
      if(col%2===0){y+=H+8;x=15;}else{x=148;}
      if(y>180){doc.addPage();y=15;x=15;col=0;}
    } catch(e){}
  }
  doc.save('crashintel_report.pdf');
  if (btn) { btn.textContent='📄 Export PDF'; btn.disabled=false; }
}

async function exportDashboardPNG() {
  const main = document.getElementById('dashboardMain');
  if (!main || typeof html2canvas === 'undefined') return;
  const canvas = await html2canvas(main, { scale:1.5, backgroundColor:null });
  const a = document.createElement('a');
  a.download='crashintel_dashboard.png'; a.href=canvas.toDataURL(); a.click();
}
