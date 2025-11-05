(function(){
  // Theme toggle
  const themeToggle = document.getElementById('themeToggle');
  const root = document.documentElement;
  const saved = localStorage.getItem('tr_theme');
  if (saved) root.setAttribute('data-theme', saved);
  themeToggle && themeToggle.addEventListener('click', ()=>{
    const cur = root.getAttribute('data-theme') || 'dark';
    const next = cur === 'dark' ? 'light' : 'dark';
    root.setAttribute('data-theme', next);
    localStorage.setItem('tr_theme', next);
  });

  // PWA install
  let deferredPrompt;
  const installBtn = document.getElementById('installBtn');
  window.addEventListener('beforeinstallprompt', (e)=>{
    e.preventDefault(); deferredPrompt = e; if (installBtn) installBtn.style.display='inline-flex';
  });
  installBtn && installBtn.addEventListener('click', async ()=>{
    if (!deferredPrompt) return;
    deferredPrompt.prompt();
    await deferredPrompt.userChoice;
    deferredPrompt = null;
    installBtn.style.display='none';
  });

  // Drag & drop + validation + progress
  const dz = document.getElementById('dropzone');
  const clientsHidden = document.getElementById('clientsInput');
  const txnsHidden = document.getElementById('txnsInput');
  const form = document.getElementById('uploadForm');
  const msg = document.getElementById('validationMsg');
  const bar = document.getElementById('bar');
  const progress = document.getElementById('progress');
  const sectorPreset = document.getElementById('sectorPreset');

  const pickFiles = () => {
    const pick = document.createElement('input');
    pick.type = 'file'; pick.accept = '.csv'; pick.multiple = true;
    pick.onchange = () => assignFiles(pick.files);
    pick.click();
  };
  const assignFiles = (files) => {
    const list = Array.from(files||[]);
    const clients = list.find(f => /client/i.test(f.name)) || list[0];
    const txns = list.find(f => /(txn|transact|payment)/i.test(f.name)) || list[1];
    if (clients){ const dt=new DataTransfer(); dt.items.add(clients); clientsHidden.files = dt.files; }
    if (txns){ const dt2=new DataTransfer(); dt2.items.add(txns); txnsHidden.files = dt2.files; }
    dz && dz.classList.add('dz-has-files');
    msg && (msg.textContent = (clients && txns) ? `Ready: ${clients?.name} + ${txns?.name}` : `Select both a Clients CSV and a Transactions CSV`);
  };
  dz && dz.addEventListener('click', pickFiles);
  dz && dz.addEventListener('dragover', (e)=>{ e.preventDefault(); dz.classList.add('dz-hover'); });
  dz && dz.addEventListener('dragleave', ()=> dz.classList.remove('dz-hover'));
  dz && dz.addEventListener('drop', (e)=>{ e.preventDefault(); dz.classList.remove('dz-hover'); assignFiles(e.dataTransfer.files); });

  form && form.addEventListener('submit', (e)=>{
    if (!clientsHidden.files.length || !txnsHidden.files.length){
      e.preventDefault(); msg.textContent='Please provide both CSV files.'; msg.style.color='#ffb020'; return;
    }
    // fake progress UI (server processes quickly anyway)
    progress.style.display='block';
    let n=0; const id=setInterval(()=>{ n = Math.min(100, n+5); bar.style.width = n+'%'; if(n>=100) clearInterval(id); }, 80);
    // Attach sector choice by renaming files (server reads CSV only; this is cosmetic)
    if (sectorPreset && sectorPreset.value){
      form.action = '/ingest?sector=' + encodeURIComponent(sectorPreset.value);
    }
  });

  // Results page helpers
  const search = document.getElementById('search');
  const bandFilter = document.getElementById('bandFilter');
  const table = document.getElementById('clientsTable');
  function filterRows(){
    if(!table) return;
    const q = (search?.value || '').toLowerCase();
    const band = bandFilter?.value || '';
    const rows = table.querySelectorAll('tbody tr');
    rows.forEach(r=>{
      const name = (r.cells[0]?.innerText || '').toLowerCase();
      const rowBand = r.getAttribute('data-band') || '';
      const match = (!q || name.includes(q)) && (!band || band===rowBand);
      r.style.display = match ? '' : 'none';
    });
  }
  search && search.addEventListener('input', filterRows);
  bandFilter && bandFilter.addEventListener('change', filterRows);

  // Copy share link
  const copyBtn = document.getElementById('copyBtn');
  const shareLink = document.getElementById('shareLink');
  const toast = document.getElementById('toast');
  function showToast(text){ if(!toast) return; toast.textContent=text; toast.style.display='block'; setTimeout(()=> toast.style.display='none', 1800); }
  copyBtn && copyBtn.addEventListener('click', async ()=>{
    const token = (shareLink?.innerText || '').trim();
    const url = location.origin + token;
    try{ await navigator.clipboard.writeText(url); showToast('Share link copied'); }catch{ showToast('Copy failed'); }
  });

  // Register SW for PWA
  if ('serviceWorker' in navigator){ navigator.serviceWorker.register('/sw.js').catch(()=>{}); }
})();
