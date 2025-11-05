// Drag & drop + validation
(function(){
  const dz = document.getElementById('dropzone');
  const clientsHidden = document.getElementById('clientsInput');
  const txnsHidden = document.getElementById('txnsInput');
  const form = document.getElementById('uploadForm');
  const msg = document.getElementById('validationMsg');

  if (!dz) return;

  const pickFiles = async () => {
    const pick = document.createElement('input');
    pick.type = 'file';
    pick.accept = '.csv';
    pick.multiple = true;
    pick.onchange = () => assignFiles(pick.files);
    pick.click();
  };

  const assignFiles = (files) => {
    // try to auto-map two CSVs based on simple filename hints
    const list = Array.from(files);
    const clients = list.find(f => /client/i.test(f.name)) || list[0];
    const txns = list.find(f => /(txn|transact|payment)/i.test(f.name)) || list[1];

    if (clients) {
      const dt = new DataTransfer(); dt.items.add(clients);
      clientsHidden.files = dt.files;
    }
    if (txns) {
      const dt2 = new DataTransfer(); if (txns) dt2.items.add(txns);
      txnsHidden.files = dt2.files;
    }
    dz.classList.add('dz-has-files');
    msg.textContent = (clients && txns)
      ? `Ready: ${clients?.name} + ${txns?.name}`
      : `Select both a Clients CSV and a Transactions CSV`;
  };

  dz.addEventListener('click', pickFiles);
  dz.addEventListener('dragover', (e)=>{ e.preventDefault(); dz.classList.add('dz-hover'); });
  dz.addEventListener('dragleave', ()=> dz.classList.remove('dz-hover'));
  dz.addEventListener('drop', (e)=>{
    e.preventDefault(); dz.classList.remove('dz-hover');
    assignFiles(e.dataTransfer.files);
  });

  form.addEventListener('submit', (e)=>{
    if (!clientsHidden.files.length || !txnsHidden.files.length) {
      e.preventDefault();
      msg.textContent = 'Please provide both CSV files.';
      msg.style.color = '#ffb020';
    }
  });
})();
