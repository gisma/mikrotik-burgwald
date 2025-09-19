(function () {
  function showTab(name) {
    const sections = document.querySelectorAll('section[id^="tab-"]');
    sections.forEach(sec => { sec.hidden = (sec.id !== `tab-${name}`); });

    document.querySelectorAll('.tabs button')
      .forEach(btn => btn.classList.toggle('active', btn.dataset.tab === name));

    postSize();
  }

  function postSize() {
    try {
      const h = Math.max(
        document.documentElement.scrollHeight,
        document.body ? document.body.scrollHeight : 0
      );
      parent.postMessage({ type: 'TTN_IFRAME_SIZE', height: h }, '*');
    } catch (e) {}
  }

  window.addEventListener('load', () => {
    document.querySelectorAll('.tabs button')
      .forEach(btn => btn.addEventListener('click', () => showTab(btn.dataset.tab)));
    showTab('overview');     // initial
    postSize();              // erste Höhenmeldung
  });

  // leicht gedrosseltes Resize
  let t;
  window.addEventListener('resize', () => {
    clearTimeout(t);
    t = setTimeout(postSize, 120);
  });
})();
