// Two ambient canvas layers behind every docs page, ported from the landing:
// an ascii ripple pinned to the top and a Bayer-dither field pinned to the
// bottom. Both are position:fixed backdrops, so they sit still while the docs
// scroll and never care which element owns the scroll.
(function () {
  var FIELDS = [
    { name: 'ripple', ramp: 5, draw: rippleDrawer },
    { name: 'dither', ramp: 3, draw: ditherDrawer }
  ];

  // main: resolve the shared colour ramp once, then mount each field.
  function main() {
    var probe = makeProbe();
    var dot = probe('--ascii-dot');
    FIELDS.forEach(function (f) {
      var colors = [];
      for (var i = 1; i <= f.ramp; i++) colors.push(probe('--ascii-' + i));
      if (colors.some(function (c) { return !c; }) || !dot) return;
      mount(f.name, f.draw(colors, dot));
    });
  }

  // Builds the wrapper + canvas, then hands the draw function a live loop.
  // Returns early on any browser that cannot give us a 2d context.
  function mount(name, drawFor) {
    var wrap = document.createElement('div');
    wrap.className = 'field field-' + name;
    wrap.setAttribute('aria-hidden', 'true');
    var cvs = document.createElement('canvas');
    wrap.appendChild(cvs);
    document.body.appendChild(wrap);
    if (!cvs.getContext) return;
    animate(wrap, cvs, drawFor(cvs.getContext('2d')));
  }

  // ── ascii ripple ─────────────────────────────────────────────────────
  // Concentric waves born at the top edge, sweeping down. Density is clamped
  // below TEXT_TOP so glyphs behind live text stay at or under --ascii-3.
  function rippleDrawer(COLORS, DOT) {
    var RAMP = ' .:-=+*xX#@';
    var FONT_PX = 12, LINE_H = 15, TEXT_TOP = 72, BAND = 90;
    var mono = getComputedStyle(document.documentElement)
      .getPropertyValue('--font-mono').trim();

    return function (ctx) {
      var cols = 0, rows = 0, cw = 0, W = 0, H = 0;

      return {
        measure: function (w, h) {
          W = w; H = h;
          ctx.font = FONT_PX + 'px ' + mono;
          ctx.textBaseline = 'top';
          cw = ctx.measureText('#').width || FONT_PX * 0.6;
          cols = Math.ceil(W / cw) + 1;
          rows = Math.ceil(H / LINE_H) + 1;
        },
        draw: function (t) {
          ctx.clearRect(0, 0, W, H);
          var sx = W * 0.5, sy = 0, sy2 = -H * 0.08;
          var FULL = RAMP.length + 2.0, COMP = 3.99;
          var last = null;

          for (var r = 0; r < rows; r++) {
            var y = r * LINE_H;
            var f = clamp01((y - (TEXT_TOP - BAND)) / BAND);
            var span = FULL + (COMP - FULL) * f;   // smooth, so there is no seam
            var pw = 1.5 + (1.0 - 1.5) * f;
            for (var c = 0; c < cols; c++) {
              var x = c * cw;
              var d = Math.hypot(x - sx, y - sy);
              var d2 = Math.hypot(x - sx, y - sy2);
              // Long wavelength so each crest reads as one broad arc across
              // the width. Both terms subtract t, so crests travel downward.
              var wave = Math.sin(d * 0.020 - t * 0.80) * 0.78
                       + Math.sin(d2 * 0.013 - t * 0.45) * 0.30;
              var v = (wave * 0.5 + 0.5) * Math.exp(-d / (W * 1.10));
              if (v < 0) v = 0;
              v = Math.pow(v, pw);

              var idx = Math.min(Math.floor(v * span), RAMP.length - 1);
              if (idx <= 0) {
                if (c % 4 === 0 && r % 2 === 0) {   // faint static lattice
                  if (last !== DOT) { ctx.fillStyle = DOT; last = DOT; }
                  ctx.fillText('.', x, y);
                }
                continue;
              }
              var col = COLORS[Math.min(idx - 1, COLORS.length - 1)];
              if (last !== col) { ctx.fillStyle = col; last = col; }
              ctx.fillText(RAMP.charAt(idx), x, y);
            }
          }
        }
      };
    };
  }

  // ── dithered squares ─────────────────────────────────────────────────
  // Ordered Bayer 4x4 over a density field bent into a U, so the page
  // dissolves into the accent ramp at the floor and up both margins.
  function ditherDrawer(COLORS) {
    var BAYER = [[0, 8, 2, 10], [12, 4, 14, 6], [3, 11, 1, 9], [15, 7, 13, 5]];
    var CELL = 9, SQ = 3.5;          // 15% coverage — squares, never a slab

    // max (not sum) of the floor and margin fields keeps the interior sparse
    function density(xn, yn, c, r, t) {
      var ex = Math.min(xn, 1 - xn) * 2;
      var base = Math.pow(yn, 1.35);
      var arm = Math.pow(1 - ex, 2.6) * (0.30 + 0.70 * yn);
      var v = Math.max(base, arm);
      v += Math.sin(c * 0.052 + t * 0.50) * 0.055
         + Math.sin(c * 0.023 - t * 0.31) * 0.045
         + Math.sin(c * 0.016 + r * 0.05 + t * 0.20) * 0.030;
      if (yn < 0.125) v *= yn / 0.125;   // no straight cut at the layer's top
      return v;
    }

    return function (ctx) {
      var cols = 0, rows = 0, W = 0, H = 0;

      return {
        measure: function (w, h) {
          W = w; H = h;
          cols = Math.ceil(W / CELL) + 1;
          rows = Math.ceil(H / CELL) + 1;
        },
        draw: function (t) {
          ctx.clearRect(0, 0, W, H);
          var inset = (CELL - SQ) / 2, last = null;
          var rdenom = Math.max(rows - 1, 1), cdenom = Math.max(cols - 1, 1);

          for (var r = 0; r < rows; r++) {
            var yn = r / rdenom;
            var y = r * CELL + inset;
            var brow = BAYER[r & 3];
            for (var c = 0; c < cols; c++) {
              var v = density(c / cdenom, yn, c, r, t);
              if (v <= 0) continue;
              if (v > 1) v = 1;
              if (v <= (brow[c & 3] + 0.5) / 16) continue;   // ordered threshold
              var col = COLORS[Math.min(Math.floor(v * COLORS.length), COLORS.length - 1)];
              if (last !== col) { ctx.fillStyle = col; last = col; }
              ctx.fillRect(c * CELL + inset, y, SQ, SQ);
            }
          }
        }
      };
    };
  }

  // ── shared plumbing ──────────────────────────────────────────────────

  // getPropertyValue() returns unresolved color-mix() text, which canvas
  // silently refuses. Round-tripping through a probe element forces a used
  // colour value. Returns '' when the token is missing.
  function makeProbe() {
    var el = document.createElement('span');
    el.setAttribute('aria-hidden', 'true');
    el.style.cssText = 'position:absolute;width:0;height:0;opacity:0;pointer-events:none';
    document.body.appendChild(el);
    return function (name) {
      el.style.color = 'currentColor';
      el.style.color = 'var(' + name + ')';
      var out = getComputedStyle(el).color;
      return (out && out !== 'currentColor') ? out : '';
    };
  }

  // Sizes the canvas to its wrapper, then runs a 30fps loop that pauses when
  // the tab is hidden. Under prefers-reduced-motion it paints one frame only.
  function animate(wrap, cvs, field) {
    var ctx = cvs.getContext('2d');
    var reduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    var t = 0, prev = 0, rafId = 0;

    function size() {
      var w = wrap.clientWidth, h = wrap.clientHeight;
      if (!w || !h) return false;
      var dpr = Math.min(window.devicePixelRatio || 1, 2);
      cvs.width = Math.floor(w * dpr);
      cvs.height = Math.floor(h * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      field.measure(w, h);
      return true;
    }
    function loop(now) {
      rafId = requestAnimationFrame(loop);
      if (now - prev < 1000 / 30) return;
      t += (now - prev) / 1000;
      prev = now;
      field.draw(t);
    }
    function start() {
      if (rafId || reduced) return;
      prev = performance.now();
      rafId = requestAnimationFrame(loop);
    }
    function stop() {
      if (rafId) cancelAnimationFrame(rafId);
      rafId = 0;
    }

    if (!size()) return;
    field.draw(0);
    if (reduced) return;
    start();
    document.addEventListener('visibilitychange', function () {
      document.hidden ? stop() : start();
    });
    var rt;
    window.addEventListener('resize', function () {
      clearTimeout(rt);
      rt = setTimeout(function () { if (size()) field.draw(t); }, 150);
    });
  }

  function clamp01(v) { return v < 0 ? 0 : v > 1 ? 1 : v; }

  main();
})();
