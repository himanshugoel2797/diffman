/* diffman UI — vanilla JS, Plotly for interactive previews. */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));
const el = (tag, props={}, kids=[]) => {
  const e = document.createElement(tag);
  for (const [k,v] of Object.entries(props)) {
    if (k === 'class') e.className = v;
    else if (k === 'onclick') e.onclick = v;
    else if (k === 'html') e.innerHTML = v;
    else if (k === 'text') e.textContent = v;
    else e.setAttribute(k, v);
  }
  for (const k of (Array.isArray(kids) ? kids : [kids])) {
    if (k == null) continue;
    e.appendChild(typeof k === 'string' ? document.createTextNode(k) : k);
  }
  return e;
};

async function jget(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}
async function jpost(url, body) {
  const r = await fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body || {}),
  });
  return r.json();
}

const SBATCH_LS_KEY = 'diffman.sbatch_flags';

const App = {
  current: null,         // {kind: 'module'|'run'|'stage', ...}
  ws: null,
  runs: [],
  submitter: null,       // {kind, accepts_sbatch_flags, default_sbatch_flags}

  async init() {
    this.connectWS();
    try { this.submitter = await jget('/api/submitter'); } catch (_) {}
    await this.refresh();
    setInterval(() => this.refresh(), 7000);
  },

  // Read/write the persisted sbatch flag string (browser localStorage).
  sbatchFlags() {
    return localStorage.getItem(SBATCH_LS_KEY) || '';
  },
  setSbatchFlags(s) {
    try { localStorage.setItem(SBATCH_LS_KEY, s); } catch (_) {}
  },

  // Build an sbatch-flags row to drop above a Launch button. Returns
  // null if the active submitter doesn't accept them (local).
  sbatchRow() {
    if (!this.submitter || !this.submitter.accepts_sbatch_flags) return null;
    const input = el('input', {
      type: 'text', id: 'sbatch-flags', class: 'sbatch-flags',
      placeholder: '--partition=regular --nodes=1 --time=01:00:00',
      value: this.sbatchFlags(),
    });
    input.oninput = () => this.setSbatchFlags(input.value);
    const defaults = (this.submitter.default_sbatch_flags || []).join(' ');
    const row = el('div', {class: 'sbatch-row'}, [
      el('label', {class: 'sbatch-label', text: 'sbatch flags:'}),
      input,
    ]);
    if (defaults) {
      row.appendChild(el('div', {class: 'hint sbatch-hint',
        text: `server defaults (always applied): ${defaults}`}));
    }
    return row;
  },

  // Pull the current sbatch flag string for inclusion in a launch payload.
  currentSbatchFlags() {
    const inp = document.getElementById('sbatch-flags');
    return inp ? inp.value : this.sbatchFlags();
  },

  connectWS() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${proto}//${location.host}/ws`);
    this.ws = ws;
    ws.onopen = () => $('#ws-status').classList.add('connected');
    ws.onclose = () => {
      $('#ws-status').classList.remove('connected');
      setTimeout(() => this.connectWS(), 2000);
    };
    ws.onerror = () => $('#ws-status').classList.add('error');
    ws.onmessage = (m) => {
      try {
        const ev = JSON.parse(m.data);
        if (ev.type === 'run_changed') this.handleRunChanged(ev);
        if (ev.type === 'launch') this.refresh();
      } catch (_) {}
    };
  },

  handleRunChanged(ev) {
    this.refresh();
    if (this.current && this.current.kind === 'run' &&
        this.current.pipeline === ev.pipeline &&
        this.current.variant === ev.variant &&
        this.current.short_fp === ev.fp) {
      this.showRun(ev.pipeline, ev.variant, ev.fp);
    } else if (this.current && this.current.kind === 'stage' &&
               this.current.pipeline === ev.pipeline &&
               this.current.variant === ev.variant &&
               this.current.short_fp === ev.fp) {
      this.showStage(ev.pipeline, ev.variant, ev.fp, this.current.stage);
    }
  },

  async scan() {
    const root = $('#scan-root').value;
    const q = root ? `?root=${encodeURIComponent(root)}` : '';
    await jget('/api/scan' + q);
    await this.refresh();
  },

  async refresh() {
    try {
      const m = await jget('/api/modules');
      const r = await jget('/api/runs');
      this.runs = r.runs || [];
      this.renderModules(m.modules || []);
      this.renderRuns(r.runs || []);
    } catch (e) {
      console.error(e);
    }
  },

  renderModules(modules) {
    const ps = $('#modules');
    ps.innerHTML = '';
    if (modules.length === 0) {
      ps.appendChild(el('div', {class: 'hint', text: '(no modules — click Scan)'}));
      return;
    }
    const byDir = {};
    for (const m of modules) (byDir[m.dir || '.'] = byDir[m.dir || '.'] || []).push(m);
    for (const dir of Object.keys(byDir).sort()) {
      ps.appendChild(el('div', {class: 'group', text: dir}));
      const wrap = el('div', {class: 'modgroup'});
      for (const m of byDir[dir]) {
        wrap.appendChild(el('a', {
          href: '#', title: m.path, text: m.module,
          onclick: ev => { ev.preventDefault(); this.showModule(m.module); }
        }));
      }
      ps.appendChild(wrap);
    }
  },

  renderRuns(runs) {
    const rs = $('#runs');
    rs.innerHTML = '';
    for (const r of runs) {
      rs.appendChild(el('a', {
        href: '#',
        text: `${r.pipeline}/${r.variant} [${r.short_fp}]`,
        onclick: ev => { ev.preventDefault();
                         this.showRun(r.pipeline, r.variant, r.short_fp); }
      }));
    }
  },

  async showModule(module) {
    this.current = {kind: 'module', module};
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: module}));
    let data;
    try {
      data = await jget('/api/variants?module=' + encodeURIComponent(module));
    } catch (e) {
      main.appendChild(el('pre', {text: 'Failed to load module: ' + e}));
      return;
    }

    const actions = el('div', {class: 'row', style: 'margin-bottom:8px'}, [
      el('button', {class: 'ghost', text: 'View / edit script',
        onclick: () => this.showScript(module)}),
      el('button', {class: 'ghost', text: 'Fork script',
        onclick: () => this.forkScriptPrompt(module)}),
    ]);
    main.appendChild(actions);

    if (data.variants.length === 0) {
      main.appendChild(el('p', {class: 'hint',
        text: '(this module registered no variants — check the dm.register() calls)'}));
    }
    const sb = this.sbatchRow();
    if (sb) main.appendChild(sb);
    const tbl = el('table', {class: 'kv'});
    for (const v of data.variants) {
      tbl.appendChild(el('tr', {}, [
        el('td', {class: 'k', text: v}),
        el('td', {}, [
          el('button', {class: 'main', text: 'Launch',
                        onclick: () => this.launch(module, v, [], null)}),
          el('button', {class: 'ghost', text: 'Configure & launch',
                        onclick: () => this.showLaunchForm(module, v)}),
          el('button', {class: 'ghost', text: 'Describe',
                        onclick: () => this.describe(module, v)}),
        ]),
      ]));
    }
    main.appendChild(tbl);
  },

  async showLaunchForm(module, variant) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Launch — ${module} / ${variant}`}));
    main.appendChild(el('p', {class: 'hint',
      text: 'Tweak any field below to launch with config overrides. ' +
            'Leaving everything unchanged is equivalent to a plain Launch. ' +
            'Each unique override set gets its own run directory.'}));

    let desc;
    try {
      desc = await jget('/api/describe?' +
        new URLSearchParams({module, variant}));
    } catch (e) {
      main.appendChild(el('pre', {text: 'describe failed: ' + e}));
      return;
    }

    const overrides = {};  // dotted-path -> {value, originalType}
    const formDiv = el('div', {class: 'cfg-form'});
    renderConfigEditor(formDiv, desc.config, '', overrides);
    main.appendChild(formDiv);

    const onlyInput = el('input', {type: 'text',
      placeholder: 'only stages (comma-separated, optional)',
      style: 'flex:1'});
    main.appendChild(el('div', {class: 'row', style: 'margin-top:8px'},
      [el('label', {text: 'only: '}), onlyInput]));

    const sb = this.sbatchRow();
    if (sb) main.appendChild(sb);

    main.appendChild(el('button', {class: 'main',
      text: 'Launch', style: 'margin-top:8px',
      onclick: () => {
        const ovr = [];
        for (const [path, info] of Object.entries(overrides)) {
          if (info.dirty) ovr.push(`${path}=${info.literal}`);
        }
        this.launch(module, variant, ovr, onlyInput.value.trim() || null);
      }}));
  },

  async describe(module, variant, overrides) {
    const params = new URLSearchParams({module, variant});
    for (const o of (overrides || [])) params.append('var', o);
    const d = await jget('/api/describe?' + params);
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `${module} / ${variant}`}));
    main.appendChild(el('p', {}, [
      'fingerprint: ', el('code', {text: d.fingerprint})
    ]));
    main.appendChild(el('pre', {text: JSON.stringify(d.config, null, 2)}));
  },

  async showScript(module) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Script — ${module}.py`}));
    let d;
    try { d = await jget('/api/script?module=' + encodeURIComponent(module)); }
    catch (e) {
      main.appendChild(el('pre', {text: 'load failed: ' + e}));
      return;
    }
    main.appendChild(el('p', {class: 'hint', text: d.path}));
    if (d.fork) {
      main.appendChild(el('p', {class: 'hint',
        text: `forked from ${d.fork.parent_module} on ${d.fork.created}`}));
    }
    const ta = el('textarea', {class: 'cfg-script-editor', spellcheck: 'false'});
    ta.value = d.source;
    main.appendChild(ta);
    const status = el('span', {class: 'hint', style: 'margin-left:8px'});
    main.appendChild(el('div', {class: 'row', style: 'margin-top:8px'}, [
      el('button', {class: 'main', text: 'Save',
        onclick: async () => {
          status.textContent = 'saving…';
          try {
            const r = await fetch('/api/script', {
              method: 'PUT',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({module, source: ta.value}),
            });
            const j = await r.json();
            if (!r.ok) throw new Error(j.detail || r.statusText);
            status.textContent = `saved ${j.bytes} bytes`;
          } catch (e) {
            status.textContent = 'save failed: ' + e;
          }
        }}),
      status,
    ]));
  },

  async forkScriptPrompt(parent_module) {
    const new_name = window.prompt(
      `Fork ${parent_module}.py — new module name (valid Python identifier):`,
      `${parent_module}_fork`);
    if (!new_name) return;
    try {
      const r = await fetch('/api/fork_script', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({parent_module, new_name: new_name.trim()}),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.detail || r.statusText);
      await this.refresh();
      this.showScript(j.module);
    } catch (e) {
      alert('fork failed: ' + e);
    }
  },

  async launch(module, variant, overrides, only) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: 'Launching…'}));
    const body = {module, variant, vars: overrides || []};
    if (only) body.only = only;
    const sbatch = this.currentSbatchFlags();
    if (sbatch && this.submitter && this.submitter.accepts_sbatch_flags) {
      body.sbatch_flags = sbatch;
      this.setSbatchFlags(sbatch);
    }
    const info = await jpost('/api/launch', body);
    main.appendChild(el('pre', {text: JSON.stringify(info, null, 2)}));
    setTimeout(() => this.refresh(), 1000);
  },

  async showRun(pipeline, variant, short_fp) {
    this.current = {kind: 'run', pipeline, variant, short_fp};
    let d;
    try {
      d = await jget(`/api/run/${encodeURIComponent(pipeline)}/${encodeURIComponent(variant)}/${encodeURIComponent(short_fp)}`);
    } catch (e) {
      $('#main').innerHTML = ''; $('#main').appendChild(el('pre', {text: String(e)}));
      return;
    }
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `${pipeline} / ${variant}`,
                               title: short_fp}));
    main.appendChild(el('p', {class: 'hint',
      text: `started ${d.run.started || '?'} → ${d.run.ended || '(running)'}`
    }));

    const stagesDiv = el('div');
    for (const s of d.stages) {
      const row = el('div', {class: 'stage-row'}, [
        el('span', {class: 'name', text: s.name}),
        el('span', {}, [el('span', {class: 'badge ' + (s.status || 'pending'),
                                    text: s.status || 'pending'})]),
        el('span', {}, [
          el('button', {class: 'ghost', text: `Inspect (${s.artifact_count})`,
                        onclick: () => this.showStage(pipeline, variant, short_fp, s.name)}),
        ]),
      ]);
      stagesDiv.appendChild(row);
    }
    main.appendChild(stagesDiv);

    main.appendChild(el('h3', {text: 'Config'}));
    main.appendChild(el('pre', {text: JSON.stringify(d.config, null, 2)}));
  },

  async showStage(pipeline, variant, short_fp, stage) {
    this.current = {kind: 'stage', pipeline, variant, short_fp, stage};
    let d;
    try {
      d = await jget(`/api/stage/${encodeURIComponent(pipeline)}/${encodeURIComponent(variant)}/${encodeURIComponent(short_fp)}/${encodeURIComponent(stage)}`);
    } catch (e) {
      $('#main').innerHTML = ''; $('#main').appendChild(el('pre', {text: String(e)}));
      return;
    }
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `${pipeline} / ${variant} / ${stage}`}));
    main.appendChild(el('p', {}, [
      'status: ', el('span', {class: 'badge ' + (d.status || 'pending'),
                              text: d.status || 'pending'}),
      ' ', el('code', {text: (d.key || '').slice(0,12)})
    ]));
    if (d.error) {
      main.appendChild(el('h3', {text: 'Error'}));
      main.appendChild(el('pre', {text: d.error}));
    }
    if (!d.artifacts.length) {
      main.appendChild(el('p', {class: 'hint', text: '(no artifacts yet)'}));
      return;
    }
    for (const a of d.artifacts) {
      const card = el('div', {class: 'artifact'});
      card.appendChild(el('div', {class: 'header'}, [
        el('span', {class: 'path', text: a.path}),
        el('span', {class: 'meta',
                    text: humanSize(a.size)}),
        el('a', {href: artifactHref(pipeline, variant, short_fp, a.path),
                 target: '_blank', text: 'download'}),
      ]));
      const body = el('div', {class: 'body'});
      card.appendChild(body);
      main.appendChild(card);
      this.renderArtifact(body, a, {pipeline, variant, short_fp});
    }
  },

  async renderArtifact(container, artifact, refs) {
    container.appendChild(el('p', {class: 'hint', text: 'loading preview…'}));
    let payload;
    try {
      payload = await jget('/api/render?path=' + encodeURIComponent(artifact.absolute));
    } catch (e) {
      container.innerHTML = '';
      container.appendChild(el('pre', {text: 'preview failed: ' + e}));
      return;
    }
    container.innerHTML = '';
    const k = payload.kind;
    const m = payload.meta || {};
    if (k === 'srw') {
      this.renderSRW(container, artifact.absolute);
    } else if (k === 'image') {
      const src = artifactHref(refs.pipeline, refs.variant, refs.short_fp, artifact.path);
      container.appendChild(el('img', {src: src + '?t=' + Date.now()}));
      if (m.width) container.appendChild(el('div', {class: 'hint',
        text: `${m.width}×${m.height} ${m.mode || ''}`}));
    } else if (k === 'text') {
      container.appendChild(el('pre', {text: payload.data + (m.truncated ? '\n…(truncated)' : '')}));
    } else if (k === 'json') {
      container.appendChild(el('pre', {text: JSON.stringify(payload.data, null, 2)}));
    } else if (k === 'plot_1d') {
      const div = el('div', {class: 'plot'});
      container.appendChild(div);
      Plotly.newPlot(div, [{x: payload.data.x, y: payload.data.y,
                            type: 'scatter', mode: 'lines'}],
                     {margin: {t: 20, l: 50, r: 20, b: 40}});
    } else if (k === 'plot_2d') {
      const div = el('div', {class: 'plot'});
      container.appendChild(div);
      Plotly.newPlot(div, [{z: payload.data, type: 'heatmap',
                            colorscale: 'Viridis'}],
                     {margin: {t: 20, l: 50, r: 20, b: 40},
                      title: m.stats ? `min=${fmt(m.stats.min)} max=${fmt(m.stats.max)} mean=${fmt(m.stats.mean)}` : ''});
    } else if (k === 'h5_tree') {
      const ul = el('ul');
      for (const ds of payload.data) {
        if (ds.kind === 'dataset') {
          const a = el('a', {href: '#',
            text: `${ds.name}  [${ds.shape.join('×')} ${ds.dtype}]`,
            onclick: ev => { ev.preventDefault();
              this.renderH5Dataset(container, artifact.absolute, ds.name); }});
          ul.appendChild(el('li', {}, [a]));
        } else {
          ul.appendChild(el('li', {class: 'hint', text: ds.name + '/'}));
        }
      }
      container.appendChild(ul);
    } else if (k === 'scalar') {
      container.appendChild(el('pre', {text: String(payload.data)}));
    } else if (k === 'binary') {
      container.appendChild(el('p', {class: 'hint',
        text: (m.note || 'binary') + ' — use download link'}));
    } else if (k === 'error') {
      container.appendChild(el('pre', {text: 'preview error: ' + payload.data}));
    } else {
      container.appendChild(el('pre', {text: JSON.stringify(payload, null, 2)}));
    }
  },

  async renderSRW(container, path) {
    // SRW-aware preview: repr selector, polarization toggle, heatmap + cuts.
    container.innerHTML = '';
    const state = {path, repr: 'intensity', polarization: 'both',
                   energy_slice: -1, row: -1, col: -1, available: null};

    // Controls bar
    const controls = el('div', {class: 'row', style: 'margin-bottom:8px'});
    const reprSel = el('select', {});
    const polSel = el('select', {});
    polSel.appendChild(el('option', {value: 'both', text: 'Ex+Ey'}));
    polSel.appendChild(el('option', {value: 'Ex',   text: 'Ex only'}));
    polSel.appendChild(el('option', {value: 'Ey',   text: 'Ey only'}));
    const eIdx = el('input', {type: 'number', value: '-1',
                              style: 'width:80px',
                              title: 'energy slice (-1 = sum/center)'});
    controls.appendChild(el('label', {}, ['repr: ']));
    controls.appendChild(reprSel);
    controls.appendChild(el('label', {}, [' polarization: ']));
    controls.appendChild(polSel);
    controls.appendChild(el('label', {}, [' E slice: ']));
    controls.appendChild(eIdx);
    container.appendChild(controls);

    // Plot containers
    const grid = el('div', {style:
      'display:grid;grid-template-columns:2fr 1fr;grid-template-rows:auto auto;gap:8px'});
    const heatDiv = el('div', {class: 'plot', style: 'grid-row:1/3'});
    const hcutDiv = el('div', {class: 'plot', style: 'min-height:180px'});
    const vcutDiv = el('div', {class: 'plot', style: 'min-height:180px'});
    grid.appendChild(heatDiv);
    grid.appendChild(hcutDiv);
    grid.appendChild(vcutDiv);
    container.appendChild(grid);

    const meta = el('div', {class: 'hint', style: 'margin-top:6px'});
    container.appendChild(meta);

    const fetchAndDraw = async () => {
      const q = new URLSearchParams({
        path, repr: state.repr,
        polarization: state.polarization,
        energy_slice: state.energy_slice,
        row: state.row, col: state.col,
      });
      let payload;
      try {
        payload = await jget('/api/srw_preview?' + q);
      } catch (e) {
        meta.textContent = 'preview failed: ' + e;
        return;
      }
      if (payload.kind === 'error') {
        meta.textContent = 'preview error: ' + payload.data;
        return;
      }
      const m = payload.meta;
      // Populate repr select once we know what's available
      if (state.available === null) {
        state.available = m.available;
        for (const r of m.available) {
          reprSel.appendChild(el('option', {value: r, text: r}));
        }
        reprSel.value = m.repr;
        polSel.value = m.polarization || 'both';
        eIdx.value = String(m.energy_slice ?? -1);
        const isWavefield = m.srw_kind === 'wavefield';
        polSel.disabled = !isWavefield;
        eIdx.disabled = !isWavefield;
      }
      const z = payload.data.z;
      const cut = payload.data.cut;
      const mesh = m.mesh;
      const xs = linspace(mesh.xStart, mesh.xFin, z[0].length);
      const ys = linspace(mesh.yStart, mesh.yFin, z.length);
      Plotly.react(heatDiv, [{z, x: xs, y: ys, type: 'heatmap',
                              colorscale: 'Viridis'}], {
        margin: {t: 24, l: 50, r: 20, b: 40},
        xaxis: {title: 'x [m]'}, yaxis: {title: 'y [m]'},
        title: `${m.srw_kind} · ${m.repr}` +
               (m.downsampled && (m.downsampled[0] > 1 || m.downsampled[1] > 1)
                ? `  (downsampled ${m.downsampled.join('×')})` : ''),
        shapes: [
          {type:'line', x0:xs[cut.col], x1:xs[cut.col],
           y0:ys[0], y1:ys[ys.length-1], line:{color:'red', width:1}},
          {type:'line', y0:ys[cut.row], y1:ys[cut.row],
           x0:xs[0], x1:xs[xs.length-1], line:{color:'red', width:1}},
        ],
      });
      Plotly.react(hcutDiv, [{x: xs, y: cut.h, type: 'scatter',
                              mode: 'lines', line: {color: '#c0392b'}}], {
        margin: {t: 20, l: 50, r: 20, b: 30},
        title: `horizontal cut @ row=${cut.row}`,
        xaxis: {title: 'x [m]'},
      });
      Plotly.react(vcutDiv, [{x: ys, y: cut.v, type: 'scatter',
                              mode: 'lines', line: {color: '#c0392b'}}], {
        margin: {t: 20, l: 50, r: 20, b: 30},
        title: `vertical cut @ col=${cut.col}`,
        xaxis: {title: 'y [m]'},
      });
      meta.textContent = `mesh: ${mesh.nx}×${mesh.ny}  E=[${mesh.eStart}, ${mesh.eFin}] eV (ne=${mesh.ne})  ·  ${m.note || ''}`;

      // Click on heatmap to move the cuts.
      heatDiv.removeAllListeners?.();
      heatDiv.on('plotly_click', ev => {
        const p = ev.points && ev.points[0];
        if (!p) return;
        state.col = p.pointIndex[1];
        state.row = p.pointIndex[0];
        fetchAndDraw();
      });
    };

    reprSel.onchange = () => { state.repr = reprSel.value; fetchAndDraw(); };
    polSel.onchange = () => { state.polarization = polSel.value; fetchAndDraw(); };
    eIdx.onchange = () => {
      state.energy_slice = parseInt(eIdx.value || '-1', 10);
      fetchAndDraw();
    };

    fetchAndDraw();
  },

  async renderH5Dataset(container, path, dataset) {
    const payload = await jget(`/api/render_dataset?path=${encodeURIComponent(path)}&dataset=${encodeURIComponent(dataset)}`);
    const wrap = el('div', {class: 'artifact'});
    wrap.appendChild(el('div', {class: 'header'}, [
      el('span', {class: 'path', text: dataset}),
      el('span', {class: 'meta',
                  text: payload.meta.shape ? `[${payload.meta.shape.join('×')} ${payload.meta.dtype}]` : ''}),
    ]));
    const body = el('div', {class: 'body'});
    wrap.appendChild(body);
    container.appendChild(wrap);
    if (payload.kind === 'plot_1d') {
      const div = el('div', {class: 'plot'});
      body.appendChild(div);
      Plotly.newPlot(div, [{x: payload.data.x, y: payload.data.y,
                            type: 'scatter', mode: 'lines'}],
                     {margin: {t: 20, l: 50, r: 20, b: 40}});
    } else if (payload.kind === 'plot_2d') {
      const div = el('div', {class: 'plot'});
      body.appendChild(div);
      Plotly.newPlot(div, [{z: payload.data, type: 'heatmap',
                            colorscale: 'Viridis'}],
                     {margin: {t: 20, l: 50, r: 20, b: 40}});
    } else if (payload.kind === 'scalar') {
      body.appendChild(el('pre', {text: String(payload.data)}));
    } else {
      body.appendChild(el('pre', {text: JSON.stringify(payload, null, 2)}));
    }
  },
};

// Render a typed editor for a (nested) config object. Each leaf gets an
// input matched to its current value type. Edits are recorded into
// `overrides` keyed by dotted path; `info.literal` is the Python-literal
// form expected by /api/launch (parsed by parse_value on the server).
function renderConfigEditor(container, cfg, prefix, overrides) {
  for (const [key, val] of Object.entries(cfg)) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
      const det = document.createElement('details');
      det.open = prefix === '';   // top-level expanded, nested collapsed
      const sum = document.createElement('summary');
      sum.textContent = key;
      det.appendChild(sum);
      const inner = el('div', {class: 'cfg-nested'});
      det.appendChild(inner);
      container.appendChild(det);
      renderConfigEditor(inner, val, path, overrides);
    } else {
      container.appendChild(renderConfigLeaf(path, key, val, overrides));
    }
  }
}

function renderConfigLeaf(path, key, val, overrides) {
  const row = el('div', {class: 'cfg-row'});
  row.appendChild(el('label', {class: 'cfg-key', text: key, title: path}));

  const type = val === null ? 'null'
    : typeof val === 'boolean' ? 'bool'
    : typeof val === 'number' ? 'number'
    : Array.isArray(val) ? 'list'
    : 'str';
  row.appendChild(el('span', {class: 'cfg-type', text: type}));

  let input;
  const record = (literal, dirty) => {
    overrides[path] = {literal, dirty};
  };
  if (type === 'bool') {
    input = el('input', {type: 'checkbox'});
    input.checked = val;
    input.onchange = () => record(input.checked ? 'true' : 'false',
                                  input.checked !== val);
  } else if (type === 'number') {
    input = el('input', {type: 'text', value: String(val)});
    input.oninput = () => {
      const t = input.value.trim();
      record(t, t !== String(val));
    };
  } else if (type === 'list') {
    input = el('input', {type: 'text', value: JSON.stringify(val)});
    input.oninput = () => {
      const t = input.value.trim();
      record(t, t !== JSON.stringify(val));
    };
  } else if (type === 'null') {
    input = el('input', {type: 'text', value: '', placeholder: 'None'});
    input.oninput = () => {
      const t = input.value.trim();
      record(t || 'None', t !== '');
    };
  } else {
    input = el('input', {type: 'text', value: String(val)});
    input.oninput = () => {
      const t = input.value;
      // quote so server parse_value treats it as a string literal,
      // not as a Python expression.
      record(JSON.stringify(t), t !== String(val));
    };
  }
  input.classList.add('cfg-input');
  row.appendChild(input);
  return row;
}

function humanSize(n) {
  if (n < 1024) return n + ' B';
  if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
  return (n / 1048576).toFixed(1) + ' MB';
}
function fmt(x) { return x == null ? '?' : (Math.abs(x) < 1e-3 || Math.abs(x) > 1e6 ? x.toExponential(3) : x.toPrecision(4)); }
function linspace(a, b, n) {
  if (n <= 1) return [a];
  const step = (b - a) / (n - 1);
  const out = new Array(n);
  for (let i = 0; i < n; i++) out[i] = a + i * step;
  return out;
}
function artifactHref(p, v, fp, rel) {
  return `/artifact/${encodeURIComponent(p)}/${encodeURIComponent(v)}/${encodeURIComponent(fp)}/${rel.split('/').map(encodeURIComponent).join('/')}`;
}

App.init();
window.App = App;
