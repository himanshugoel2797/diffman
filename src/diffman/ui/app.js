/* diffman UI — read-only viewer for pipeline forks, parameter diffs, and runs. */

const $ = (sel) => document.querySelector(sel);
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

const App = {
  current: null,
  ws: null,

  async init() {
    this.connectWS();
    await this.refresh();
    setInterval(() => this.refresh(), 7000);
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
    ws.onmessage = (m) => {
      try {
        const ev = JSON.parse(m.data);
        if (ev.type === 'run_changed') this.handleRunChanged(ev);
        else if (ev.type === 'pipelines_changed') this.refresh();
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

  async refresh() {
    try {
      const p = await jget('/api/pipelines');
      const r = await jget('/api/runs');
      //Build a pipeline.name → module lookup for run→pipeline navigation.
      this._pipelineToModule = {};
      const walk = n => {
        if (n.pipeline && n.module) this._pipelineToModule[n.pipeline] = n.module;
        (n.children || []).forEach(walk);
      };
      (p.forest || []).forEach(walk);
      this.allRuns = r.runs || [];
      this.renderForest(p.forest || []);
      this.renderRuns(this.allRuns);
    } catch (e) {
      console.error(e);
    }
  },

  // -- sidebar ----------------------------------------------------------

  renderForest(forest) {
    const ps = $('#modules');
    ps.innerHTML = '';
    if (forest.length === 0) {
      ps.appendChild(el('div', {class: 'hint', text: '(no pipelines discovered)'}));
      return;
    }
    for (const node of forest) ps.appendChild(this.renderForestNode(node, 0));
  },

  renderForestNode(node, depth) {
    const wrap = el('div', {class: 'fork-node', style: `padding-left:${depth*10}px`});
    if (node.error) {
      wrap.appendChild(el('span', {class: 'fork-err',
        title: node.error, text: `${node.module} (error)`}));
      return wrap;
    }
    const isActive = this._activeModule() === node.module;
    const a = el('a', {href: '#', text: node.pipeline,
      class: isActive ? 'active' : '',
      title: `module: ${node.module}` +
             (node.orphan_parent ? `  (parent ${node.orphan_parent} not found)` : ''),
      onclick: ev => { ev.preventDefault(); this.showPipeline(node.module); }});
    wrap.appendChild(a);
    wrap.appendChild(el('span', {class: 'fork-meta',
      text: ` · ${node.variant_count} var${node.variant_count===1?'':'s'}`}));
    if (node.orphan_parent) {
      wrap.appendChild(el('span', {class: 'fork-orphan',
        text: ` ⚠ parent ${node.orphan_parent} not found`}));
    }
    for (const c of (node.children || []))
      wrap.appendChild(this.renderForestNode(c, depth + 1));
    return wrap;
  },

  _activeModule() {
    //Best-effort: derive the active pipeline module from current view.
    const cur = this.current;
    if (!cur) return null;
    if (cur.kind === 'pipeline' || cur.kind === 'variant') return cur.module;
    if (cur.kind === 'run' || cur.kind === 'stage') {
      //Map run.pipeline (the Pipeline.name) back to module via our cache.
      return this._pipelineToModule ? this._pipelineToModule[cur.pipeline] : null;
    }
    return null;
  },

  renderRuns(runs) {
    const rs = $('#runs');
    rs.innerHTML = '';
    if (!runs || runs.length === 0) {
      rs.appendChild(el('div', {class: 'hint', text: '(no runs)'}));
      return;
    }
    //Filter dropdown by pipeline.
    const pipelines = Array.from(new Set(runs.map(r => r.pipeline))).sort();
    const sel = el('select', {class: 'run-filter'});
    sel.appendChild(el('option', {value: '', text: '(all pipelines)'}));
    for (const p of pipelines) sel.appendChild(el('option', {value: p, text: p}));
    sel.value = this._runFilter || '';
    sel.onchange = () => {
      this._runFilter = sel.value || null;
      this.renderRuns(this.allRuns);
    };
    rs.appendChild(sel);

    let filtered = this._runFilter
      ? runs.filter(r => r.pipeline === this._runFilter)
      : runs;
    //Sort by start time descending so the most recent shows at top.
    filtered = filtered.slice().sort((a, b) =>
      String(b.started || '').localeCompare(String(a.started || '')));
    const cur = this.current;
    const activeFp = cur && (cur.kind === 'run' || cur.kind === 'stage')
      ? `${cur.pipeline}/${cur.variant}/${cur.short_fp}` : null;
    for (const r of filtered) {
      const id = `${r.pipeline}/${r.variant}/${r.short_fp}`;
      rs.appendChild(el('a', {
        href: '#', class: id === activeFp ? 'active' : '',
        text: `${r.pipeline}/${r.variant} [${r.short_fp}]`,
        title: r.started || '',
        onclick: ev => { ev.preventDefault();
                         this.showRun(r.pipeline, r.variant, r.short_fp); }
      }));
    }
  },

  // -- pipeline view ----------------------------------------------------

  async showPipeline(module) {
    this.current = {kind: 'pipeline', module};
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: module}));

    let diff;
    try {
      diff = await jget('/api/diff?module=' + encodeURIComponent(module));
    } catch (e) {
      main.appendChild(el('pre', {text: 'load failed: ' + e}));
      return;
    }

    const actions = el('div', {class: 'row', style: 'margin:6px 0'});
    if (diff.parent) {
      actions.appendChild(el('button', {class: 'ghost', text: 'Source diff vs parent',
        onclick: () => this.showSourceDiff(module)}));
    }
    actions.appendChild(el('button', {class: 'ghost', text: 'Compare across pipelines',
      onclick: () => this.showComparePicker(module)}));
    main.appendChild(actions);

    if (diff.parent) {
      const link = el('a', {href: '#', text: diff.parent,
        onclick: ev => { ev.preventDefault();
                         this.showPipeline(diff.parent_module); }});
      main.appendChild(el('p', {class: 'hint'}, ['forked from ', link]));
      main.appendChild(this.renderDiffSummary(diff));
    } else {
      main.appendChild(el('p', {class: 'hint', text: 'root pipeline (no parent declared)'}));
    }

    const childVariants = diff.variants.filter(v => v.kind !== 'only_in_parent');
    main.appendChild(el('h3', {text: `Variants (${childVariants.length})`}));
    const list = el('div', {class: 'variant-list'});
    for (const v of childVariants) {
      list.appendChild(el('a', {href: '#', text: v.variant,
        onclick: ev => { ev.preventDefault(); this.showVariant(module, v.variant); }}));
    }
    main.appendChild(list);
    //Refresh the sidebar so the active-pipeline highlight follows us.
    this.refresh();
  },

  async showSourceDiff(module) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Source diff — ${module} vs parent`}));
    let d;
    try { d = await jget('/api/source_diff?module=' + encodeURIComponent(module)); }
    catch (e) { main.appendChild(el('pre', {text: 'failed: ' + e})); return; }
    if (!d.parent_module) {
      main.appendChild(el('p', {class: 'hint', text: 'no parent pipeline'}));
      return;
    }
    main.appendChild(el('p', {class: 'hint',
      text: `${d.parent_path} → ${d.child_path}`}));
    if (!d.diff.trim()) {
      main.appendChild(el('p', {class: 'hint', text: '(files are identical)'}));
      return;
    }
    main.appendChild(renderUnifiedDiff(d.diff));
  },

  async showComparePicker(module) {
    const all = (await jget('/api/pipelines')).forest;
    const flat = [];
    const walk = n => { if (n.module) flat.push(n.module); (n.children || []).forEach(walk); };
    all.forEach(walk);
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: 'Compare variant across pipelines'}));
    main.appendChild(el('p', {class: 'hint',
      text: 'Pick which pipelines to include, then enter a variant name.'}));
    const checks = {};
    const wrap = el('div', {class: 'variant-list'});
    for (const m of flat) {
      const cb = el('input', {type: 'checkbox', id: 'cmp-' + m});
      if (m === module) cb.checked = true;
      checks[m] = cb;
      wrap.appendChild(el('label', {class: 'cfg-key'}, [cb, ' ' + m]));
    }
    main.appendChild(wrap);
    const vIn = el('input', {type: 'text', placeholder: 'variant name', style: 'min-width:200px'});
    main.appendChild(el('div', {class: 'row', style: 'margin-top:8px'},
      [el('label', {text: 'variant: '}), vIn,
       el('button', {class: 'main', text: 'Compare',
         onclick: () => {
           const mods = Object.keys(checks).filter(m => checks[m].checked);
           if (mods.length < 2) { alert('pick at least 2'); return; }
           if (!vIn.value.trim()) { alert('variant name required'); return; }
           this.showCompare(mods, vIn.value.trim());
         }})]));
  },

  async showCompare(modules, variant) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Compare: ${variant}`}));
    let d;
    try {
      d = await jget('/api/compare?' +
        new URLSearchParams({modules: modules.join(','), variant}));
    } catch (e) { main.appendChild(el('pre', {text: 'failed: ' + e})); return; }
    const tbl = el('table', {class: 'compare-table'});
    const header = el('tr', {}, [el('th', {text: 'key'})]);
    for (const c of d.columns) {
      header.appendChild(el('th', {text: c.module,
        class: c.present ? '' : 'missing',
        title: c.present ? c.fingerprint.slice(0,12) : (c.error || 'missing')}));
    }
    tbl.appendChild(header);
    for (const row of d.rows) {
      const tr = el('tr', {class: row.equal ? '' : 'compare-differs'}, [
        el('td', {class: 'diff-key', text: row.path}),
      ]);
      for (const v of row.values) {
        tr.appendChild(el('td', {text: v === null ? '—' : fmtVal(v)}));
      }
      tbl.appendChild(tr);
    }
    main.appendChild(tbl);
  },

  renderDiffSummary(diff) {
    const box = el('div', {class: 'diff-box'});
    box.appendChild(el('h3', {text: 'Differences vs parent'}));
    if (!diff.variants.length) {
      box.appendChild(el('p', {class: 'hint',
        text: '(neither pipeline registered any variants)'}));
      return box;
    }
    for (const v of diff.variants) {
      const head = el('div', {class: 'diff-variant-head'}, [
        el('span', {class: 'diff-name', text: v.variant}),
        el('span', {class: 'diff-kind ' + v.kind, text: v.kind.replace(/_/g,' ')}),
      ]);
      if (v.parent_variant) {
        head.appendChild(el('span', {class: 'hint',
          text: ` (matched parent's ${v.parent_variant})`}));
      }
      if (v.forks_of_unresolved) {
        head.appendChild(el('span', {class: 'fork-orphan',
          text: ` ⚠ forks_of='${v.forks_of_unresolved}' not found in parent`}));
      }
      box.appendChild(head);
      if (v.kind === 'differs') {
        const tbl = el('table', {class: 'diff-table'});
        tbl.appendChild(el('tr', {}, [
          el('th', {text: 'key'}),
          el('th', {text: 'parent'}),
          el('th', {text: 'this'}),
        ]));
        for (const e of v.entries) {
          tbl.appendChild(el('tr', {class: 'diff-' + e.kind}, [
            el('td', {class: 'diff-key', text: e.path}),
            el('td', {text: e.kind === 'added' ? '—' : fmtVal(e.parent)}),
            el('td', {text: e.kind === 'removed' ? '—' : fmtVal(e.child)}),
          ]));
        }
        box.appendChild(tbl);
      }
    }
    return box;
  },

  // -- variant view -----------------------------------------------------

  async showVariant(module, variant) {
    this.current = {kind: 'variant', module, variant};
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `${module} / ${variant}`}));
    let d;
    try {
      d = await jget('/api/variant_overrides?' +
        new URLSearchParams({module, variant}));
    } catch (e) {
      main.appendChild(el('pre', {text: 'load failed: ' + e}));
      return;
    }
    if (d.base) {
      main.appendChild(el('p', {class: 'hint'}, ['inherits from ',
        el('a', {href: '#', text: d.base,
          onclick: ev => { ev.preventDefault(); this.showVariant(module, d.base); }})]));
      main.appendChild(el('h3', {text: `Overrides on top of ${d.base}`}));
      if (d.diff.length === 0) {
        main.appendChild(el('p', {class: 'hint',
          text: '(this variant adds nothing — identical to base)'}));
      } else {
        const tbl = el('table', {class: 'diff-table'});
        tbl.appendChild(el('tr', {}, [
          el('th', {text: 'key'}), el('th', {text: 'base'}), el('th', {text: 'this'}),
        ]));
        for (const e of d.diff) {
          tbl.appendChild(el('tr', {class: 'diff-' + e.kind}, [
            el('td', {class: 'diff-key', text: e.path}),
            el('td', {text: e.kind === 'added' ? '—' : fmtVal(e.parent)}),
            el('td', {text: e.kind === 'removed' ? '—' : fmtVal(e.child)}),
          ]));
        }
        main.appendChild(tbl);
      }
    } else {
      main.appendChild(el('p', {class: 'hint', text: 'root variant (no inheritance base)'}));
    }
    main.appendChild(el('h3', {text: 'Resolved config'}));
    main.appendChild(el('pre', {text: JSON.stringify(d.config, null, 2)}));
  },

  async find() {
    const q = $('#find-q').value.trim();
    if (q.length < 4) { alert('need at least 4 chars'); return; }
    let r;
    try { r = await jget('/api/find?q=' + encodeURIComponent(q)); }
    catch (e) { alert('find failed: ' + e); return; }
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `find: ${q}`}));
    main.appendChild(el('h3', {text: `Variants (${r.variants.length})`}));
    for (const v of r.variants) {
      main.appendChild(el('div', {}, [
        el('a', {href: '#', text: `${v.module} / ${v.variant}`,
          onclick: ev => { ev.preventDefault(); this.showVariant(v.module, v.variant); }}),
        el('span', {class: 'hint', text: '  ' + v.fingerprint.slice(0, 12)}),
      ]));
    }
    main.appendChild(el('h3', {text: `Runs (${r.runs.length})`}));
    for (const run of r.runs) {
      main.appendChild(el('div', {}, [
        el('a', {href: '#', text: `${run.pipeline}/${run.variant} [${run.short_fp}]`,
          onclick: ev => { ev.preventDefault();
            this.showRun(run.pipeline, run.variant, run.short_fp); }}),
        el('span', {class: 'hint', text: '  ' + (run.started || '')}),
      ]));
    }
  },

  // -- run / stage views (artifact previews) ---------------------------

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
    main.appendChild(el('h2', {text: `${pipeline} / ${variant}`, title: short_fp}));
    main.appendChild(el('p', {class: 'hint',
      text: `started ${d.run.started || '?'} → ${d.run.ended || '(running)'}`}));
    //Link back to the variant page (overrides view).
    const mod = (this._pipelineToModule || {})[pipeline];
    if (mod) {
      main.appendChild(el('p', {}, [
        el('a', {href: '#', text: `→ ${mod} / ${variant} (variant overrides)`,
          onclick: ev => { ev.preventDefault(); this.showVariant(mod, variant); }})
      ]));
    }

    const stagesDiv = el('div');
    for (const s of d.stages) {
      stagesDiv.appendChild(el('div', {class: 'stage-row'}, [
        el('span', {class: 'name', text: s.name}),
        el('span', {}, [el('span', {class: 'badge ' + (s.status || 'pending'),
                                    text: s.status || 'pending'})]),
        el('span', {}, [
          el('button', {class: 'ghost', text: `Inspect (${s.artifact_count})`,
                        onclick: () => this.showStage(pipeline, variant, short_fp, s.name)}),
        ]),
      ]));
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
        el('span', {class: 'meta', text: humanSize(a.size)}),
        el('a', {href: artifactHref(pipeline, variant, short_fp, a.path),
                 target: '_blank', text: 'download'}),
        el('button', {class: 'ghost', text: 'Diff vs…',
          onclick: () => this.diffArtifactPrompt(a)}),
      ]));
      const body = el('div', {class: 'body'});
      card.appendChild(body);
      main.appendChild(card);
      this.renderArtifact(body, a, {pipeline, variant, short_fp});
    }
  },

  async diffArtifactPrompt(artifact) {
    //Inline picker. We only let the user pick runs that aren't the
    //current one. Pipeline → variant → run cascading dropdowns.
    const runs = (await jget('/api/runs')).runs;
    if (!runs.length) { alert('no runs to diff against'); return; }
    const cur = this.current;
    const self_id = cur && `${cur.pipeline}/${cur.variant}/${cur.short_fp}`;
    const candidates = runs.filter(r =>
      `${r.pipeline}/${r.variant}/${r.short_fp}` !== self_id);

    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Diff artifact: ${artifact.path}`}));
    main.appendChild(el('p', {class: 'hint',
      text: `path_a (this run): ${artifact.absolute}`}));

    const pipelines = Array.from(new Set(candidates.map(r => r.pipeline))).sort();
    const pSel = el('select');
    pSel.appendChild(el('option', {value: '', text: '(pick pipeline)'}));
    for (const p of pipelines) pSel.appendChild(el('option', {value: p, text: p}));
    const vSel = el('select'); vSel.disabled = true;
    const rSel = el('select'); rSel.disabled = true;
    const go = el('button', {class: 'main', text: 'Diff', disabled: true});

    pSel.onchange = () => {
      vSel.innerHTML = ''; rSel.innerHTML = ''; rSel.disabled = true;
      go.disabled = true;
      if (!pSel.value) { vSel.disabled = true; return; }
      const variants = Array.from(new Set(
        candidates.filter(r => r.pipeline === pSel.value).map(r => r.variant))).sort();
      vSel.appendChild(el('option', {value: '', text: '(pick variant)'}));
      for (const v of variants) vSel.appendChild(el('option', {value: v, text: v}));
      vSel.disabled = false;
    };
    vSel.onchange = () => {
      rSel.innerHTML = ''; go.disabled = true;
      if (!vSel.value) { rSel.disabled = true; return; }
      const matching = candidates
        .filter(r => r.pipeline === pSel.value && r.variant === vSel.value)
        .slice().sort((a, b) =>
          String(b.started || '').localeCompare(String(a.started || '')));
      rSel.appendChild(el('option', {value: '', text: '(pick run)'}));
      for (const r of matching) {
        rSel.appendChild(el('option', {
          value: r.fdir, text: `${r.short_fp}  ${r.started || ''}`}));
      }
      rSel.disabled = false;
    };
    rSel.onchange = () => { go.disabled = !rSel.value; };

    go.onclick = async () => {
      const path_b = rSel.value + '/' + artifact.path;
      go.disabled = true;
      go.textContent = 'running…';
      try {
        const r = await jget('/api/artifact_diff?' + new URLSearchParams({
          path_a: artifact.absolute, path_b}));
        //Replace controls with result.
        const out = el('div');
        out.appendChild(el('p', {class: 'hint',
          text: `path_b: ${path_b}`}));
        this.renderArtifactDiff(out, r);
        controls.replaceWith(out);
      } catch (e) {
        go.disabled = false; go.textContent = 'Diff';
        alert('diff failed: ' + e);
      }
    };

    const controls = el('div', {class: 'row', style: 'margin:8px 0'},
      [el('label', {text: 'compare against: '}), pSel, vSel, rSel, go]);
    main.appendChild(controls);
  },

  renderArtifactDiff(main, r) {
    if (r.kind === 'error') {
      main.appendChild(el('pre', {text: 'error: ' + (r.note || JSON.stringify(r))}));
      return;
    }
    if (r.kind === 'text_diff') {
      if (!r.diff.trim()) {
        main.appendChild(el('p', {class: 'hint', text: '(identical)'}));
        return;
      }
      main.appendChild(renderUnifiedDiff(r.diff));
      return;
    }
    if (r.kind === 'json_diff') {
      if (r.entries && r.entries.length === 0) {
        main.appendChild(el('p', {class: 'hint', text: '(identical)'}));
        return;
      }
      const tbl = el('table', {class: 'diff-table'});
      tbl.appendChild(el('tr', {}, [
        el('th', {text: 'key'}), el('th', {text: 'a'}), el('th', {text: 'b'})]));
      for (const e of (r.entries || [])) {
        tbl.appendChild(el('tr', {class: 'diff-' + e.kind}, [
          el('td', {class: 'diff-key', text: e.path}),
          el('td', {text: e.kind === 'added' ? '—' : fmtVal(e.parent)}),
          el('td', {text: e.kind === 'removed' ? '—' : fmtVal(e.child)}),
        ]));
      }
      main.appendChild(tbl);
      return;
    }
    if (r.kind === 'array_diff') {
      main.appendChild(el('p', {class: 'hint',
        text: `shapes: ${r.shape_a.join('×')} vs ${r.shape_b.join('×')}  ·  dtypes: ${r.dtype_a} vs ${r.dtype_b}`}));
      if (r.note) main.appendChild(el('p', {class: 'hint', text: r.note}));
      if (r.stats) {
        const s = r.stats;
        const tbl = el('table', {class: 'diff-table'});
        for (const [k, v] of Object.entries(s)) {
          tbl.appendChild(el('tr', {}, [
            el('td', {class: 'diff-key', text: k}),
            el('td', {text: typeof v === 'number' ? fmt(v) : String(v)}),
          ]));
        }
        main.appendChild(tbl);
      }
      if (r.delta_heatmap) {
        const div = el('div', {class: 'plot'});
        main.appendChild(div);
        Plotly.newPlot(div, [{z: r.delta_heatmap, type: 'heatmap',
                              colorscale: 'RdBu', zmid: 0}],
          {margin: {t: 30, l: 40, r: 20, b: 30}, title: 'delta (b − a)'});
      }
      return;
    }
    main.appendChild(el('pre', {text: JSON.stringify(r, null, 2)}));
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
    container.innerHTML = '';
    const state = {path, repr: 'intensity', polarization: 'both',
                   energy_slice: -1, row: -1, col: -1, available: null};
    const controls = el('div', {class: 'row', style: 'margin-bottom:8px'});
    const reprSel = el('select', {});
    const polSel = el('select', {});
    polSel.appendChild(el('option', {value: 'both', text: 'Ex+Ey'}));
    polSel.appendChild(el('option', {value: 'Ex',   text: 'Ex only'}));
    polSel.appendChild(el('option', {value: 'Ey',   text: 'Ey only'}));
    const eIdx = el('input', {type: 'number', value: '-1', style: 'width:80px',
                              title: 'energy slice (-1 = sum/center)'});
    controls.appendChild(el('label', {}, ['repr: '])); controls.appendChild(reprSel);
    controls.appendChild(el('label', {}, [' polarization: '])); controls.appendChild(polSel);
    controls.appendChild(el('label', {}, [' E slice: '])); controls.appendChild(eIdx);
    container.appendChild(controls);
    const grid = el('div', {style:
      'display:grid;grid-template-columns:2fr 1fr;grid-template-rows:auto auto;gap:8px'});
    const heatDiv = el('div', {class: 'plot', style: 'grid-row:1/3'});
    const hcutDiv = el('div', {class: 'plot', style: 'min-height:180px'});
    const vcutDiv = el('div', {class: 'plot', style: 'min-height:180px'});
    grid.appendChild(heatDiv); grid.appendChild(hcutDiv); grid.appendChild(vcutDiv);
    container.appendChild(grid);
    const meta = el('div', {class: 'hint', style: 'margin-top:6px'});
    container.appendChild(meta);
    const fetchAndDraw = async () => {
      const q = new URLSearchParams({
        path, repr: state.repr, polarization: state.polarization,
        energy_slice: state.energy_slice, row: state.row, col: state.col});
      let payload;
      try { payload = await jget('/api/srw_preview?' + q); }
      catch (e) { meta.textContent = 'preview failed: ' + e; return; }
      if (payload.kind === 'error') { meta.textContent = 'preview error: ' + payload.data; return; }
      const m = payload.meta;
      if (state.available === null) {
        state.available = m.available;
        for (const r of m.available) reprSel.appendChild(el('option', {value: r, text: r}));
        reprSel.value = m.repr;
        polSel.value = m.polarization || 'both';
        eIdx.value = String(m.energy_slice ?? -1);
        const isWavefield = m.srw_kind === 'wavefield';
        polSel.disabled = !isWavefield; eIdx.disabled = !isWavefield;
      }
      const z = payload.data.z, cut = payload.data.cut, mesh = m.mesh;
      const xs = linspace(mesh.xStart, mesh.xFin, z[0].length);
      const ys = linspace(mesh.yStart, mesh.yFin, z.length);
      Plotly.react(heatDiv, [{z, x: xs, y: ys, type: 'heatmap', colorscale: 'Viridis'}], {
        margin: {t: 24, l: 50, r: 20, b: 40},
        xaxis: {title: 'x [m]'}, yaxis: {title: 'y [m]'},
        title: `${m.srw_kind} · ${m.repr}` +
               (m.downsampled && (m.downsampled[0] > 1 || m.downsampled[1] > 1)
                ? `  (downsampled ${m.downsampled.join('×')})` : ''),
        shapes: [
          {type:'line', x0:xs[cut.col], x1:xs[cut.col], y0:ys[0], y1:ys[ys.length-1], line:{color:'red', width:1}},
          {type:'line', y0:ys[cut.row], y1:ys[cut.row], x0:xs[0], x1:xs[xs.length-1], line:{color:'red', width:1}},
        ],
      });
      Plotly.react(hcutDiv, [{x: xs, y: cut.h, type: 'scatter', mode: 'lines', line: {color: '#c0392b'}}],
        {margin: {t: 20, l: 50, r: 20, b: 30}, title: `horizontal cut @ row=${cut.row}`, xaxis: {title: 'x [m]'}});
      Plotly.react(vcutDiv, [{x: ys, y: cut.v, type: 'scatter', mode: 'lines', line: {color: '#c0392b'}}],
        {margin: {t: 20, l: 50, r: 20, b: 30}, title: `vertical cut @ col=${cut.col}`, xaxis: {title: 'y [m]'}});
      meta.textContent = `mesh: ${mesh.nx}×${mesh.ny}  E=[${mesh.eStart}, ${mesh.eFin}] eV (ne=${mesh.ne})  ·  ${m.note || ''}`;
      heatDiv.removeAllListeners?.();
      heatDiv.on('plotly_click', ev => {
        const p = ev.points && ev.points[0]; if (!p) return;
        state.col = p.pointIndex[1]; state.row = p.pointIndex[0];
        fetchAndDraw();
      });
    };
    reprSel.onchange = () => { state.repr = reprSel.value; fetchAndDraw(); };
    polSel.onchange = () => { state.polarization = polSel.value; fetchAndDraw(); };
    eIdx.onchange = () => { state.energy_slice = parseInt(eIdx.value || '-1', 10); fetchAndDraw(); };
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
    wrap.appendChild(body); container.appendChild(wrap);
    if (payload.kind === 'plot_1d') {
      const div = el('div', {class: 'plot'}); body.appendChild(div);
      Plotly.newPlot(div, [{x: payload.data.x, y: payload.data.y, type: 'scatter', mode: 'lines'}],
        {margin: {t: 20, l: 50, r: 20, b: 40}});
    } else if (payload.kind === 'plot_2d') {
      const div = el('div', {class: 'plot'}); body.appendChild(div);
      Plotly.newPlot(div, [{z: payload.data, type: 'heatmap', colorscale: 'Viridis'}],
        {margin: {t: 20, l: 50, r: 20, b: 40}});
    } else if (payload.kind === 'scalar') {
      body.appendChild(el('pre', {text: String(payload.data)}));
    } else {
      body.appendChild(el('pre', {text: JSON.stringify(payload, null, 2)}));
    }
  },
};

function renderUnifiedDiff(text) {
  //Render a unified diff with per-line coloring. Simple line-classifier.
  const pre = document.createElement('pre');
  pre.className = 'unified-diff';
  for (const raw of text.split('\n')) {
    const span = document.createElement('span');
    span.textContent = raw + '\n';
    if (raw.startsWith('+++') || raw.startsWith('---')) span.className = 'ud-file';
    else if (raw.startsWith('@@')) span.className = 'ud-hunk';
    else if (raw.startsWith('+')) span.className = 'ud-add';
    else if (raw.startsWith('-')) span.className = 'ud-del';
    pre.appendChild(span);
  }
  return pre;
}

function humanSize(n) {
  if (n < 1024) return n + ' B';
  if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
  return (n / 1048576).toFixed(1) + ' MB';
}
function fmt(x) {
  return x == null ? '?' :
    (Math.abs(x) < 1e-3 || Math.abs(x) > 1e6 ? x.toExponential(3) : x.toPrecision(4));
}
function fmtVal(v) {
  if (v === null || v === undefined) return String(v);
  if (typeof v === 'number') return fmt(v);
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}
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
