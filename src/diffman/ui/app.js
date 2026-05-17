/* diffman UI — read-only viewer for pipeline forks, parameter diffs, and runs. */

const $ = (sel) => document.querySelector(sel);
const el = (tag, props={}, kids=[]) => {
  const e = document.createElement(tag);
  for (const [k,v] of Object.entries(props)) {
    if (v === null || v === undefined) continue;
    if (k === 'class') { if (v) e.className = v; }
    else if (k === 'onclick') e.onclick = v;
    else if (k === 'html') e.innerHTML = v;
    else if (k === 'text') e.textContent = v;
    else if (typeof v === 'boolean') { if (v) e.setAttribute(k, ''); }
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
    ws.onopen = () => {
      $('#ws-status').classList.add('connected');
      $('#ws-status').classList.remove('error');
    };
    ws.onerror = () => $('#ws-status').classList.add('error');
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
      const ch = await jget('/api/chains').catch(() => ({forest: []}));
      //Build a pipeline.name → module lookup for run→pipeline navigation.
      this._pipelineToModule = {};
      const walk = n => {
        if (n.pipeline && n.module) this._pipelineToModule[n.pipeline] = n.module;
        (n.children || []).forEach(walk);
      };
      (p.forest || []).forEach(walk);
      this.allRuns = r.runs || [];
      this.renderForest(p.forest || []);
      this.renderChainForest(ch.forest || []);
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

    //Runs in this pipeline (RunRecord.pipeline is the Pipeline.name, not
    //the module). `this.allRuns` is kept fresh by the periodic refresh.
    const pipeRuns = (this.allRuns || [])
      .filter(r => r.pipeline === diff.pipeline);
    const runsByVariant = {};
    for (const r of pipeRuns) {
      (runsByVariant[r.variant] = runsByVariant[r.variant] || []).push(r);
    }

    const childVariants = diff.variants.filter(v => v.kind !== 'only_in_parent');
    main.appendChild(el('h3', {text: `Variants (${childVariants.length})`}));
    const list = el('div', {class: 'variant-list'});
    for (const v of childVariants) {
      const n = (runsByVariant[v.variant] || []).length;
      const row = el('a', {href: '#',
        onclick: ev => { ev.preventDefault(); this.showVariant(module, v.variant); }},
        [v.variant,
         el('span', {class: 'hint', style: 'margin-left:8px',
           text: n ? `(${n} run${n === 1 ? '' : 's'})` : '(no runs)'})]);
      list.appendChild(row);
    }
    main.appendChild(list);

    //Recent runs section — flat list sorted by start time desc. Capped to
    //keep the page snappy for pipelines with hundreds of runs.
    const RUN_CAP = 25;
    main.appendChild(el('h3', {text: `Runs (${pipeRuns.length})`}));
    if (!pipeRuns.length) {
      main.appendChild(el('p', {class: 'hint', text: '(no runs yet)'}));
    } else {
      const sorted = pipeRuns.slice().sort((a, b) =>
        String(b.started || '').localeCompare(String(a.started || '')));
      const shown = sorted.slice(0, RUN_CAP);
      const tbl = el('table', {class: 'runs-table'});
      tbl.appendChild(el('tr', {}, [
        el('th', {text: 'variant'}), el('th', {text: 'run'}),
        el('th', {text: 'started'}), el('th', {text: 'status'}),
      ]));
      for (const r of shown) {
        const status = _runStatus(r);
        tbl.appendChild(el('tr', {}, [
          el('td', {}, [el('a', {href: '#', text: r.variant,
            onclick: ev => { ev.preventDefault();
              this.showVariant(module, r.variant); }})]),
          el('td', {}, [el('a', {href: '#', text: r.short_fp,
            title: r.fdir,
            onclick: ev => { ev.preventDefault();
              this.showRun(r.pipeline, r.variant, r.short_fp); }})]),
          el('td', {class: 'hint', text: r.started || ''}),
          el('td', {}, [el('span', {class: 'badge ' + status, text: status})]),
        ]));
      }
      main.appendChild(tbl);
      if (sorted.length > RUN_CAP) {
        main.appendChild(el('p', {class: 'hint',
          text: `(${sorted.length - RUN_CAP} older runs hidden — ` +
                `filter by pipeline in the sidebar to see all)`}));
      }
    }

    //Refresh the sidebar so the active-pipeline highlight follows us.
    this.refresh();
  },

  async showSourceDiff(module) {
    //Keep sidebar highlight on the pipeline we forked from.
    this.current = {kind: 'pipeline', module};
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Source diff — ${module} vs parent`}));
    main.appendChild(el('p', {}, [
      el('a', {href: '#', text: `← back to ${module}`,
        onclick: ev => { ev.preventDefault(); this.showPipeline(module); }}),
    ]));
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
    //Best-effort back link: drop the user at the first module's pipeline.
    if (modules.length) {
      const m0 = modules[0];
      main.appendChild(el('p', {}, [
        el('a', {href: '#', text: `← back to ${m0}`,
          onclick: ev => { ev.preventDefault(); this.showPipeline(m0); }}),
      ]));
    }
    let d;
    try {
      d = await jget('/api/compare?' +
        new URLSearchParams({modules: modules.join(','), variant}));
    } catch (e) { main.appendChild(el('pre', {text: 'failed: ' + e})); return; }
    if (!d.rows.length && d.columns.every(c => !c.present)) {
      main.appendChild(el('p', {class: 'hint',
        text: `(no module registered a variant named '${variant}')`}));
      return;
    }
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
    main.appendChild(el('p', {}, [
      el('a', {href: '#', text: `← back to ${module}`,
        onclick: ev => { ev.preventDefault(); this.showPipeline(module); }}),
    ]));
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

  // -- chain sidebar + page ---------------------------------------------

  renderChainForest(forest) {
    const cs = $('#chains');
    cs.innerHTML = '';
    if (forest.length === 0) {
      cs.appendChild(el('div', {class: 'hint', text: '(no chains discovered)'}));
      return;
    }
    for (const node of forest) cs.appendChild(this.renderChainNode(node, 0));
  },

  renderChainNode(node, depth) {
    const wrap = el('div', {class: 'fork-node', style: `padding-left:${depth*10}px`});
    const isActive = this.current && this.current.kind === 'chain'
                     && this.current.name === node.name;
    const a = el('a', {href: '#', text: node.name,
      class: isActive ? 'active' : '',
      title: `module: ${node.module || '?'}` +
             (node.orphan_parent ? `  (parent ${node.orphan_parent} not found)` : ''),
      onclick: ev => { ev.preventDefault(); this.showChain(node.name); }});
    wrap.appendChild(a);
    wrap.appendChild(el('span', {class: 'fork-meta',
      text: ` · ${node.step_count} step${node.step_count===1?'':'s'}` +
            ` · ${node.variation_count} var${node.variation_count===1?'':'s'}`}));
    if (node.orphan_parent) {
      wrap.appendChild(el('span', {class: 'fork-orphan',
        text: ` ⚠ parent ${node.orphan_parent} not found`}));
    }
    for (const c of (node.children || []))
      wrap.appendChild(this.renderChainNode(c, depth + 1));
    return wrap;
  },

  async showChain(name, variationName) {
    this.current = {kind: 'chain', name, variation: variationName || null};
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `chain: ${name}`}));
    let d;
    try { d = await jget('/api/chain/' + encodeURIComponent(name)); }
    catch (e) { main.appendChild(el('pre', {text: 'load failed: ' + e})); return; }

    //Action bar: source diff vs parent, scoreboard, variation compare.
    const actions = el('div', {class: 'row', style: 'margin:6px 0'});
    if (d.parent) {
      actions.appendChild(el('button', {class: 'ghost',
        text: 'Source diff vs parent',
        onclick: () => this.showChainSourceDiff(name)}));
      actions.appendChild(el('button', {class: 'ghost',
        text: 'Variation diff vs parent',
        onclick: () => this.showChainDiff(name)}));
    }
    actions.appendChild(el('button', {class: 'ghost', text: 'Scoreboard',
      onclick: () => this.showScoreboard(name)}));
    actions.appendChild(el('button', {class: 'ghost',
      text: 'Compare variations',
      onclick: () => this.showChainVariationPicker(name, d.variations)}));
    main.appendChild(actions);

    if (d.parent) {
      main.appendChild(el('p', {class: 'hint'}, ['forked from ',
        el('a', {href: '#', text: d.parent,
          onclick: ev => { ev.preventDefault(); this.showChain(d.parent); }})]));
    }

    //Variation selector.
    main.appendChild(el('h3', {text: `Variations (${d.variations.length})`}));
    const variation = variationName ||
      (d.variations[0] && d.variations[0].name) || null;
    const sel = el('select', {class: 'run-filter',
      style: 'max-width:280px;margin-bottom:8px'});
    for (const v of d.variations) {
      sel.appendChild(el('option', {value: v.name, text: v.name}));
    }
    if (variation) sel.value = variation;
    sel.onchange = () => this.showChain(name, sel.value);
    main.appendChild(sel);

    //Variation tree — show base= inheritance for this chain's variations.
    main.appendChild(this.renderVariationTree(d.variations));

    if (!variation) {
      main.appendChild(el('p', {class: 'hint',
        text: '(this chain has no variations defined yet)'}));
      return;
    }

    //DAG view for the selected variation.
    let prog;
    try {
      prog = await jget(`/api/chain_progress/${encodeURIComponent(name)}/${encodeURIComponent(variation)}`);
    } catch (e) {
      main.appendChild(el('pre', {text: 'progress failed: ' + e})); return;
    }
    main.appendChild(el('h3', {text: `Pipeline (variation: ${variation})`}));
    main.appendChild(this.renderChainDAG(prog));
    this.refresh();
  },

  renderVariationTree(variations) {
    //Group variations by their base= so we can show the inheritance forest.
    const wrap = el('div', {class: 'diff-box', style: 'margin-bottom:10px'});
    wrap.appendChild(el('h3', {text: 'Variation inheritance', style: 'margin-top:0'}));
    const byBase = {};
    for (const v of variations) {
      const b = v.base || '';
      (byBase[b] = byBase[b] || []).push(v);
    }
    const seen = new Set();
    const renderNode = (v, depth) => {
      const node = el('div', {style: `padding-left:${depth*16}px; font-family:ui-monospace,monospace; font-size:12px`});
      const mapping = v.mapping || {};
      const summary = Object.entries(mapping)
        .map(([k, val]) => Array.isArray(val)
          ? `${k}=[${val.join('|')}]`   //fan-out: pipe-separated for readability
          : `${k}=${val}`).join('  ');
      node.appendChild(el('span', {text: v.name}));
      if (v.base) {
        node.appendChild(el('span', {class: 'hint', text: ` ← ${v.base}`}));
      }
      node.appendChild(el('span', {class: 'hint',
        style: 'margin-left:8px', text: summary}));
      if (v.error) {
        node.appendChild(el('span', {class: 'fork-orphan',
          text: ' ⚠ ' + v.error}));
      }
      seen.add(v.name);
      wrap.appendChild(node);
      for (const child of (byBase[v.name] || [])) renderNode(child, depth + 1);
    };
    for (const root of (byBase[''] || [])) renderNode(root, 0);
    //Orphans (base= refers to something we don't know about).
    for (const v of variations) {
      if (seen.has(v.name)) continue;
      renderNode(v, 0);
    }
    return wrap;
  },

  renderChainDAG(prog) {
    //Tree-style chain view: each chain step is a grid column, each
    //branch (= "lane") is a grid row. Non-fan-out steps render one
    //card spanning all lanes; fan-out steps render one card per lane.
    //Between columns sits a connector cell whose SVG depends on the
    //adjacent steps' branch shapes (1→1, 1→N, N→N, N→1), making the
    //fan-out visually unambiguous. Failed branches surface inline below.
    const branchesOf = s => (s.branches && s.branches.length)
      ? s.branches
      : [{branch_key: null, variant: s.variant, status: s.status,
          short_fp: s.short_fp, fingerprint: s.fingerprint,
          stage_status: s.stage_status, errors: s.errors}];

    //Lanes: the set of branch_keys this variation introduces. If only
    //null appears, the variation isn't fan-out and the tree collapses
    //to a single-lane row of cards (visually identical to the prior
    //flat layout for back-compat).
    const keySet = new Set();
    prog.steps.forEach(s => branchesOf(s).forEach(b => keySet.add(b.branch_key)));
    const namedLanes = [...keySet].filter(k => k !== null).sort();
    const lanes = namedLanes.length ? namedLanes : [null];
    const nLanes = lanes.length;
    const laneIdx = k => lanes.indexOf(k);

    const stepIsFanned = s => {
      const bs = branchesOf(s);
      return bs.length > 1 || bs[0].branch_key !== null;
    };

    const wrap = el('div', {class: 'chain-tree-wrap'});
    const grid = el('div', {class: 'chain-tree-grid'});
    //Alternating column template: [step][conn][step][conn]...[step].
    //Connector columns are narrow and fixed so the SVG geometry stays
    //predictable when the surrounding cards wrap or grow.
    const colTemplate = [];
    prog.steps.forEach((_, i) => {
      colTemplate.push('minmax(170px, max-content)');
      if (i < prog.steps.length - 1) colTemplate.push('50px');
    });
    grid.style.gridTemplateColumns = colTemplate.join(' ');
    //Header row + N lane rows. minmax keeps lanes aligned across cols
    //even when cards in different columns have different content sizes.
    grid.style.gridTemplateRows = `auto repeat(${nLanes}, minmax(64px, auto))`;

    prog.steps.forEach((s, i) => {
      const stepCol = i * 2 + 1;
      const fanned = stepIsFanned(s);

      //Per-step header above the column.
      const hdr = el('div', {class: 'chain-tree-header'});
      hdr.appendChild(el('div', {class: 'chain-tree-header-name', text: s.name}));
      hdr.appendChild(el('div', {class: 'chain-tree-header-pipe', text: s.pipeline}));
      if (s.consumes && s.consumes.length) {
        hdr.appendChild(el('div', {class: 'chain-tree-header-meta',
          text: '← ' + s.consumes.join(', ')}));
      }
      hdr.style.gridColumn = stepCol;
      hdr.style.gridRow = 1;
      grid.appendChild(hdr);

      //Step body: one spanning card or one per lane.
      const branches = branchesOf(s);
      if (!fanned) {
        const card = this.renderTreeCard(s, branches[0], false);
        card.style.gridColumn = stepCol;
        card.style.gridRow = `2 / span ${nLanes}`;
        grid.appendChild(card);
      } else {
        branches.forEach(b => {
          const card = this.renderTreeCard(s, b, true);
          card.style.gridColumn = stepCol;
          card.style.gridRow = `${laneIdx(b.branch_key) + 2}`;
          grid.appendChild(card);
        });
      }

      //Connector to the next step.
      if (i < prog.steps.length - 1) {
        const next = prog.steps[i + 1];
        const conn = this.renderTreeConnector(
          fanned, stepIsFanned(next), nLanes);
        conn.style.gridColumn = stepCol + 1;
        conn.style.gridRow = `2 / span ${nLanes}`;
        grid.appendChild(conn);
      }
    });

    wrap.appendChild(grid);

    //Inline per-branch error panels — fan-out variations regularly
    //have one branch fail while siblings succeed, so each gets its own.
    prog.steps.forEach(s => {
      branchesOf(s).forEach(b => {
        if (b.status !== 'failed') return;
        const label = b.branch_key !== null
          ? `${s.name}[${b.branch_key}]` : s.name;
        const errBox = el('div', {class: 'chain-step-error'});
        errBox.appendChild(el('h4', {text: `error in ${label}`}));
        for (const [stage, tb] of Object.entries(b.errors || {})) {
          errBox.appendChild(el('p', {class: 'hint', text: `stage: ${stage}`}));
          errBox.appendChild(el('pre', {text: tb}));
        }
        wrap.appendChild(errBox);
      });
    });
    return wrap;
  },

  renderTreeCard(step, b, isFanned) {
    const status = b.status || 'pending';
    const card = el('div', {class: 'chain-tree-card ' + status});

    const head = el('div', {class: 'chain-tree-card-head'});
    head.appendChild(el('span', {class: 'badge ' + status, text: status}));
    //Branch key as the primary identifier for fanned cards; for
    //single-branch cards the variant takes that role.
    const primary = isFanned && b.branch_key !== null
      ? b.branch_key : (b.variant || '?');
    head.appendChild(el('span', {class: 'chain-tree-card-key', text: primary}));
    card.appendChild(head);

    //When the branch was inherited from a fanned upstream (branch_key !=
    //variant), surface the underlying variant so it's not hidden — the
    //user needs to see e.g. that recon_analysis[aps_2mode] ran the
    //'default' variant.
    if (isFanned && b.branch_key !== null && b.variant
        && b.variant !== b.branch_key) {
      card.appendChild(el('div', {class: 'chain-tree-card-sub',
        text: `variant: ${b.variant}`}));
    }

    if (b.short_fp) {
      card.appendChild(el('div', {class: 'chain-tree-card-meta'}, [
        el('a', {href: '#', text: `[${b.short_fp}]`,
          onclick: ev => { ev.preventDefault();
            this.showRun(step.pipeline, b.variant, b.short_fp); }})
      ]));
    } else {
      card.appendChild(el('div', {class: 'chain-tree-card-meta hint',
        text: status === 'pending' ? '(no run yet)' : ''}));
    }
    return card;
  },

  renderTreeConnector(fromMulti, toMulti, nLanes) {
    //SVG connector with preserveAspectRatio="none" so lines stretch
    //smoothly to whatever the surrounding grid produces. The viewBox
    //uses 100 units per lane vertically so the arithmetic below stays
    //the same shape regardless of how the cells actually size.
    const SVG_NS = 'http://www.w3.org/2000/svg';
    const W = 50;
    const LH = 100;
    const H = Math.max(1, nLanes) * LH;
    const svg = document.createElementNS(SVG_NS, 'svg');
    svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
    svg.setAttribute('preserveAspectRatio', 'none');
    svg.setAttribute('class', 'chain-tree-svg');
    const laneY = i => i * LH + LH / 2;
    const line = (x1, y1, x2, y2) => {
      const l = document.createElementNS(SVG_NS, 'line');
      l.setAttribute('x1', x1); l.setAttribute('y1', y1);
      l.setAttribute('x2', x2); l.setAttribute('y2', y2);
      return l;
    };
    if (!fromMulti && !toMulti) {
      svg.appendChild(line(0, H / 2, W, H / 2));
    } else if (!fromMulti && toMulti) {
      //1→N: stub from prev, vertical trunk, one branch per lane.
      svg.appendChild(line(0, H / 2, W / 2, H / 2));
      svg.appendChild(line(W / 2, laneY(0), W / 2, laneY(nLanes - 1)));
      for (let i = 0; i < nLanes; i++) {
        svg.appendChild(line(W / 2, laneY(i), W, laneY(i)));
      }
    } else if (fromMulti && toMulti) {
      //N→N: parallel arrows, one per lane.
      for (let i = 0; i < nLanes; i++) {
        svg.appendChild(line(0, laneY(i), W, laneY(i)));
      }
    } else {
      //N→1: merge mirror of fan-out (defensive — current chains don't
      //merge, but a downstream consuming multiple branched upstreams
      //via a non-fanned variant would land here).
      for (let i = 0; i < nLanes; i++) {
        svg.appendChild(line(0, laneY(i), W / 2, laneY(i)));
      }
      svg.appendChild(line(W / 2, laneY(0), W / 2, laneY(nLanes - 1)));
      svg.appendChild(line(W / 2, H / 2, W, H / 2));
    }
    const cell = el('div', {class: 'chain-tree-conn'});
    cell.appendChild(svg);
    return cell;
  },

  async showChainDiff(name) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Variation diff — ${name} vs parent chain`}));
    let d;
    try { d = await jget('/api/chain_diff?chain=' + encodeURIComponent(name)); }
    catch (e) { main.appendChild(el('pre', {text: 'failed: ' + e})); return; }
    if (!d.parent) {
      main.appendChild(el('p', {class: 'hint', text: 'no parent chain'}));
      return;
    }
    main.appendChild(el('p', {class: 'hint'}, ['parent: ',
      el('a', {href: '#', text: d.parent,
        onclick: ev => { ev.preventDefault(); this.showChain(d.parent); }})]));
    for (const v of d.variations) {
      const box = el('div', {class: 'diff-box'});
      box.appendChild(el('div', {class: 'diff-variant-head'}, [
        el('span', {class: 'diff-name', text: v.variation}),
        el('span', {class: 'diff-kind ' + v.kind,
          text: v.kind.replace(/_/g, ' ')}),
      ]));
      if (v.parent_variation) {
        box.appendChild(el('p', {class: 'hint',
          text: `matched parent's ${v.parent_variation}`}));
      }
      if (v.forks_of_unresolved) {
        box.appendChild(el('p', {class: 'fork-orphan',
          text: `forks_of='${v.forks_of_unresolved}' not in parent`}));
      }
      if (v.kind === 'differs' && v.steps) {
        const tbl = el('table', {class: 'diff-table'});
        tbl.appendChild(el('tr', {}, [el('th', {text: 'step'}),
          el('th', {text: 'parent variant'}), el('th', {text: 'this variant'})]));
        for (const s of v.steps) {
          tbl.appendChild(el('tr', {class: 'diff-changed'}, [
            el('td', {class: 'diff-key', text: s.step}),
            el('td', {text: s.parent || '—'}),
            el('td', {text: s.child || '—'})]));
        }
        box.appendChild(tbl);
      }
      main.appendChild(box);
    }
  },

  async showChainSourceDiff(name) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Chain source diff — ${name} vs parent`}));
    let d;
    try { d = await jget('/api/chain_source_diff?chain=' + encodeURIComponent(name)); }
    catch (e) { main.appendChild(el('pre', {text: 'failed: ' + e})); return; }
    if (!d.parent) {
      main.appendChild(el('p', {class: 'hint', text: 'no parent chain'}));
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

  showChainVariationPicker(chainName, variations) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Compare variations — ${chainName}`}));
    main.appendChild(el('p', {class: 'hint',
      text: 'Pick ≥2 variations to compare per-step parameter differences.'}));
    const checks = {};
    const wrap = el('div', {class: 'variant-list'});
    for (const v of variations) {
      const cb = el('input', {type: 'checkbox', id: 'cv-' + v.name});
      checks[v.name] = cb;
      wrap.appendChild(el('label', {class: 'cfg-key'}, [cb, ' ' + v.name]));
    }
    main.appendChild(wrap);
    main.appendChild(el('div', {class: 'row', style: 'margin-top:8px'}, [
      el('button', {class: 'main', text: 'Compare',
        onclick: () => {
          const picked = Object.keys(checks).filter(n => checks[n].checked);
          if (picked.length < 2) { alert('pick at least 2'); return; }
          this.showChainVariationDiff(chainName, picked);
        }}),
      el('button', {class: 'ghost', text: 'Back to chain',
        onclick: () => this.showChain(chainName)}),
    ]));
  },

  async showChainVariationDiff(chainName, variations) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2',
      {text: `Compare variations — ${chainName}: ${variations.join(' / ')}`}));
    let d;
    try {
      d = await jget('/api/chain_variation_diff?' + new URLSearchParams({
        chain: chainName, variations: variations.join(',')}));
    } catch (e) {
      main.appendChild(el('pre', {text: 'failed: ' + e})); return;
    }
    for (const step of d.steps) {
      main.appendChild(el('h3', {text: `step: ${step.step}  (pipeline ${step.pipeline})`}));
      const tbl = el('table', {class: 'compare-table'});
      const header = el('tr', {}, [el('th', {text: 'key'})]);
      for (const c of step.columns) {
        header.appendChild(el('th', {
          text: `${c.variation} (${c.variant})`,
          class: c.present ? '' : 'missing',
          title: c.present ? c.fingerprint.slice(0,12) : (c.error || 'missing')}));
      }
      tbl.appendChild(header);
      if (!step.rows.length) {
        const tr = el('tr', {}, [el('td', {colspan: step.columns.length + 1,
                                           class: 'hint',
                                           text: '(no parameters)'})]);
        tbl.appendChild(tr);
      }
      for (const row of step.rows) {
        const tr = el('tr', {class: row.equal ? '' : 'compare-differs'}, [
          el('td', {class: 'diff-key', text: row.path}),
        ]);
        for (const v of row.values) {
          tr.appendChild(el('td', {text: v === null ? '—' : fmtVal(v)}));
        }
        tbl.appendChild(tr);
      }
      main.appendChild(tbl);
    }
    main.appendChild(el('button', {class: 'ghost', style: 'margin-top:12px',
      text: 'Back to chain',
      onclick: () => this.showChain(chainName)}));
  },

  async showScoreboard(chainName, baseline) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Scoreboard — ${chainName}`}));
    const url = '/api/scoreboard/' + encodeURIComponent(chainName)
      + (baseline ? '?baseline=' + encodeURIComponent(baseline) : '');
    let d;
    try { d = await jget(url); }
    catch (e) { main.appendChild(el('pre', {text: 'failed: ' + e})); return; }

    //Baseline picker — null = absolute values, any variation = deltas.
    const sel = el('select', {class: 'run-filter',
      style: 'max-width:240px; margin:8px 0'});
    sel.appendChild(el('option', {value: '', text: '(no baseline)'}));
    for (const row of d.rows) {
      sel.appendChild(el('option', {value: row.variation, text: row.variation}));
    }
    if (baseline) sel.value = baseline;
    sel.onchange = () => this.showScoreboard(chainName, sel.value || null);
    main.appendChild(el('div', {}, [
      el('label', {text: 'baseline: '}), sel]));

    if (!d.metric_keys.length) {
      main.appendChild(el('p', {class: 'hint',
        text: '(no metrics recorded yet — stages call ctx.metric(stage, name, value) to populate this)'}));
    }
    const tbl = el('table', {class: 'compare-table'});
    const header = el('tr', {}, [el('th', {text: 'variation'})]);
    for (const k of d.metric_keys) header.appendChild(el('th', {text: k}));
    tbl.appendChild(header);
    for (const row of d.rows) {
      const tr = el('tr', {class: row.variation === baseline ? 'baseline-row' : ''},
        [el('td', {class: 'diff-key', text: row.variation})]);
      for (const k of d.metric_keys) {
        const v = row.metrics[k];
        const delta = row.deltas && row.deltas[k];
        let txt;
        if (v === undefined) txt = '—';
        else if (baseline && row.variation !== baseline
                 && typeof delta === 'number') {
          const sign = delta >= 0 ? '+' : '';
          txt = `${fmtVal(v)} (${sign}${fmt(delta)})`;
        } else txt = fmtVal(v);
        tr.appendChild(el('td', {text: txt}));
      }
      tbl.appendChild(tr);
    }
    main.appendChild(tbl);
    main.appendChild(el('button', {class: 'ghost', style: 'margin-top:12px',
      text: 'Back to chain',
      onclick: () => this.showChain(chainName)}));
  },

  async showDiskUsage() {
    this.current = {kind: 'disk'};
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: 'Disk usage'}));
    let d;
    try { d = await jget('/api/disk_usage'); }
    catch (e) { main.appendChild(el('pre', {text: 'failed: ' + e})); return; }
    main.appendChild(el('p', {class: 'hint',
      text: `${d.root}  ·  total ${humanSize(d.total)}`}));
    const tbl = el('table', {class: 'compare-table'});
    tbl.appendChild(el('tr', {}, [
      el('th', {text: 'pipeline'}), el('th', {text: 'variant'}),
      el('th', {text: 'run'}), el('th', {text: 'size'})]));
    for (const p of d.pipelines) {
      tbl.appendChild(el('tr', {}, [
        el('td', {class: 'diff-key', text: p.pipeline}),
        el('td', {text: ''}), el('td', {text: ''}),
        el('td', {text: humanSize(p.size)})]));
      for (const v of p.variants) {
        tbl.appendChild(el('tr', {}, [
          el('td', {text: ''}),
          el('td', {class: 'diff-key', text: v.variant}),
          el('td', {text: ''}),
          el('td', {text: humanSize(v.size)})]));
        for (const r of v.runs) {
          tbl.appendChild(el('tr', {}, [
            el('td', {text: ''}), el('td', {text: ''}),
            el('td', {}, [
              el('a', {href: '#', text: r.short_fp,
                onclick: ev => { ev.preventDefault();
                  this.showRun(p.pipeline, v.variant, r.short_fp); }})]),
            el('td', {text: humanSize(r.size)})]));
        }
      }
    }
    main.appendChild(tbl);
  },

  async find() {
    const q = $('#find-q').value.trim();
    if (q.length < 4) { alert('need at least 4 chars'); return; }
    let r;
    try { r = await jget('/api/find?q=' + encodeURIComponent(q)); }
    catch (e) { alert('find failed: ' + e); return; }
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `find: ${q}`}));
    if (!r.variants.length && !r.runs.length) {
      main.appendChild(el('p', {class: 'hint',
        text: `(no variants or runs match fingerprint prefix '${q}')`}));
      return;
    }
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
    main.appendChild(el('h2', {text: `${pipeline} / ${variant} [${short_fp}]`,
                               title: short_fp}));
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

    //"Why did this re-run vs ..." action.
    main.appendChild(el('div', {class: 'row', style: 'margin:6px 0'}, [
      el('button', {class: 'ghost', text: 'Why did this re-run vs…',
        onclick: () => this.showWhyRerunPicker(pipeline, variant, short_fp)}),
    ]));

    //Stage list — now also shows duration when available, plus a tiny
    //horizontal bar whose width is proportional to the longest stage.
    const total = d.stages.reduce(
      (m, s) => Math.max(m, s.duration_s || 0), 0);
    const stagesDiv = el('div');
    for (const s of d.stages) {
      const pct = total > 0 && s.duration_s
        ? Math.max(2, Math.round(100 * s.duration_s / total)) : 0;
      stagesDiv.appendChild(el('div', {class: 'stage-row'}, [
        el('span', {class: 'name', text: s.name}),
        el('span', {}, [el('span', {class: 'badge ' + (s.status || 'pending'),
                                    text: s.status || 'pending'})]),
        el('span', {class: 'stage-bar'}, [
          el('span', {class: 'stage-bar-fill ' + (s.status || 'pending'),
                      style: `width:${pct}%`}),
          el('span', {class: 'stage-bar-label',
            text: s.duration_s != null
              ? `${s.duration_s.toFixed(2)}s` : ''}),
        ]),
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

  async showWhyRerunPicker(pipeline, variant, short_fp) {
    //Show every other run of the same (pipeline, variant) tuple and let
    //the user pick one to diff against. /api/run_diff explains where the
    //stage cache keys diverged.
    const runs = (await jget('/api/runs?pipeline=' + encodeURIComponent(pipeline)
      + '&variant=' + encodeURIComponent(variant))).runs;
    const others = runs.filter(r => r.short_fp !== short_fp);
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `Why did this re-run? — ${pipeline} / ${variant}`}));
    main.appendChild(el('p', {}, [
      el('a', {href: '#', text: `← back to run ${short_fp}`,
        onclick: ev => { ev.preventDefault();
          this.showRun(pipeline, variant, short_fp); }}),
    ]));
    main.appendChild(el('p', {class: 'hint',
      text: 'Pick another run of the same pipeline+variant to compare ' +
            'stage cache keys.'}));
    if (!others.length) {
      main.appendChild(el('p', {class: 'hint',
        text: '(no other runs of this pipeline/variant exist yet)'}));
      return;
    }
    const list = el('div', {class: 'variant-list'});
    for (const r of others) {
      list.appendChild(el('a', {href: '#',
        text: `${r.short_fp}  ${r.started || ''}`,
        onclick: ev => { ev.preventDefault();
          this.showRunDiff(pipeline, variant, short_fp, r.short_fp); }}));
    }
    main.appendChild(list);
  },

  async showRunDiff(pipeline, variant, a, b) {
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2',
      {text: `Run diff — ${pipeline}/${variant}  ${a} vs ${b}`}));
    main.appendChild(el('p', {}, [
      el('a', {href: '#', text: `← back to run ${a}`,
        onclick: ev => { ev.preventDefault();
          this.showRun(pipeline, variant, a); }}),
    ]));
    let d;
    try {
      d = await jget('/api/run_diff?' + new URLSearchParams({
        pipeline, variant, a, b}));
    } catch (e) { main.appendChild(el('pre', {text: 'failed: ' + e})); return; }
    for (const st of d.stages) {
      const box = el('div', {class: 'diff-box'});
      const head = el('div', {class: 'diff-variant-head'}, [
        el('span', {class: 'diff-name', text: st.name}),
        el('span', {class: 'diff-kind ' + (st.identical ? 'matches' : 'differs'),
          text: st.identical ? 'identical' : 'differs'}),
      ]);
      box.appendChild(head);
      if (!st.identical) {
        const flags = [];
        if (st.fn_changed) flags.push('fn');
        if (st.config_changed) flags.push('config');
        if (st.upstream_changed) flags.push('upstream');
        box.appendChild(el('p', {class: 'hint',
          text: 'changed: ' + flags.join(', ')}));
        if (st.config_diff && st.config_diff.length) {
          const tbl = el('table', {class: 'diff-table'});
          tbl.appendChild(el('tr', {}, [
            el('th', {text: 'key'}),
            el('th', {text: a}), el('th', {text: b})]));
          for (const e of st.config_diff) {
            tbl.appendChild(el('tr', {class: 'diff-' + e.kind}, [
              el('td', {class: 'diff-key', text: e.path}),
              el('td', {text: e.kind === 'added' ? '—' : fmtVal(e.parent)}),
              el('td', {text: e.kind === 'removed' ? '—' : fmtVal(e.child)})]));
          }
          box.appendChild(tbl);
        }
        if (st.upstream_diff && st.upstream_diff.length) {
          const tbl = el('table', {class: 'diff-table'});
          tbl.appendChild(el('tr', {}, [
            el('th', {text: 'upstream stage'}),
            el('th', {text: a}), el('th', {text: b})]));
          for (const u of st.upstream_diff) {
            tbl.appendChild(el('tr', {class: 'diff-changed'}, [
              el('td', {class: 'diff-key', text: u.name}),
              el('td', {text: (u.a || '').slice(0,12)}),
              el('td', {text: (u.b || '').slice(0,12)})]));
          }
          box.appendChild(tbl);
        }
      }
      main.appendChild(box);
    }
  },

  async showStage(pipeline, variant, short_fp, stage) {
    //Track whether the previous view was already this exact stage; if so,
    //we may be able to skip the re-render and preserve expanded previews.
    const wasSameStage = this.current
      && this.current.kind === 'stage'
      && this.current.pipeline === pipeline
      && this.current.variant === variant
      && this.current.short_fp === short_fp
      && this.current.stage === stage;
    this.current = {kind: 'stage', pipeline, variant, short_fp, stage};
    let d;
    try {
      d = await jget(`/api/stage/${encodeURIComponent(pipeline)}/${encodeURIComponent(variant)}/${encodeURIComponent(short_fp)}/${encodeURIComponent(stage)}`);
    } catch (e) {
      $('#main').innerHTML = ''; $('#main').appendChild(el('pre', {text: String(e)}));
      this._lastStageSig = null;
      return;
    }
    //handleRunChanged re-invokes showStage on any /ws run_changed event;
    //skip the re-render when the payload hasn't meaningfully changed, so
    //we don't wipe out the user's expanded-preview state.
    const sig = JSON.stringify({
      status: d.status, key: d.key, error: d.error,
      artifacts: (d.artifacts || []).map(a => [a.path, a.size]),
    });
    if (wasSameStage && this._lastStageSig === sig) return;
    this._lastStageSig = sig;
    const main = $('#main'); main.innerHTML = '';
    main.appendChild(el('h2', {text: `${pipeline} / ${variant} / ${stage}`,
                               title: short_fp}));
    main.appendChild(el('p', {}, [
      el('a', {href: '#', text: `← back to run ${short_fp}`,
        onclick: ev => { ev.preventDefault();
          this.showRun(pipeline, variant, short_fp); }}),
    ]));
    main.appendChild(el('p', {}, [
      'status: ', el('span', {class: 'badge ' + (d.status || 'pending'),
                              text: d.status || 'pending'}),
      ' ',
      d.key
        ? el('code', {text: d.key.slice(0,12)})
        : el('span', {class: 'hint', text: '(no key)'}),
    ]));
    if (d.error) {
      main.appendChild(el('h3', {text: 'Error'}));
      main.appendChild(el('pre', {text: d.error}));
    }
    if (!d.artifacts.length) {
      main.appendChild(el('p', {class: 'hint', text: '(no artifacts yet)'}));
      return;
    }
    main.appendChild(el('p', {class: 'hint',
      text: `${d.artifacts.length} output${d.artifacts.length === 1 ? '' : 's'} ` +
            '— click a row to preview.'}));
    for (const a of d.artifacts) {
      const card = el('div', {class: 'artifact collapsed'});
      const caret = el('span', {class: 'caret', text: '▶'});
      const header = el('div', {class: 'header clickable'}, [
        caret,
        el('span', {class: 'path', text: a.path}),
        el('span', {class: 'meta', text: humanSize(a.size)}),
        el('a', {href: artifactHref(pipeline, variant, short_fp, a.path),
                 target: '_blank', text: 'download',
                 onclick: ev => ev.stopPropagation()}),
        el('button', {class: 'ghost', text: 'Diff vs…',
          onclick: ev => { ev.stopPropagation();
                           this.diffArtifactPrompt(a); }}),
      ]);
      const body = el('div', {class: 'body', style: 'display:none'});
      let loaded = false;
      header.onclick = () => {
        const isCollapsed = card.classList.toggle('collapsed');
        caret.textContent = isCollapsed ? '▶' : '▼';
        body.style.display = isCollapsed ? 'none' : '';
        if (!isCollapsed && !loaded) {
          loaded = true;
          this.renderArtifact(body, a, {pipeline, variant, short_fp});
        }
      };
      card.appendChild(header);
      card.appendChild(body);
      main.appendChild(card);
    }
  },

  async diffArtifactPrompt(artifact) {
    //Inline picker. We only let the user pick runs that aren't the
    //current one. Pipeline → variant → run cascading dropdowns.
    //When the current run is part of a chain, a sibling-variation
    //quick-pick is shown first as a shortcut for the common case.
    const runs = (await jget('/api/runs')).runs;
    if (!runs.length) { alert('no runs to diff against'); return; }
    const cur = this.current;
    let chainCtx = null;
    if (cur && (cur.kind === 'run' || cur.kind === 'stage')) {
      try {
        const rd = await jget(`/api/run/${encodeURIComponent(cur.pipeline)}/${encodeURIComponent(cur.variant)}/${encodeURIComponent(cur.short_fp)}`);
        if (rd.run && rd.run.chain && rd.run.variation) {
          chainCtx = {chain: rd.run.chain, variation: rd.run.variation};
        }
      } catch (_) {}
    }
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

    const back = el('button', {class: 'ghost', text: 'Cancel',
      onclick: () => {
        if (cur && cur.kind === 'stage') {
          this.showStage(cur.pipeline, cur.variant, cur.short_fp, cur.stage);
        } else if (cur && cur.kind === 'run') {
          this.showRun(cur.pipeline, cur.variant, cur.short_fp);
        } else {
          this.refresh();
        }
      }});
    const controls = el('div', {class: 'row', style: 'margin:8px 0'},
      [el('label', {text: 'compare against: '}), pSel, vSel, rSel, go, back]);
    main.appendChild(controls);

    if (chainCtx) {
      //Sibling-variation shortcut. Walk the chain's variations, look up
      //each variation's run for the same step (= this run's pipeline),
      //and offer a one-click diff against the same artifact path.
      try {
        const detail = await jget('/api/chain/' + encodeURIComponent(chainCtx.chain));
        const stepName = (detail.steps.find(s => s.pipeline === cur.pipeline) || {}).name;
        if (stepName) {
          const others = detail.variations.filter(v => v.name !== chainCtx.variation);
          if (others.length) {
            const box = el('div', {class: 'diff-box',
                                   style: 'margin-top:10px'});
            box.appendChild(el('h3', {style: 'margin-top:0',
              text: `Sibling variations of chain '${chainCtx.chain}'`}));
            for (const v of others) {
              const link = el('a', {href: '#', text: `→ diff vs '${v.name}'`,
                onclick: async ev => {
                  ev.preventDefault();
                  try {
                    const prog = await jget(`/api/chain_progress/${encodeURIComponent(chainCtx.chain)}/${encodeURIComponent(v.name)}`);
                    const sib = (prog.steps || []).find(s => s.name === stepName);
                    if (!sib || !sib.short_fp) {
                      alert(`variation '${v.name}' hasn't produced this step yet`);
                      return;
                    }
                    //Build path_b inside the sibling's run dir.
                    const sibRun = (await jget('/api/runs')).runs.find(r =>
                      r.pipeline === sib.pipeline && r.variant === sib.variant &&
                      r.short_fp === sib.short_fp);
                    if (!sibRun) { alert('sibling run dir not found'); return; }
                    const path_b = sibRun.fdir + '/' + artifact.path;
                    const r = await jget('/api/artifact_diff?' + new URLSearchParams({
                      path_a: artifact.absolute, path_b}));
                    const out = el('div');
                    out.appendChild(el('p', {class: 'hint',
                      text: `'${chainCtx.variation}' vs '${v.name}'  ·  path_b: ${path_b}`}));
                    this.renderArtifactDiff(out, r);
                    box.replaceWith(out);
                  } catch (e) { alert('diff failed: ' + e); }
                }});
              box.appendChild(el('div', {}, [link]));
            }
            main.appendChild(box);
          }
        }
      } catch (_) {}
    }
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
      //Dict-shaped JSON: server returns `entries` (a configs-diff list).
      if (r.entries) {
        if (r.entries.length === 0) {
          main.appendChild(el('p', {class: 'hint', text: '(identical)'}));
          return;
        }
        const tbl = el('table', {class: 'diff-table'});
        tbl.appendChild(el('tr', {}, [
          el('th', {text: 'key'}), el('th', {text: 'a'}), el('th', {text: 'b'})]));
        for (const e of r.entries) {
          tbl.appendChild(el('tr', {class: 'diff-' + e.kind}, [
            el('td', {class: 'diff-key', text: e.path}),
            el('td', {text: e.kind === 'added' ? '—' : fmtVal(e.parent)}),
            el('td', {text: e.kind === 'removed' ? '—' : fmtVal(e.child)}),
          ]));
        }
        main.appendChild(tbl);
        return;
      }
      //Non-dict JSON (array, primitive, null): server returns `{a, b, equal}`.
      if (r.equal) {
        main.appendChild(el('p', {class: 'hint', text: '(identical)'}));
        return;
      }
      const tbl = el('table', {class: 'diff-table'});
      tbl.appendChild(el('tr', {}, [el('th', {text: 'a'}), el('th', {text: 'b'})]));
      tbl.appendChild(el('tr', {class: 'diff-changed'}, [
        el('td', {text: JSON.stringify(r.a, null, 2)}),
        el('td', {text: JSON.stringify(r.b, null, 2)}),
      ]));
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
      if (r.overlay) {
        const div = el('div', {class: 'plot'});
        main.appendChild(div);
        Plotly.newPlot(div, [
          {x: r.overlay.x, y: r.overlay.y_a, type: 'scatter',
           mode: 'lines', name: 'a'},
          {x: r.overlay.x, y: r.overlay.y_b, type: 'scatter',
           mode: 'lines', name: 'b'},
        ], {margin: {t: 30, l: 40, r: 20, b: 30},
            title: 'overlay (b on a)' +
              (r.overlay.stride > 1 ? `  · stride ${r.overlay.stride}` : '')});
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
    } else if (k === 'ptyr') {
      this.renderPtyr(container, artifact.absolute);
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

  async renderPtyr(container, path) {
    //PtyPy .ptyr preview — mirrors renderSRW but axes are obj/probe
    //storages, layer/mode index, and complex-array repr. Cuts are wired
    //up the same way (click in the heatmap to move the cross-hair).
    container.innerHTML = '';
    const state = {path, kind: 'obj', storage: '', mode: 0,
                   repr: 'amplitude', row: -1, col: -1, summary: null};
    const controls = el('div', {class: 'row', style: 'margin-bottom:8px'});
    const kindSel = el('select');
    kindSel.appendChild(el('option', {value: 'obj',   text: 'object'}));
    kindSel.appendChild(el('option', {value: 'probe', text: 'probe'}));
    const storSel = el('select');
    const modeSel = el('select');
    const reprSel = el('select');
    for (const r of ['amplitude', 'phase', 'real', 'imag', 'intensity'])
      reprSel.appendChild(el('option', {value: r, text: r}));
    reprSel.value = 'amplitude';
    controls.appendChild(el('label', {}, ['kind: ']));    controls.appendChild(kindSel);
    controls.appendChild(el('label', {}, [' storage: '])); controls.appendChild(storSel);
    controls.appendChild(el('label', {}, [' mode: ']));   controls.appendChild(modeSel);
    controls.appendChild(el('label', {}, [' repr: ']));   controls.appendChild(reprSel);
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
    //Convergence sub-plot — only rendered once when the summary first
    //arrives (it's static for the lifetime of the file).
    const convDiv = el('div', {class: 'plot', style: 'min-height:220px;margin-top:8px'});
    container.appendChild(convDiv);

    const repopulateStorageAndMode = (summary, fillSelectors) => {
      const list = (summary.storages && summary.storages[state.kind]) || [];
      if (fillSelectors) {
        storSel.innerHTML = '';
        for (const s of list)
          storSel.appendChild(el('option', {value: s.id,
            text: `${s.id}  [${(s.shape || []).join('×')}]`}));
        if (list.length) {
          state.storage = list[0].id;
          storSel.value = state.storage;
        } else {
          state.storage = '';
        }
      }
      const cur = list.find(s => s.id === state.storage) || list[0];
      const nmodes = cur ? (cur.shape || [1])[0] : 1;
      modeSel.innerHTML = '';
      for (let i = 0; i < nmodes; i++)
        modeSel.appendChild(el('option', {value: String(i), text: String(i)}));
      state.mode = Math.min(state.mode, nmodes - 1);
      modeSel.value = String(state.mode);
    };

    const fetchAndDraw = async () => {
      const q = new URLSearchParams({
        path, kind: state.kind, storage: state.storage,
        mode: state.mode, repr: state.repr,
        row: state.row, col: state.col});
      let payload;
      try { payload = await jget('/api/ptyr_preview?' + q); }
      catch (e) { meta.textContent = 'preview failed: ' + e; return; }
      if (payload.kind === 'error') {
        meta.textContent = 'preview error: ' + payload.data; return;
      }
      const m = payload.meta;
      if (state.summary === null) {
        state.summary = {storages: m.storages, iter_info: m.iter_info || {}};
        //Default kind: if obj is empty but probe isn't, pick probe.
        if (!(state.summary.storages.obj || []).length
            && (state.summary.storages.probe || []).length) {
          state.kind = 'probe'; kindSel.value = 'probe';
        }
        repopulateStorageAndMode(state.summary, true);
        //Re-fetch with the correct storage if defaulting picked one.
        if (m.storage !== state.storage && state.storage) {
          return fetchAndDraw();
        }
        drawConvergence(state.summary.iter_info);
      }
      const z = payload.data.z, cut = payload.data.cut;
      //Axes in microns relative to origin. psize/origin are in metres.
      const ny = z.length, nx = z[0].length;
      const sy = (m.downsampled && m.downsampled[0]) || 1;
      const sx = (m.downsampled && m.downsampled[1]) || 1;
      const py = (m.psize && m.psize[0]) || 1;
      const px = (m.psize && m.psize[1]) || 1;
      const oy = (m.origin && m.origin[0]) || 0;
      const ox = (m.origin && m.origin[1]) || 0;
      const xs = new Array(nx);
      for (let i = 0; i < nx; i++) xs[i] = (ox + i * sx * px) * 1e6;
      const ys = new Array(ny);
      for (let j = 0; j < ny; j++) ys[j] = (oy + j * sy * py) * 1e6;
      const ds = (sy > 1 || sx > 1) ? `  (downsampled ${sy}×${sx})` : '';
      Plotly.react(heatDiv, [{z, x: xs, y: ys, type: 'heatmap',
                              colorscale: state.repr === 'phase' ? 'HSV' : 'Viridis'}], {
        margin: {t: 24, l: 50, r: 20, b: 40},
        xaxis: {title: 'x [µm]'}, yaxis: {title: 'y [µm]', scaleanchor: 'x'},
        title: `${m.kind} · ${m.storage} · mode ${m.mode}/${m.nmodes - 1} · ${m.repr}${ds}`,
        shapes: [
          {type:'line', x0:xs[cut.col], x1:xs[cut.col], y0:ys[0], y1:ys[ys.length-1], line:{color:'red', width:1}},
          {type:'line', y0:ys[cut.row], y1:ys[cut.row], x0:xs[0], x1:xs[xs.length-1], line:{color:'red', width:1}},
        ],
      });
      Plotly.react(hcutDiv, [{x: xs, y: cut.h, type: 'scatter', mode: 'lines',
                              line: {color: '#c0392b'}}],
        {margin: {t: 20, l: 50, r: 20, b: 30},
         title: `horizontal cut @ row=${cut.row}`, xaxis: {title: 'x [µm]'}});
      Plotly.react(vcutDiv, [{x: ys, y: cut.v, type: 'scatter', mode: 'lines',
                              line: {color: '#c0392b'}}],
        {margin: {t: 20, l: 50, r: 20, b: 30},
         title: `vertical cut @ col=${cut.col}`, xaxis: {title: 'y [µm]'}});
      const psStr = (m.psize || []).map(v => fmt(v)).join(' × ');
      meta.textContent =
        `shape: ${(m.shape || []).join('×')}  ·  pixel size: ${psStr} m  ` +
        `·  cross-hair: (row=${cut.row}, col=${cut.col})`;
      heatDiv.removeAllListeners?.();
      heatDiv.on('plotly_click', ev => {
        const p = ev.points && ev.points[0]; if (!p) return;
        state.col = p.pointIndex[1]; state.row = p.pointIndex[0];
        fetchAndDraw();
      });
    };

    const drawConvergence = (ii) => {
      if (!ii || !ii.iteration || !ii.iteration.length) {
        convDiv.style.display = 'none'; return;
      }
      const traces = [];
      if (ii.error_fourier)
        traces.push({x: ii.iteration, y: ii.error_fourier, type: 'scatter',
                     mode: 'lines', name: 'fourier'});
      if (ii.error_overlap)
        traces.push({x: ii.iteration, y: ii.error_overlap, type: 'scatter',
                     mode: 'lines', name: 'overlap'});
      Plotly.newPlot(convDiv, traces, {
        margin: {t: 24, l: 60, r: 20, b: 40},
        title: 'convergence',
        xaxis: {title: 'iteration'},
        yaxis: {title: 'error', type: 'log'},
      });
    };

    kindSel.onchange = () => {
      state.kind = kindSel.value;
      if (state.summary) repopulateStorageAndMode(state.summary, true);
      state.row = -1; state.col = -1;
      fetchAndDraw();
    };
    storSel.onchange = () => {
      state.storage = storSel.value;
      if (state.summary) repopulateStorageAndMode(state.summary, false);
      state.row = -1; state.col = -1;
      fetchAndDraw();
    };
    modeSel.onchange = () => {
      state.mode = parseInt(modeSel.value, 10) || 0; fetchAndDraw();
    };
    reprSel.onchange = () => { state.repr = reprSel.value; fetchAndDraw(); };
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

function _runStatus(r) {
  //Derive an overall run status from per-stage statuses. failed > running
  //> pending > done — whichever's strongest "wins" — so a partially-
  //failed run reads as 'failed' at a glance.
  const ss = r.stage_status || {};
  const vals = Object.values(ss);
  if (!vals.length) return 'pending';
  if (vals.some(s => s === 'failed')) return 'failed';
  if (vals.some(s => s === 'running')) return 'running';
  if (vals.some(s => s === 'pending')) return 'pending';
  return 'done';
}

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
  if (x == null) return '?';
  if (x === 0) return '0';
  if (Number.isNaN(x)) return 'NaN';
  if (!Number.isFinite(x)) return String(x);
  return Math.abs(x) < 1e-3 || Math.abs(x) > 1e6
    ? x.toExponential(3) : x.toPrecision(4);
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
