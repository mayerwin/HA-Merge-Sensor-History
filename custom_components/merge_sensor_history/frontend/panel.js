/**
 * Merge Sensors History - Custom Panel
 *
 * Provides a UI to select source/destination entity pairs
 * and import historical data between them.
 */
class MergeSensorsHistoryPanel extends HTMLElement {
  constructor() {
    super();
    this._hass = null;
    this._pairs = [{ source: "", destination: "" }];
    this._importing = false;
    this._results = null;
  }

  set hass(hass) {
    this._hass = hass;
    if (!this.shadowRoot) {
      this._render();
    }
  }

  set panel(panel) {
    this._panel = panel;
  }

  _render() {
    const shadow = this.attachShadow({ mode: "open" });
    shadow.innerHTML = `
      <style>
        :host {
          display: block;
          padding: 16px;
          max-width: 900px;
          margin: 0 auto;
          font-family: var(--paper-font-body1_-_font-family, "Roboto", sans-serif);
          color: var(--primary-text-color, #212121);
        }
        .card {
          background: var(--ha-card-background, var(--card-background-color, white));
          border-radius: var(--ha-card-border-radius, 12px);
          box-shadow: var(--ha-card-box-shadow, 0 2px 6px rgba(0,0,0,0.1));
          padding: 24px;
          margin-bottom: 16px;
        }
        h1 {
          font-size: 24px;
          font-weight: 400;
          margin: 0 0 8px 0;
          color: var(--primary-text-color);
        }
        .subtitle {
          color: var(--secondary-text-color, #727272);
          font-size: 14px;
          margin-bottom: 24px;
          line-height: 1.5;
        }
        .pair-row {
          display: flex;
          align-items: center;
          gap: 12px;
          margin-bottom: 12px;
          padding: 12px;
          background: var(--secondary-background-color, #f5f5f5);
          border-radius: 8px;
        }
        .pair-row .entity-col {
          flex: 1;
        }
        .pair-row label {
          display: block;
          font-size: 12px;
          font-weight: 500;
          color: var(--secondary-text-color);
          margin-bottom: 4px;
          text-transform: uppercase;
          letter-spacing: 0.5px;
        }
        .pair-row select {
          width: 100%;
          padding: 8px 12px;
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 6px;
          font-size: 14px;
          background: var(--ha-card-background, white);
          color: var(--primary-text-color);
          cursor: pointer;
          appearance: auto;
        }
        .pair-row select:focus {
          outline: none;
          border-color: var(--primary-color, #03a9f4);
          box-shadow: 0 0 0 1px var(--primary-color, #03a9f4);
        }
        .arrow {
          font-size: 20px;
          color: var(--secondary-text-color);
          padding-top: 18px;
        }
        .btn {
          padding: 8px 20px;
          border: none;
          border-radius: 6px;
          font-size: 14px;
          font-weight: 500;
          cursor: pointer;
          transition: background 0.2s, opacity 0.2s;
        }
        .btn:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }
        .btn-primary {
          background: var(--primary-color, #03a9f4);
          color: var(--text-primary-color, white);
        }
        .btn-primary:hover:not(:disabled) {
          filter: brightness(1.1);
        }
        .btn-secondary {
          background: var(--secondary-background-color, #f5f5f5);
          color: var(--primary-text-color);
          border: 1px solid var(--divider-color, #e0e0e0);
        }
        .btn-danger {
          background: none;
          color: var(--error-color, #db4437);
          padding: 4px 8px;
          font-size: 20px;
          min-width: 36px;
          padding-top: 18px;
        }
        .actions {
          display: flex;
          gap: 12px;
          margin-top: 16px;
          align-items: center;
        }
        .results {
          margin-top: 16px;
        }
        .result-item {
          padding: 12px;
          border-radius: 6px;
          margin-bottom: 8px;
          font-size: 14px;
        }
        .result-success {
          background: var(--success-color, #4caf50);
          color: white;
        }
        .result-error {
          background: var(--error-color, #db4437);
          color: white;
        }
        .spinner {
          display: inline-block;
          width: 18px;
          height: 18px;
          border: 2px solid rgba(255,255,255,0.3);
          border-top-color: white;
          border-radius: 50%;
          animation: spin 0.8s linear infinite;
          vertical-align: middle;
          margin-right: 8px;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
        .warning {
          background: var(--warning-color, #ff9800);
          color: white;
          padding: 12px;
          border-radius: 6px;
          margin-bottom: 16px;
          font-size: 13px;
          line-height: 1.5;
        }
        .filter-row {
          margin-bottom: 12px;
        }
        .filter-row input {
          width: 100%;
          padding: 8px 12px;
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 6px;
          font-size: 14px;
          background: var(--ha-card-background, white);
          color: var(--primary-text-color);
          box-sizing: border-box;
        }
        .filter-row input:focus {
          outline: none;
          border-color: var(--primary-color, #03a9f4);
        }
      </style>
      <div class="card">
        <h1>Merge Sensors History</h1>
        <p class="subtitle">
          Import historical data from source sensors into destination sensors.
          Only data older than the destination's oldest record will be imported (no duplicates).
        </p>
        <div class="warning">
          This operation writes directly to the recorder database. It is recommended to
          <strong>back up your Home Assistant database</strong> before importing.
          Imported states will appear in history graphs after the next recorder refresh.
        </div>
        <div class="filter-row">
          <input type="text" id="entity-filter" placeholder="Filter entities (e.g. ecowitt, temperature)..." />
        </div>
        <div id="pairs-container"></div>
        <div class="actions">
          <button class="btn btn-secondary" id="add-pair-btn">+ Add Pair</button>
          <div style="flex:1"></div>
          <button class="btn btn-primary" id="import-btn">Import History</button>
        </div>
        <div id="results-container" class="results"></div>
      </div>
    `;

    this._pairsContainer = shadow.getElementById("pairs-container");
    this._resultsContainer = shadow.getElementById("results-container");
    this._importBtn = shadow.getElementById("import-btn");
    this._filterInput = shadow.getElementById("entity-filter");

    shadow.getElementById("add-pair-btn").addEventListener("click", () => {
      this._pairs.push({ source: "", destination: "" });
      this._renderPairs();
    });

    this._importBtn.addEventListener("click", () => this._doImport());

    this._filterInput.addEventListener("input", () => {
      this._renderPairs();
    });

    this._renderPairs();
  }

  _getFilteredEntities() {
    if (!this._hass) return [];
    const filter = (this._filterInput?.value || "").toLowerCase();
    const entities = Object.keys(this._hass.states).sort();
    if (!filter) return entities;
    return entities.filter((e) => e.toLowerCase().includes(filter));
  }

  _buildOptions(entities, selected) {
    // Always include the currently selected value so it stays visible
    // even when the filter would otherwise hide it.
    let opts = '<option value="">-- Select entity --</option>';
    const seen = new Set();
    if (selected && !entities.includes(selected)) {
      opts += `<option value="${selected}" selected>${selected} (filtered)</option>`;
      seen.add(selected);
    }
    for (const e of entities) {
      if (seen.has(e)) continue;
      opts += `<option value="${e}" ${e === selected ? "selected" : ""}>${e}</option>`;
    }
    return opts;
  }

  _renderPairs() {
    const entities = this._getFilteredEntities();
    const container = this._pairsContainer;
    container.innerHTML = "";

    this._pairs.forEach((pair, index) => {
      const row = document.createElement("div");
      row.className = "pair-row";

      const sourceCol = document.createElement("div");
      sourceCol.className = "entity-col";
      const sourceLabel = document.createElement("label");
      sourceLabel.textContent = "Source (old sensor)";
      const sourceSelect = document.createElement("select");
      sourceSelect.innerHTML = this._buildOptions(entities, pair.source);
      sourceSelect.addEventListener("change", (ev) => {
        this._pairs[index].source = ev.target.value;
      });
      sourceCol.appendChild(sourceLabel);
      sourceCol.appendChild(sourceSelect);

      const arrow = document.createElement("div");
      arrow.className = "arrow";
      arrow.textContent = "\u2192";

      const destCol = document.createElement("div");
      destCol.className = "entity-col";
      const destLabel = document.createElement("label");
      destLabel.textContent = "Destination (new sensor)";
      const destSelect = document.createElement("select");
      destSelect.innerHTML = this._buildOptions(entities, pair.destination);
      destSelect.addEventListener("change", (ev) => {
        this._pairs[index].destination = ev.target.value;
      });
      destCol.appendChild(destLabel);
      destCol.appendChild(destSelect);

      const removeBtn = document.createElement("button");
      removeBtn.className = "btn btn-danger";
      removeBtn.textContent = "\u00d7";
      removeBtn.title = "Remove pair";
      removeBtn.addEventListener("click", () => {
        if (this._pairs.length > 1) {
          this._pairs.splice(index, 1);
          this._renderPairs();
        }
      });

      row.appendChild(sourceCol);
      row.appendChild(arrow);
      row.appendChild(destCol);
      row.appendChild(removeBtn);
      container.appendChild(row);
    });
  }

  async _doImport() {
    // Validate pairs
    const validPairs = this._pairs.filter((p) => p.source && p.destination);
    if (validPairs.length === 0) {
      alert("Please select at least one complete source/destination pair.");
      return;
    }

    const dupes = validPairs.filter((p) => p.source === p.destination);
    if (dupes.length > 0) {
      alert("Source and destination cannot be the same entity.");
      return;
    }

    if (
      !confirm(
        `Import history for ${validPairs.length} pair(s)?\n\n` +
          validPairs.map((p) => `  ${p.source}  \u2192  ${p.destination}`).join("\n") +
          "\n\nThis will write to your recorder database."
      )
    ) {
      return;
    }

    this._importing = true;
    this._importBtn.disabled = true;
    this._importBtn.innerHTML = '<span class="spinner"></span>Importing...';
    this._resultsContainer.innerHTML = "";

    try {
      const response = await this._hass.callWS({
        type: "merge_sensor_history/import",
        pairs: validPairs,
      });

      this._renderResults(response.results);
    } catch (err) {
      this._resultsContainer.innerHTML = `
        <div class="result-item result-error">
          Import failed: ${err.message || err}
        </div>`;
    } finally {
      this._importing = false;
      this._importBtn.disabled = false;
      this._importBtn.textContent = "Import History";
    }
  }

  _renderResults(results) {
    this._resultsContainer.innerHTML = results
      .map((r) => {
        if (r.error) {
          return `<div class="result-item result-error">
            <strong>${r.source} \u2192 ${r.destination}</strong><br/>
            Error: ${r.error}<br/>
            <em>No data was written — the import was rolled back.</em>
          </div>`;
        }
        const details = [];
        details.push(`${r.states_imported} states imported`);
        if (r.states_already_covered > 0) {
          details.push(`${r.states_already_covered} already covered by destination`);
        }
        if (r.stats_imported > 0) {
          details.push(`${r.stats_imported} statistic rows imported`);
        }
        if (r.source_total > 0) {
          details.push(`${r.source_total} total source states`);
        }
        if (r.stats_error) {
          details.push(`Stats error: ${r.stats_error}`);
        }
        const safe = r.states_imported === 0 && r.stats_imported === 0 && !r.stats_error;
        return `<div class="result-item result-success">
          <strong>${r.source} \u2192 ${r.destination}</strong><br/>
          ${safe ? "Nothing to import (already up to date)." : details.join(" | ")}
        </div>`;
      })
      .join("");
  }
}

customElements.define("merge-sensor-history-panel", MergeSensorsHistoryPanel);
