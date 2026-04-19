/**
 * Merge Sensor History - Custom Panel
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

  /** Get friendly name for an entity, or empty string if not found. */
  _friendlyName(entityId) {
    if (!entityId || !this._hass) return "";
    const stateObj = this._hass.states[entityId];
    if (!stateObj) return "";
    const name = stateObj.attributes.friendly_name;
    return name && name !== entityId ? name : "";
  }

  _render() {
    const shadow = this.attachShadow({ mode: "open" });
    shadow.innerHTML = `
      <style>
        :host {
          display: block;
          padding: 24px 16px;
          max-width: 960px;
          margin: 0 auto;
          font-family: var(--paper-font-body1_-_font-family, "Roboto", sans-serif);
          color: var(--primary-text-color, #212121);
          -webkit-font-smoothing: antialiased;
        }
        .card {
          background: var(--ha-card-background, var(--card-background-color, white));
          border-radius: var(--ha-card-border-radius, 12px);
          box-shadow: var(--ha-card-box-shadow, 0 2px 8px rgba(0,0,0,0.08));
          padding: 28px;
          margin-bottom: 16px;
        }
        .header {
          display: flex;
          align-items: center;
          gap: 12px;
          margin-bottom: 6px;
        }
        .header-icon {
          font-size: 28px;
          opacity: 0.8;
        }
        h1 {
          font-size: 22px;
          font-weight: 500;
          margin: 0;
          color: var(--primary-text-color);
        }
        .subtitle {
          color: var(--secondary-text-color, #727272);
          font-size: 14px;
          margin-bottom: 20px;
          line-height: 1.6;
        }
        .warning-banner {
          display: flex;
          align-items: flex-start;
          gap: 10px;
          background: color-mix(in srgb, var(--warning-color, #ff9800) 12%, transparent);
          border: 1px solid color-mix(in srgb, var(--warning-color, #ff9800) 30%, transparent);
          color: var(--primary-text-color);
          padding: 14px 16px;
          border-radius: 8px;
          margin-bottom: 20px;
          font-size: 13px;
          line-height: 1.5;
        }
        .warning-banner .warn-icon {
          font-size: 18px;
          flex-shrink: 0;
          margin-top: 1px;
        }
        .filter-row {
          margin-bottom: 16px;
          position: relative;
        }
        .filter-row input {
          width: 100%;
          padding: 10px 14px 10px 38px;
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 8px;
          font-size: 14px;
          background: var(--input-fill-color, var(--secondary-background-color, #f5f5f5));
          color: var(--primary-text-color);
          box-sizing: border-box;
          transition: border-color 0.2s, box-shadow 0.2s;
        }
        .filter-row input::placeholder {
          color: var(--secondary-text-color, #999);
          opacity: 0.8;
        }
        .filter-row input:focus {
          outline: none;
          border-color: var(--primary-color, #03a9f4);
          box-shadow: 0 0 0 1px var(--primary-color, #03a9f4);
        }
        .filter-row .search-icon {
          position: absolute;
          left: 12px;
          top: 50%;
          transform: translateY(-50%);
          font-size: 16px;
          color: var(--secondary-text-color);
          pointer-events: none;
        }
        .pair-row {
          display: flex;
          align-items: flex-start;
          gap: 12px;
          margin-bottom: 14px;
          padding: 16px;
          background: var(--secondary-background-color, #f5f5f5);
          border-radius: 10px;
          border: 1px solid var(--divider-color, #e0e0e0);
          transition: border-color 0.2s;
        }
        .pair-row:hover {
          border-color: color-mix(in srgb, var(--primary-color, #03a9f4) 40%, transparent);
        }
        .pair-row .entity-col {
          flex: 1;
          min-width: 0;
        }
        .pair-row label {
          display: block;
          font-size: 11px;
          font-weight: 600;
          color: var(--secondary-text-color);
          margin-bottom: 6px;
          text-transform: uppercase;
          letter-spacing: 0.8px;
        }
        .pair-row select {
          width: 100%;
          padding: 9px 12px;
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 6px;
          font-size: 13px;
          background: var(--ha-card-background, var(--card-background-color, white));
          color: var(--primary-text-color);
          cursor: pointer;
          appearance: auto;
          transition: border-color 0.2s, box-shadow 0.2s;
        }
        .pair-row select option {
          color: var(--primary-text-color);
          background: var(--ha-card-background, var(--card-background-color, white));
        }
        .pair-row select:focus {
          outline: none;
          border-color: var(--primary-color, #03a9f4);
          box-shadow: 0 0 0 1px var(--primary-color, #03a9f4);
        }
        .entity-info {
          margin-top: 5px;
          font-size: 12px;
          color: var(--secondary-text-color, #727272);
          min-height: 18px;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
          font-style: italic;
        }
        .arrow-col {
          display: flex;
          align-items: center;
          padding-top: 24px;
          font-size: 22px;
          color: var(--primary-color, #03a9f4);
          opacity: 0.7;
          flex-shrink: 0;
        }
        .remove-col {
          display: flex;
          align-items: center;
          padding-top: 24px;
          flex-shrink: 0;
        }
        .btn {
          padding: 9px 22px;
          border: none;
          border-radius: 8px;
          font-size: 14px;
          font-weight: 500;
          cursor: pointer;
          transition: background 0.2s, opacity 0.2s, transform 0.1s;
          user-select: none;
        }
        .btn:active:not(:disabled) {
          transform: scale(0.98);
        }
        .btn:disabled {
          opacity: 0.45;
          cursor: not-allowed;
        }
        .btn-primary {
          background: var(--primary-color, #03a9f4);
          color: var(--text-primary-color, white);
          min-width: 140px;
        }
        .btn-primary:hover:not(:disabled) {
          filter: brightness(1.08);
        }
        .btn-secondary {
          background: transparent;
          color: var(--primary-color, #03a9f4);
          border: 1px solid var(--primary-color, #03a9f4);
        }
        .btn-secondary:hover:not(:disabled) {
          background: color-mix(in srgb, var(--primary-color, #03a9f4) 8%, transparent);
        }
        .btn-remove {
          background: none;
          border: none;
          color: var(--secondary-text-color, #999);
          font-size: 22px;
          padding: 4px 8px;
          cursor: pointer;
          border-radius: 4px;
          line-height: 1;
          transition: color 0.2s, background 0.2s;
        }
        .btn-remove:hover {
          color: var(--error-color, #db4437);
          background: color-mix(in srgb, var(--error-color, #db4437) 10%, transparent);
        }
        .actions {
          display: flex;
          gap: 12px;
          margin-top: 20px;
          align-items: center;
        }
        .results {
          margin-top: 20px;
        }
        .result-item {
          padding: 14px 16px;
          border-radius: 8px;
          margin-bottom: 10px;
          font-size: 14px;
          line-height: 1.6;
        }
        .result-success {
          background: color-mix(in srgb, var(--success-color, #4caf50) 12%, transparent);
          border: 1px solid color-mix(in srgb, var(--success-color, #4caf50) 30%, transparent);
          color: var(--primary-text-color);
        }
        .result-success .result-icon { color: var(--success-color, #4caf50); }
        .result-error {
          background: color-mix(in srgb, var(--error-color, #db4437) 12%, transparent);
          border: 1px solid color-mix(in srgb, var(--error-color, #db4437) 30%, transparent);
          color: var(--primary-text-color);
        }
        .result-error .result-icon { color: var(--error-color, #db4437); }
        .result-header {
          display: flex;
          align-items: center;
          gap: 8px;
          font-weight: 500;
          margin-bottom: 4px;
        }
        .result-icon {
          font-size: 18px;
        }
        .result-details {
          font-size: 13px;
          color: var(--secondary-text-color);
          padding-left: 26px;
        }
        .result-stat-grid {
          display: grid;
          grid-template-columns: auto 1fr;
          gap: 3px 12px;
          padding-left: 26px;
          margin-top: 6px;
          font-size: 13px;
        }
        .result-stat-value {
          font-weight: 600;
          color: var(--primary-text-color);
          text-align: right;
        }
        .result-stat-label {
          color: var(--secondary-text-color);
        }
        .result-stat-grid > .result-stat-label:first-child,
        .result-stat-grid > .result-stat-label[style*="margin-top"] {
          font-weight: 600;
          color: var(--primary-text-color);
          font-size: 12px;
          text-transform: uppercase;
          letter-spacing: 0.5px;
        }
        .result-stat-error {
          color: var(--error-color, #db4437);
          grid-column: 1 / -1;
          margin-top: 4px;
        }
        .result-stat-range {
          color: var(--secondary-text-color);
          font-size: 12px;
          font-style: italic;
          margin-top: 2px;
          padding-top: 2px;
        }
        .spinner {
          display: inline-block;
          width: 16px;
          height: 16px;
          border: 2px solid rgba(255,255,255,0.3);
          border-top-color: white;
          border-radius: 50%;
          animation: spin 0.8s linear infinite;
          vertical-align: middle;
          margin-right: 8px;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
        .empty-state {
          text-align: center;
          padding: 32px 16px;
          color: var(--secondary-text-color);
          font-size: 14px;
        }

        .bulk-section {
          margin-bottom: 16px;
        }
        .bulk-toggle {
          background: none;
          border: none;
          color: var(--primary-color, #03a9f4);
          font-size: 13px;
          cursor: pointer;
          padding: 4px 0;
          display: flex;
          align-items: center;
          gap: 6px;
          font-family: inherit;
        }
        .bulk-toggle:hover {
          text-decoration: underline;
        }
        .bulk-toggle .chevron {
          display: inline-block;
          transition: transform 0.2s;
          font-size: 10px;
        }
        .bulk-toggle .chevron.open {
          transform: rotate(90deg);
        }
        .bulk-body {
          display: none;
          margin-top: 10px;
        }
        .bulk-body.open {
          display: block;
        }
        .bulk-body textarea {
          width: 100%;
          min-height: 100px;
          padding: 10px 12px;
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 8px;
          font-size: 13px;
          font-family: "Roboto Mono", "Consolas", "Monaco", monospace;
          background: var(--input-fill-color, var(--secondary-background-color, #f5f5f5));
          color: var(--primary-text-color);
          box-sizing: border-box;
          resize: vertical;
          line-height: 1.6;
        }
        .bulk-body textarea::placeholder {
          color: var(--secondary-text-color, #999);
          opacity: 0.8;
          font-family: inherit;
        }
        .bulk-body textarea:focus {
          outline: none;
          border-color: var(--primary-color, #03a9f4);
          box-shadow: 0 0 0 1px var(--primary-color, #03a9f4);
        }
        .bulk-hint {
          font-size: 12px;
          color: var(--secondary-text-color);
          margin-top: 6px;
          line-height: 1.5;
        }
        .bulk-actions {
          margin-top: 10px;
          display: flex;
          align-items: center;
          gap: 12px;
        }
        .bulk-error {
          margin-top: 10px;
          padding: 10px 14px;
          border-radius: 6px;
          font-size: 13px;
          background: color-mix(in srgb, var(--error-color, #db4437) 12%, transparent);
          border: 1px solid color-mix(in srgb, var(--error-color, #db4437) 30%, transparent);
          color: var(--primary-text-color);
          line-height: 1.6;
        }
        .bulk-error code {
          background: color-mix(in srgb, var(--error-color, #db4437) 8%, transparent);
          padding: 1px 5px;
          border-radius: 3px;
          font-size: 12px;
        }

        .options-section {
          margin-top: 14px;
          padding: 14px 16px;
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 10px;
          background: var(--secondary-background-color, #f5f5f5);
        }
        .option-row {
          display: flex;
          align-items: center;
          gap: 8px;
          font-size: 13px;
          flex-wrap: wrap;
          cursor: pointer;
        }
        .option-row input[type="checkbox"] {
          width: 16px;
          height: 16px;
          accent-color: var(--primary-color, #03a9f4);
          cursor: pointer;
          margin: 0;
        }
        .option-row .option-label {
          color: var(--primary-text-color);
          font-weight: 500;
        }
        .option-row.sub-row {
          margin-top: 10px;
          padding-left: 24px;
          cursor: default;
          transition: opacity 0.15s;
        }
        .option-row.sub-row.disabled {
          opacity: 0.5;
          pointer-events: none;
        }
        .option-row.sub-row .option-label {
          font-weight: 400;
          color: var(--secondary-text-color);
        }
        .option-row input[type="number"] {
          width: 70px;
          padding: 6px 8px;
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 6px;
          font-size: 13px;
          background: var(--ha-card-background, var(--card-background-color, white));
          color: var(--primary-text-color);
          box-sizing: border-box;
        }
        .option-row input[type="number"]:focus {
          outline: none;
          border-color: var(--primary-color, #03a9f4);
          box-shadow: 0 0 0 1px var(--primary-color, #03a9f4);
        }
        .option-row .option-unit {
          color: var(--secondary-text-color);
          font-size: 13px;
        }
        .option-row .option-hint {
          color: var(--secondary-text-color);
          font-size: 12px;
          flex-basis: 100%;
          margin-top: 4px;
          margin-left: 0;
          line-height: 1.5;
        }

        @media (max-width: 600px) {
          .pair-row {
            flex-direction: column;
            gap: 8px;
          }
          .arrow-col {
            padding-top: 0;
            justify-content: center;
            transform: rotate(90deg);
          }
          .remove-col {
            padding-top: 0;
            align-self: flex-end;
          }
        }
      </style>
      <div class="card">
        <div class="header">
          <span class="header-icon">&#128337;</span>
          <h1>Merge Sensor History</h1>
        </div>
        <p class="subtitle">
          Import historical data from source sensors into destination sensors.<br/>
          Only data older than the destination's oldest record will be imported &mdash; no duplicates.
        </p>
        <div class="warning-banner">
          <span class="warn-icon">&#9888;</span>
          <span>
            This writes directly to the recorder database.
            <strong>Back up your database</strong> before importing.
            Imported states will appear in history graphs after the next recorder refresh.
          </span>
        </div>
        <div class="filter-row">
          <span class="search-icon">&#128269;</span>
          <input type="text" id="entity-filter" placeholder="Filter entities by name or ID..." />
        </div>
        <div class="bulk-section">
          <button class="bulk-toggle" id="bulk-toggle">
            <span class="chevron" id="bulk-chevron">&#9654;</span>
            Bulk add pairs
          </button>
          <div class="bulk-body" id="bulk-body">
            <textarea id="bulk-textarea" placeholder="sensor.old_temp, sensor.new_temp&#10;sensor.old_humidity&#9;sensor.new_humidity&#10;..."></textarea>
            <div class="bulk-hint">
              One pair per line. Separate source and destination with a <strong>comma</strong> or <strong>tab</strong>.
            </div>
            <div class="bulk-actions">
              <button class="btn btn-secondary" id="bulk-add-btn">Add Pairs</button>
            </div>
            <div id="bulk-error"></div>
          </div>
        </div>
        <div id="pairs-container"></div>
        <div class="options-section">
          <label class="option-row" title="By default, only data older than the destination's oldest existing entry is imported, to avoid duplicates. Enable this to also fill quiet periods inside the destination's existing time range.">
            <input type="checkbox" id="fill-gaps-cb" />
            <span class="option-label">Fill mid-stream gaps in the destination's existing time range</span>
          </label>
          <div class="option-row sub-row" id="gap-threshold-row">
            <span class="option-label">Gap threshold:</span>
            <input type="number" id="gap-threshold" min="1" max="1440" value="60" />
            <span class="option-unit">minutes</span>
            <span class="option-hint">&mdash; a gap is any period this long where the destination has no state but the source does</span>
          </div>
        </div>
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
    this._bulkBody = shadow.getElementById("bulk-body");
    this._bulkChevron = shadow.getElementById("bulk-chevron");
    this._bulkTextarea = shadow.getElementById("bulk-textarea");
    this._bulkError = shadow.getElementById("bulk-error");
    this._fillGapsCb = shadow.getElementById("fill-gaps-cb");
    this._gapThreshold = shadow.getElementById("gap-threshold");
    this._gapThresholdRow = shadow.getElementById("gap-threshold-row");

    const syncGapThresholdEnabled = () => {
      this._gapThresholdRow.classList.toggle(
        "disabled",
        !this._fillGapsCb.checked
      );
      this._gapThreshold.disabled = !this._fillGapsCb.checked;
    };
    syncGapThresholdEnabled();
    this._fillGapsCb.addEventListener("change", syncGapThresholdEnabled);

    shadow.getElementById("add-pair-btn").addEventListener("click", () => {
      this._pairs.push({ source: "", destination: "" });
      this._renderPairs();
    });

    this._importBtn.addEventListener("click", () => this._doImport());

    this._filterInput.addEventListener("input", () => {
      this._renderPairs();
    });

    shadow.getElementById("bulk-toggle").addEventListener("click", () => {
      const open = this._bulkBody.classList.toggle("open");
      this._bulkChevron.classList.toggle("open", open);
    });

    shadow.getElementById("bulk-add-btn").addEventListener("click", () => {
      this._handleBulkAdd();
    });

    this._renderPairs();
  }

  _getFilteredEntities() {
    if (!this._hass) return [];
    const filter = (this._filterInput?.value || "").toLowerCase();
    const entities = Object.keys(this._hass.states).sort();
    if (!filter) return entities;
    return entities.filter((e) => {
      if (e.toLowerCase().includes(filter)) return true;
      const name = this._friendlyName(e);
      return name && name.toLowerCase().includes(filter);
    });
  }

  _buildOptions(entities, selected) {
    let opts = '<option value="">-- Select entity --</option>';
    const seen = new Set();
    if (selected && !entities.includes(selected)) {
      const name = this._friendlyName(selected);
      const label = name ? `${selected} (${name}) [filtered]` : `${selected} [filtered]`;
      opts += `<option value="${selected}" selected>${label}</option>`;
      seen.add(selected);
    }
    for (const e of entities) {
      if (seen.has(e)) continue;
      const name = this._friendlyName(e);
      const label = name ? `${e} (${name})` : e;
      opts += `<option value="${e}" ${e === selected ? "selected" : ""}>${label}</option>`;
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

      // --- Source column ---
      const sourceCol = document.createElement("div");
      sourceCol.className = "entity-col";
      const sourceLabel = document.createElement("label");
      sourceLabel.textContent = "Source (old sensor)";
      const sourceSelect = document.createElement("select");
      sourceSelect.innerHTML = this._buildOptions(entities, pair.source);

      const sourceInfo = document.createElement("div");
      sourceInfo.className = "entity-info";
      sourceInfo.textContent = this._friendlyName(pair.source);

      sourceSelect.addEventListener("change", (ev) => {
        this._pairs[index].source = ev.target.value;
        sourceInfo.textContent = this._friendlyName(ev.target.value);
      });
      sourceCol.appendChild(sourceLabel);
      sourceCol.appendChild(sourceSelect);
      sourceCol.appendChild(sourceInfo);

      // --- Arrow ---
      const arrow = document.createElement("div");
      arrow.className = "arrow-col";
      arrow.innerHTML = "&#8594;";

      // --- Destination column ---
      const destCol = document.createElement("div");
      destCol.className = "entity-col";
      const destLabel = document.createElement("label");
      destLabel.textContent = "Destination (new sensor)";
      const destSelect = document.createElement("select");
      destSelect.innerHTML = this._buildOptions(entities, pair.destination);

      const destInfo = document.createElement("div");
      destInfo.className = "entity-info";
      destInfo.textContent = this._friendlyName(pair.destination);

      destSelect.addEventListener("change", (ev) => {
        this._pairs[index].destination = ev.target.value;
        destInfo.textContent = this._friendlyName(ev.target.value);
      });
      destCol.appendChild(destLabel);
      destCol.appendChild(destSelect);
      destCol.appendChild(destInfo);

      // --- Remove button ---
      const removeCol = document.createElement("div");
      removeCol.className = "remove-col";
      const removeBtn = document.createElement("button");
      removeBtn.className = "btn-remove";
      removeBtn.innerHTML = "&#215;";
      removeBtn.title = "Remove pair";
      removeBtn.addEventListener("click", () => {
        if (this._pairs.length > 1) {
          this._pairs.splice(index, 1);
          this._renderPairs();
        }
      });
      removeCol.appendChild(removeBtn);

      row.appendChild(sourceCol);
      row.appendChild(arrow);
      row.appendChild(destCol);
      row.appendChild(removeCol);
      container.appendChild(row);
    });
  }

  /** Strip invisible/non-printable characters and normalize whitespace. */
  _cleanId(raw) {
    // Remove everything that isn't a printable ASCII char (entity IDs are
    // domain.object_id — only lowercase alphanumeric, underscores, dots).
    // This catches non-breaking spaces, zero-width chars, smart quotes, BOM, etc.
    return raw.replace(/[^\x09\x20-\x7E]/g, "").trim();
  }

  _handleBulkAdd() {
    const text = this._bulkTextarea.value.trim();
    this._bulkError.innerHTML = "";

    if (!text) {
      this._bulkError.innerHTML = '<div class="bulk-error">Please enter at least one pair.</div>';
      return;
    }

    const knownEntities = this._hass ? new Set(Object.keys(this._hass.states)) : new Set();
    const parsed = [];
    const parseErrors = [];
    const invalidIds = new Set();

    const lines = text.split(/\r?\n/);
    for (let i = 0; i < lines.length; i++) {
      const line = this._cleanId(lines[i]);
      if (!line) continue;

      // Split by tab first, then comma
      let parts;
      if (line.includes("\t")) {
        parts = line.split("\t").map((s) => s.trim()).filter(Boolean);
      } else {
        parts = line.split(",").map((s) => s.trim()).filter(Boolean);
      }

      if (parts.length !== 2) {
        parseErrors.push(`Line ${i + 1}: expected 2 entities, got ${parts.length} &mdash; <code>${line}</code>`);
        continue;
      }

      const [source, destination] = parts;
      if (!knownEntities.has(source)) invalidIds.add(source);
      if (!knownEntities.has(destination)) invalidIds.add(destination);
      parsed.push({ source, destination });
    }

    if (parseErrors.length > 0) {
      this._bulkError.innerHTML = `<div class="bulk-error"><strong>Could not parse:</strong><br/>${parseErrors.join("<br/>")}</div>`;
      return;
    }

    if (invalidIds.size > 0) {
      const list = [...invalidIds].map((id) => `<code>${id}</code>`).join(", ");
      this._bulkError.innerHTML = `<div class="bulk-error"><strong>Unknown entity IDs:</strong> ${list}<br/>No pairs were added. Please fix the IDs and try again.</div>`;
      return;
    }

    if (parsed.length === 0) {
      this._bulkError.innerHTML = '<div class="bulk-error">No valid pairs found in the input.</div>';
      return;
    }

    // Remove the initial empty pair if it's still the only one and untouched
    if (this._pairs.length === 1 && !this._pairs[0].source && !this._pairs[0].destination) {
      this._pairs = [];
    }

    this._pairs.push(...parsed);
    this._bulkTextarea.value = "";
    this._bulkBody.classList.remove("open");
    this._bulkChevron.classList.remove("open");
    this._renderPairs();
  }

  async _doImport() {
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

    const fillGaps = !!this._fillGapsCb.checked;
    let gapThresholdMinutes = Number(this._gapThreshold.value);
    if (fillGaps) {
      if (
        !Number.isFinite(gapThresholdMinutes) ||
        gapThresholdMinutes < 1 ||
        gapThresholdMinutes > 1440
      ) {
        alert("Gap threshold must be a number between 1 and 1440 minutes.");
        return;
      }
    } else {
      gapThresholdMinutes = 60;
    }

    const pairLines = validPairs
      .map((p) => {
        const sn = this._friendlyName(p.source);
        const dn = this._friendlyName(p.destination);
        const src = sn ? `${p.source} (${sn})` : p.source;
        const dst = dn ? `${p.destination} (${dn})` : p.destination;
        return `  ${src}  \u2192  ${dst}`;
      })
      .join("\n");

    const gapsLine = fillGaps
      ? `\n\nMid-stream & trailing gap-fill: ON (threshold ${gapThresholdMinutes} min)`
      : "";

    if (
      !confirm(
        `Import history for ${validPairs.length} pair(s)?\n\n` +
          pairLines +
          gapsLine +
          "\n\nThis will write to your recorder database."
      )
    ) {
      return;
    }

    this._importing = true;
    this._importBtn.disabled = true;
    this._importBtn.innerHTML = '<span class="spinner"></span>Importing\u2026';
    this._resultsContainer.innerHTML = "";

    try {
      const response = await this._hass.callWS({
        type: "merge_sensor_history/import",
        pairs: validPairs,
        fill_gaps: fillGaps,
        gap_threshold_minutes: gapThresholdMinutes,
      });

      this._renderResults(response.results);
    } catch (err) {
      this._resultsContainer.innerHTML = `
        <div class="result-item result-error">
          <div class="result-header">
            <span class="result-icon">&#10060;</span>
            Import failed
          </div>
          <div class="result-details">${err.message || err}</div>
        </div>`;
    } finally {
      this._importing = false;
      this._importBtn.disabled = false;
      this._importBtn.textContent = "Import History";
    }
  }

  /** Format an ISO datetime string for display. Returns "" if null. */
  _formatTs(iso) {
    if (!iso) return "";
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toLocaleString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  /** Format a signed numeric offset with a unit for display. */
  _formatOffset(offset, unit) {
    if (offset === null || offset === undefined) return "";
    const abs = Math.abs(offset);
    // Use sensible precision: more decimals for small numbers, fewer for large.
    let formatted;
    if (abs >= 1000) formatted = abs.toLocaleString(undefined, { maximumFractionDigits: 2 });
    else if (abs >= 1) formatted = abs.toLocaleString(undefined, { maximumFractionDigits: 3 });
    else formatted = abs.toLocaleString(undefined, { maximumFractionDigits: 6 });
    const sign = offset >= 0 ? "+" : "\u2212";
    return unit ? `${sign}${formatted} ${unit}` : `${sign}${formatted}`;
  }

  _renderResults(results) {
    this._resultsContainer.innerHTML = results
      .map((r) => {
        const srcName = this._friendlyName(r.source);
        const dstName = this._friendlyName(r.destination);
        const srcLabel = srcName ? `${r.source} (${srcName})` : r.source;
        const dstLabel = dstName ? `${r.destination} (${dstName})` : r.destination;
        const pairLabel = `${srcLabel} \u2192 ${dstLabel}`;

        if (r.error) {
          return `<div class="result-item result-error">
            <div class="result-header">
              <span class="result-icon">&#10060;</span>
              ${pairLabel}
            </div>
            <div class="result-details">
              ${r.error}<br/>
              <em>No data was written &mdash; the import was rolled back.</em>
            </div>
          </div>`;
        }

        const nothingImported =
          r.states_imported === 0 &&
          r.stats_imported === 0 &&
          (r.stats_short_imported || 0) === 0 &&
          !r.stats_error &&
          !r.stats_short_error;
        const noSourceData =
          (r.states_source_total || 0) === 0 &&
          (r.stats_source_total || 0) === 0 &&
          (r.stats_short_source_total || 0) === 0;

        if (nothingImported && noSourceData) {
          return `<div class="result-item result-success">
            <div class="result-header">
              <span class="result-icon">&#9989;</span>
              ${pairLabel}
            </div>
            <div class="result-details">No source data found &mdash; nothing to import.</div>
          </div>`;
        }

        let grid = "";

        // --- States summary ---
        if (r.states_source_total > 0) {
          grid += `<span class="result-stat-label">States</span><span class="result-stat-label"></span>`;
          grid += `<span class="result-stat-value">${r.states_source_total.toLocaleString()}</span><span class="result-stat-label">total in source</span>`;
          if (r.states_already_covered > 0)
            grid += `<span class="result-stat-value">${r.states_already_covered.toLocaleString()}</span><span class="result-stat-label">already present in destination</span>`;
          grid += `<span class="result-stat-value">${r.states_imported.toLocaleString()}</span><span class="result-stat-label">imported</span>`;
          if (r.states_mid_stream_filled > 0)
            grid += `<span class="result-stat-value">${r.states_mid_stream_filled.toLocaleString()}</span><span class="result-stat-label">&nbsp;&nbsp;&mdash; mid-stream gap-fill</span>`;
          if (r.states_trailing_filled > 0)
            grid += `<span class="result-stat-value">${r.states_trailing_filled.toLocaleString()}</span><span class="result-stat-label">&nbsp;&nbsp;&mdash; trailing fill (past destination's newest)</span>`;
          if (r.states_source_skipped_non_good > 0)
            grid += `<span class="result-stat-value">${r.states_source_skipped_non_good.toLocaleString()}</span><span class="result-stat-label">skipped (source was unavailable/unknown in a gap)</span>`;
          // Diagnostic block: helps the user see WHY nothing was filled.
          // Only shown when the user enabled gap-fill (dest_total_rows > 0).
          if (r.states_dest_total_rows > 0) {
            const hidden = r.states_dest_total_rows - r.states_dest_good_rows;
            const diag = `Destination history: ${r.states_dest_total_rows.toLocaleString()} rows total, ${r.states_dest_good_rows.toLocaleString()} good, ${hidden.toLocaleString()} hidden (unavailable/unknown). Gap intervals \u2265 threshold detected: <strong>${r.states_gap_intervals_count.toLocaleString()}</strong>.`;
            grid += `<span class="result-stat-range" style="grid-column:1/-1">${diag}</span>`;
          }
          if (r.states_imported_start && r.states_imported_end) {
            const range = `${this._formatTs(r.states_imported_start)} \u2192 ${this._formatTs(r.states_imported_end)}`;
            grid += `<span class="result-stat-range" style="grid-column:1/-1">${range}</span>`;
          }
        }

        // --- Statistics summary ---
        // Always show this section if source has any stats data, even when
        // nothing was imported — the user needs to see WHY (e.g. all rows
        // already complete, or skipped as too recent).
        const hasStatsInfo =
          r.stats_source_total > 0 || r.stats_imported > 0 || r.stats_error;
        if (hasStatsInfo) {
          grid += `<span class="result-stat-label" style="margin-top:6px">Long-term statistics (hourly)</span><span class="result-stat-label"></span>`;
          if (r.stats_source_total > 0)
            grid += `<span class="result-stat-value">${r.stats_source_total.toLocaleString()}</span><span class="result-stat-label">total in source</span>`;
          if (r.stats_already_covered > 0)
            grid += `<span class="result-stat-value">${r.stats_already_covered.toLocaleString()}</span><span class="result-stat-label">already complete in destination</span>`;
          if (r.stats_gap_filled > 0)
            grid += `<span class="result-stat-value">${r.stats_gap_filled.toLocaleString()}</span><span class="result-stat-label">gap-filled (NULL columns in destination)</span>`;
          if (r.stats_skipped_recent > 0)
            grid += `<span class="result-stat-value">${r.stats_skipped_recent.toLocaleString()}</span><span class="result-stat-label">skipped (recent &mdash; not yet compiled by HA)</span>`;
          grid += `<span class="result-stat-value">${(r.stats_imported || 0).toLocaleString()}</span><span class="result-stat-label">total imported</span>`;
          if (r.stats_imported_start && r.stats_imported_end) {
            const range = `${this._formatTs(r.stats_imported_start)} \u2192 ${this._formatTs(r.stats_imported_end)}`;
            grid += `<span class="result-stat-range" style="grid-column:1/-1">${range}</span>`;
          }
          if (r.stats_sum_offset !== null && r.stats_sum_offset !== undefined) {
            const offsetStr = this._formatOffset(r.stats_sum_offset, r.stats_unit);
            grid += `<span class="result-stat-range" style="grid-column:1/-1">Cumulative-sum offset applied: <strong>${offsetStr}</strong> (aligns energy totals at splice point)</span>`;
          }
          if (r.stats_error)
            grid += `<span class="result-stat-error">Error: ${r.stats_error}</span>`;
        }

        // --- Short-term statistics summary (only shown when backfill ran) ---
        const hasShortInfo =
          (r.stats_short_source_total || 0) > 0 ||
          (r.stats_short_imported || 0) > 0 ||
          r.stats_short_error;
        if (hasShortInfo) {
          grid += `<span class="result-stat-label" style="margin-top:6px">Short-term statistics (5-min)</span><span class="result-stat-label"></span>`;
          if (r.stats_short_source_total > 0)
            grid += `<span class="result-stat-value">${r.stats_short_source_total.toLocaleString()}</span><span class="result-stat-label">total in source</span>`;
          if (r.stats_short_already_covered > 0)
            grid += `<span class="result-stat-value">${r.stats_short_already_covered.toLocaleString()}</span><span class="result-stat-label">already complete in destination</span>`;
          if (r.stats_short_skipped_recent > 0)
            grid += `<span class="result-stat-value">${r.stats_short_skipped_recent.toLocaleString()}</span><span class="result-stat-label">skipped (too recent or under threshold)</span>`;
          grid += `<span class="result-stat-value">${(r.stats_short_imported || 0).toLocaleString()}</span><span class="result-stat-label">imported</span>`;
          if (r.stats_short_imported_start && r.stats_short_imported_end) {
            const range = `${this._formatTs(r.stats_short_imported_start)} \u2192 ${this._formatTs(r.stats_short_imported_end)}`;
            grid += `<span class="result-stat-range" style="grid-column:1/-1">${range}</span>`;
          }
          if (r.stats_short_error)
            grid += `<span class="result-stat-error">Error: ${r.stats_short_error}</span>`;
        }

        return `<div class="result-item result-success">
          <div class="result-header">
            <span class="result-icon">&#9989;</span>
            ${pairLabel}
          </div>
          <div class="result-stat-grid">${grid}</div>
        </div>`;
      })
      .join("");
  }
}

customElements.define("merge-sensor-history-panel", MergeSensorsHistoryPanel);
