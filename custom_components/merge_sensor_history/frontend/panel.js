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
    this._debugByPair = new Map();
    // Deleted entities: orphaned recorder statistics whose entity is gone from
    // the state machine. Fetched lazily via recorder/list_statistic_ids when
    // the "Show deleted entities" toggle is first enabled.
    this._showDeleted = false;
    this._deletedIds = []; // sorted list of orphaned statistic_ids
    this._deletedNames = new Map(); // id -> stored statistics name (may be "")
    this._deletedFetched = false;
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

  /** Get friendly name for an entity, or empty string if not found.
   *  Falls back to the stored statistics name for deleted entities (whose
   *  live state is gone), so the confirm dialog and results still show a name. */
  _friendlyName(entityId) {
    if (!entityId || !this._hass) return "";
    const stateObj = this._hass.states[entityId];
    if (stateObj) {
      const name = stateObj.attributes.friendly_name;
      return name && name !== entityId ? name : "";
    }
    const stored = this._deletedNames.get(entityId);
    return stored && stored !== entityId ? stored : "";
  }

  /** True if the id is a known deleted entity (orphaned statistics only). */
  _isDeleted(entityId) {
    return this._deletedNames.has(entityId);
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
        .filter-area {
          display: flex;
          align-items: center;
          gap: 12px;
          flex-wrap: wrap;
          margin-bottom: 16px;
        }
        .filter-row {
          position: relative;
          flex: 1;
          min-width: 200px;
        }
        .filter-mode-toggle {
          display: flex;
          align-items: center;
          gap: 6px;
          font-size: 12px;
          color: var(--secondary-text-color);
          cursor: pointer;
          white-space: nowrap;
          flex-shrink: 0;
          user-select: none;
        }
        .filter-mode-toggle input[type="checkbox"] {
          width: 15px;
          height: 15px;
          accent-color: var(--primary-color, #03a9f4);
          cursor: pointer;
          margin: 0;
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
        .btn-preview {
          background: var(--secondary-background-color, #f5f5f5);
          color: var(--primary-text-color);
          border: 1px solid var(--divider-color, #e0e0e0);
          margin-right: 8px;
        }
        .btn-preview:hover:not(:disabled) {
          background: var(--divider-color, #e0e0e0);
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
        .debug-dl-btn {
          background: transparent;
          border: 1px solid color-mix(in srgb, var(--primary-color, #03a9f4) 50%, transparent);
          color: var(--primary-color, #03a9f4);
          font-size: 10px;
          font-weight: 500;
          font-family: inherit;
          padding: 2px 8px;
          border-radius: 4px;
          margin-left: 8px;
          cursor: pointer;
          letter-spacing: 0.3px;
          text-transform: none;
          vertical-align: middle;
          transition: background 0.15s;
        }
        .debug-dl-btn:hover {
          background: color-mix(in srgb, var(--primary-color, #03a9f4) 12%, transparent);
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

        .deleted-toggle {
          display: flex;
          align-items: center;
          gap: 8px;
          font-size: 13px;
          color: var(--primary-text-color);
          cursor: pointer;
          user-select: none;
          margin-bottom: 12px;
        }
        .deleted-toggle input[type="checkbox"] {
          width: 15px;
          height: 15px;
          accent-color: var(--primary-color, #03a9f4);
          cursor: pointer;
          margin: 0;
          flex-shrink: 0;
        }
        .deleted-toggle .deleted-note {
          color: var(--secondary-text-color);
          font-size: 12px;
        }
        .deleted-toggle .deleted-status {
          color: var(--secondary-text-color);
          font-size: 12px;
          font-style: italic;
        }
        .deleted-toggle .deleted-status.err {
          color: var(--error-color, #db4437);
          font-style: normal;
        }
        .bulk-section {
          margin-bottom: 16px;
        }
        .bulk-toggle {
          width: 100%;
          box-sizing: border-box;
          background: var(--secondary-background-color, #f5f5f5);
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 10px;
          color: var(--primary-text-color);
          font-size: 14px;
          font-weight: 500;
          cursor: pointer;
          padding: 12px 16px;
          display: flex;
          align-items: center;
          gap: 10px;
          font-family: inherit;
          transition: border-color 0.2s;
        }
        .bulk-toggle:hover {
          border-color: color-mix(in srgb, var(--primary-color, #03a9f4) 45%, transparent);
        }
        .bulk-toggle .chevron {
          display: inline-block;
          transition: transform 0.2s;
          font-size: 10px;
          color: var(--primary-color, #03a9f4);
        }
        .bulk-subtitle {
          margin-left: auto;
          font-size: 12px;
          font-weight: 400;
          color: var(--secondary-text-color);
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

        .pair-actions {
          margin-top: 2px;
        }
        .options-title {
          font-size: 11px;
          font-weight: 600;
          letter-spacing: 0.8px;
          text-transform: uppercase;
          color: var(--secondary-text-color);
          margin-bottom: 10px;
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
        .option-row input[type="text"] {
          flex: 1;
          min-width: 160px;
          max-width: 320px;
          padding: 6px 8px;
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 6px;
          font-size: 13px;
          font-family: "Roboto Mono", "Consolas", "Monaco", monospace;
          background: var(--ha-card-background, var(--card-background-color, white));
          color: var(--primary-text-color);
          box-sizing: border-box;
        }
        .option-row input[type="text"]:focus {
          outline: none;
          border-color: var(--primary-color, #03a9f4);
          box-shadow: 0 0 0 1px var(--primary-color, #03a9f4);
        }
        .adjust-mode-label {
          display: flex;
          align-items: center;
          gap: 6px;
          font-size: 13px;
          color: var(--primary-text-color);
          font-weight: 500;
          cursor: pointer;
          white-space: nowrap;
        }
        .adjust-mode-label input[type="radio"] {
          accent-color: var(--primary-color, #03a9f4);
          cursor: pointer;
          margin: 0;
        }
        .option-hint.err {
          color: var(--error-color, #db4437);
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
          Only data older than the destination's oldest record will be imported &mdash; no duplicates.<br/>
          <strong>Tip:</strong> energy and its cost are tracked by <em>separate</em> sensors &mdash; add a pair for each, or past cost stays at 0.
        </p>
        <div class="warning-banner">
          <span class="warn-icon">&#9888;</span>
          <span>
            This writes directly to the recorder database.
            <strong>Back up your database</strong> before importing.
            Imported states will appear in history graphs after the next recorder refresh.
          </span>
        </div>
        <label class="deleted-toggle" id="deleted-toggle" title="Deleted entities are gone from Home Assistant but their long-term (hourly) statistics can still be in the recorder. Turn this on to select them as a source. Only statistics will be merged (raw state history and 5-minute data are purged after about 10 days).">
          <input type="checkbox" id="show-deleted-cb" />
          <span>Show deleted entities <span class="deleted-note">(statistics only)</span></span>
          <span class="deleted-status" id="deleted-status"></span>
        </label>
        <div class="bulk-section">
          <button class="bulk-toggle" id="bulk-toggle">
            <span class="chevron" id="bulk-chevron">&#9654;</span>
            Bulk add pairs
            <span class="bulk-subtitle">paste a list of source &#8594; destination pairs</span>
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
        <div class="filter-area">
          <div class="filter-row" id="single-filter-row">
            <span class="search-icon">&#128269;</span>
            <input type="text" id="entity-filter" placeholder="Filter entities by name or ID..." />
          </div>
          <div class="filter-row" id="source-filter-row" style="display:none">
            <span class="search-icon">&#128269;</span>
            <input type="text" id="source-filter" placeholder="Filter source entities..." />
          </div>
          <div class="filter-row" id="dest-filter-row" style="display:none">
            <span class="search-icon">&#128269;</span>
            <input type="text" id="dest-filter" placeholder="Filter destination entities..." />
          </div>
          <label class="filter-mode-toggle" title="When checked, one filter narrows both dropdowns. Uncheck to filter the source and destination lists separately &mdash; handy when only a serial number differs between the old and new sensors.">
            <input type="checkbox" id="shared-filter-cb" checked />
            Same filter for both
          </label>
        </div>
        <div id="pairs-container"></div>
        <div class="pair-actions">
          <button class="btn btn-secondary" id="add-pair-btn">+ Add Pair</button>
        </div>
        <div class="options-section">
          <div class="options-title">Options</div>
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
          <label class="option-row" style="margin-top:14px" title="Convert every numeric value read from the source (states and statistics) before importing. Use when the two sensors record the same quantity in different units.">
            <input type="checkbox" id="scale-cb" />
            <span class="option-label">Adjust imported values</span>
          </label>
          <div class="option-row sub-row" id="adjust-multiply-row">
            <label class="adjust-mode-label"><input type="radio" name="adjust-mode" id="adjust-mode-multiply" checked /> Multiply by:</label>
            <input type="number" id="scale-factor" step="any" min="0" value="1000" />
            <span class="option-hint">&mdash; e.g. 1000 for kWh &rarr; Wh, or 0.001 for Wh &rarr; kWh</span>
          </div>
          <div class="option-row sub-row" id="adjust-custom-row">
            <label class="adjust-mode-label"><input type="radio" name="adjust-mode" id="adjust-mode-custom" /> Custom function:</label>
            <input type="text" id="custom-fn" placeholder="v * 9/5 + 32" spellcheck="false" autocomplete="off" />
            <span class="option-hint" id="custom-fn-preview"></span>
            <span class="option-hint">&mdash; a math formula of <strong>v</strong> (the source value), evaluated safely (never as JavaScript). Allowed: numbers, + - * / % ^ ( ) and abs, round, floor, ceil, sqrt, log, log10, exp, min, max, pow, pi, e. Applied to states and statistics; energy totals are spliced after conversion; non-numeric states pass through. For cumulative (energy-style) sensors keep the formula linear, like a*v + b, so hourly deltas stay correct.</span>
          </div>
        </div>
        <div class="actions">
          <div style="flex:1"></div>
          <button class="btn btn-preview" id="preview-btn">Preview</button>
          <button class="btn btn-primary" id="import-btn">Import History</button>
        </div>
        <div id="results-container" class="results"></div>
      </div>
    `;

    this._pairsContainer = shadow.getElementById("pairs-container");
    this._resultsContainer = shadow.getElementById("results-container");
    this._importBtn = shadow.getElementById("import-btn");
    this._previewBtn = shadow.getElementById("preview-btn");
    this._filterInput = shadow.getElementById("entity-filter");
    this._sourceFilterInput = shadow.getElementById("source-filter");
    this._destFilterInput = shadow.getElementById("dest-filter");
    this._sharedFilterCb = shadow.getElementById("shared-filter-cb");
    this._singleFilterRow = shadow.getElementById("single-filter-row");
    this._sourceFilterRow = shadow.getElementById("source-filter-row");
    this._destFilterRow = shadow.getElementById("dest-filter-row");
    this._bulkBody = shadow.getElementById("bulk-body");
    this._bulkChevron = shadow.getElementById("bulk-chevron");
    this._bulkTextarea = shadow.getElementById("bulk-textarea");
    this._bulkError = shadow.getElementById("bulk-error");
    this._fillGapsCb = shadow.getElementById("fill-gaps-cb");
    this._gapThreshold = shadow.getElementById("gap-threshold");
    this._gapThresholdRow = shadow.getElementById("gap-threshold-row");
    this._scaleCb = shadow.getElementById("scale-cb");
    this._scaleFactor = shadow.getElementById("scale-factor");
    this._adjustMultiplyRow = shadow.getElementById("adjust-multiply-row");
    this._adjustCustomRow = shadow.getElementById("adjust-custom-row");
    this._adjustModeMultiply = shadow.getElementById("adjust-mode-multiply");
    this._adjustModeCustom = shadow.getElementById("adjust-mode-custom");
    this._customFn = shadow.getElementById("custom-fn");
    this._customFnPreview = shadow.getElementById("custom-fn-preview");
    this._showDeletedCb = shadow.getElementById("show-deleted-cb");
    this._deletedStatus = shadow.getElementById("deleted-status");

    this._showDeletedCb.addEventListener("change", () =>
      this._onShowDeletedChange()
    );

    const syncGapThresholdEnabled = () => {
      this._gapThresholdRow.classList.toggle(
        "disabled",
        !this._fillGapsCb.checked
      );
      this._gapThreshold.disabled = !this._fillGapsCb.checked;
    };
    syncGapThresholdEnabled();
    this._fillGapsCb.addEventListener("change", syncGapThresholdEnabled);

    const updateFnPreview = () => {
      const el = this._customFnPreview;
      const active =
        this._scaleCb.checked &&
        this._adjustModeCustom.checked &&
        this._customFn.value.trim();
      if (!active) {
        el.textContent = "";
        el.classList.remove("err");
        return;
      }
      try {
        const fn = this._compileMathExpr(this._customFn.value);
        const fmt = (x) => {
          try {
            return String(parseFloat(fn(x).toPrecision(10)));
          } catch (e) {
            return "error";
          }
        };
        el.classList.remove("err");
        el.textContent = `→ f(0) = ${fmt(0)}, f(1) = ${fmt(1)}, f(1000) = ${fmt(1000)}`;
      } catch (e) {
        el.classList.add("err");
        el.textContent = "⚠ " + (e.message || e);
      }
    };

    const syncScaleEnabled = () => {
      const on = this._scaleCb.checked;
      const multiply = this._adjustModeMultiply.checked;
      this._adjustMultiplyRow.classList.toggle("disabled", !on);
      this._adjustCustomRow.classList.toggle("disabled", !on);
      this._adjustModeMultiply.disabled = !on;
      this._adjustModeCustom.disabled = !on;
      this._scaleFactor.disabled = !on || !multiply;
      this._customFn.disabled = !on || multiply;
      updateFnPreview();
    };
    syncScaleEnabled();
    this._scaleCb.addEventListener("change", syncScaleEnabled);
    this._adjustModeMultiply.addEventListener("change", syncScaleEnabled);
    this._adjustModeCustom.addEventListener("change", syncScaleEnabled);
    this._customFn.addEventListener("input", updateFnPreview);

    shadow.getElementById("add-pair-btn").addEventListener("click", () => {
      this._pairs.push({ source: "", destination: "" });
      this._renderPairs();
    });

    this._importBtn.addEventListener("click", () => this._doImport());
    this._previewBtn.addEventListener("click", () => this._doImport(true));

    for (const el of [
      this._filterInput,
      this._sourceFilterInput,
      this._destFilterInput,
    ]) {
      el.addEventListener("input", () => {
        this._renderPairs();
      });
    }

    this._sharedFilterCb.addEventListener("change", () => {
      const shared = this._sharedFilterCb.checked;
      this._singleFilterRow.style.display = shared ? "" : "none";
      this._sourceFilterRow.style.display = shared ? "none" : "";
      this._destFilterRow.style.display = shared ? "none" : "";
      if (shared) {
        // Collapsing back: carry the source filter into the shared field.
        this._filterInput.value = this._sourceFilterInput.value;
      } else {
        // Splitting: seed both filters from the shared value so nothing
        // changes until the user edits one of them.
        this._sourceFilterInput.value = this._filterInput.value;
        this._destFilterInput.value = this._filterInput.value;
      }
      this._renderPairs();
    });

    shadow.getElementById("bulk-toggle").addEventListener("click", () => {
      const open = this._bulkBody.classList.toggle("open");
      this._bulkChevron.classList.toggle("open", open);
    });

    shadow.getElementById("bulk-add-btn").addEventListener("click", () => {
      this._handleBulkAdd();
    });

    this._resultsContainer.addEventListener("click", (ev) => {
      const btn = ev.target.closest(".debug-dl-btn");
      if (!btn) return;
      this._downloadDebug(btn.dataset.pair, btn.dataset.kind);
    });

    this._renderPairs();
  }

  /** All selectable ids: live entities, plus deleted (orphaned-stats) ids when
   *  the toggle is on. */
  _allEntityIds() {
    if (!this._hass) return [];
    const live = Object.keys(this._hass.states);
    if (this._showDeleted && this._deletedIds.length) {
      return [...live, ...this._deletedIds].sort();
    }
    return live.sort();
  }

  _getFilteredEntities(role) {
    if (!this._hass) return [];
    const shared = !this._sharedFilterCb || this._sharedFilterCb.checked;
    const input = shared
      ? this._filterInput
      : role === "destination"
        ? this._destFilterInput
        : this._sourceFilterInput;
    const filter = (input?.value || "").toLowerCase();
    const entities = this._allEntityIds();
    if (!filter) return entities;
    return entities.filter((e) => {
      if (e.toLowerCase().includes(filter)) return true;
      const name = this._friendlyName(e);
      return name && name.toLowerCase().includes(filter);
    });
  }

  /** Fetch orphaned recorder statistic_ids (deleted entities) once, then
   *  re-render. Returns via status text on the toggle. */
  async _onShowDeletedChange() {
    this._showDeleted = this._showDeletedCb.checked;
    if (!this._showDeleted) {
      this._deletedStatus.textContent = "";
      this._deletedStatus.classList.remove("err");
      this._renderPairs();
      return;
    }
    if (this._deletedFetched) {
      this._setDeletedStatus();
      this._renderPairs();
      return;
    }
    this._deletedStatus.classList.remove("err");
    this._deletedStatus.textContent = "loading…";
    try {
      const rows = await this._hass.callWS({
        type: "recorder/list_statistic_ids",
      });
      const live = this._hass.states;
      const isLive = (id) => Object.prototype.hasOwnProperty.call(live, id);
      this._deletedIds = [];
      this._deletedNames = new Map();
      for (const r of rows || []) {
        const id = r.statistic_id;
        // Only recorder-sourced sensor stats can be merged as an entity (an
        // entity-form id we can write to); external stats (colon ids) can't.
        // "Deleted" = has stats but no live entity.
        if (r.source === "recorder" && id && !isLive(id)) {
          this._deletedIds.push(id);
          this._deletedNames.set(id, r.name || "");
        }
      }
      this._deletedIds.sort();
      this._deletedFetched = true;
      this._setDeletedStatus();
    } catch (err) {
      this._deletedStatus.classList.add("err");
      this._deletedStatus.textContent =
        "could not load: " + (err.message || err);
      this._showDeleted = false;
      this._showDeletedCb.checked = false;
    }
    this._renderPairs();
  }

  _setDeletedStatus() {
    const n = this._deletedIds.length;
    this._deletedStatus.classList.remove("err");
    this._deletedStatus.textContent =
      n === 0
        ? "none found"
        : `${n} found (shown as “deleted” below)`;
  }

  /** Build a dropdown option label, tagging deleted (orphaned-stats) ids. */
  _optionLabel(e) {
    const name = this._friendlyName(e);
    if (this._isDeleted(e)) {
      return name
        ? `${e} (${name}) [deleted, statistics only]`
        : `${e} [deleted, statistics only]`;
    }
    return name ? `${e} (${name})` : e;
  }

  _buildOptions(entities, selected) {
    let opts = '<option value="">-- Select entity --</option>';
    const seen = new Set();
    if (selected && !entities.includes(selected)) {
      opts += `<option value="${selected}" selected>${this._optionLabel(selected)} [filtered]</option>`;
      seen.add(selected);
    }
    for (const e of entities) {
      if (seen.has(e)) continue;
      opts += `<option value="${e}" ${e === selected ? "selected" : ""}>${this._optionLabel(e)}</option>`;
    }
    return opts;
  }

  _renderPairs() {
    const sourceEntities = this._getFilteredEntities("source");
    const destEntities =
      !this._sharedFilterCb || this._sharedFilterCb.checked
        ? sourceEntities
        : this._getFilteredEntities("destination");
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
      sourceSelect.innerHTML = this._buildOptions(sourceEntities, pair.source);

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
      destSelect.innerHTML = this._buildOptions(destEntities, pair.destination);

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

    // Valid ids = live entities, plus deleted (orphaned-stats) ids when the
    // "Show deleted entities" toggle is on.
    const knownEntities = new Set(this._allEntityIds());
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

  /**
   * Compile a restricted math formula of `v` into a JS function — used for
   * the live preview and pre-submit validation.
   *
   * SECURITY: this is a hand-written recursive-descent parser over a
   * whitelist grammar (numbers, v/pi/e, + - * / % ^ **, parentheses, and a
   * fixed set of math functions). The input is NEVER passed to eval() or
   * Function(). The backend independently re-validates and interprets the
   * formula via Python's ast module, so the frontend check is convenience,
   * not the security boundary.
   */
  _compileMathExpr(src) {
    const FUNCS = {
      abs: { f: Math.abs, min: 1, max: 1 },
      round: { f: Math.round, min: 1, max: 1 },
      floor: { f: Math.floor, min: 1, max: 1 },
      ceil: { f: Math.ceil, min: 1, max: 1 },
      sqrt: { f: Math.sqrt, min: 1, max: 1 },
      log: {
        f: (x, b) => (b === undefined ? Math.log(x) : Math.log(x) / Math.log(b)),
        min: 1,
        max: 2,
      },
      log10: { f: Math.log10, min: 1, max: 1 },
      log2: { f: Math.log2, min: 1, max: 1 },
      exp: { f: Math.exp, min: 1, max: 1 },
      min: { f: Math.min, min: 2, max: 8 },
      max: { f: Math.max, min: 2, max: 8 },
      pow: { f: Math.pow, min: 2, max: 2 },
    };
    const CONSTS = { pi: Math.PI, e: Math.E };

    const s = String(src).trim().toLowerCase().replace(/math\./g, "");
    if (!s) throw new Error("The formula is empty.");
    if (s.length > 200)
      throw new Error("The formula is too long (max 200 characters).");

    const tokens = [];
    const re = /(\*\*|[+\-*/%^(),]|(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?|[a-z_][a-z0-9_]*)/g;
    let idx = 0;
    while (idx < s.length) {
      if (/\s/.test(s[idx])) {
        idx++;
        continue;
      }
      re.lastIndex = idx;
      const m = re.exec(s);
      if (!m || m.index !== idx)
        throw new Error(`Unexpected character '${s[idx]}'.`);
      tokens.push(m[0]);
      idx = re.lastIndex;
    }

    let pos = 0;
    let usesV = false;
    const peek = () => tokens[pos];
    const expect = (tok) => {
      if (tokens[pos] !== tok)
        throw new Error(
          `Expected '${tok}'` +
            (tokens[pos] !== undefined ? ` but found '${tokens[pos]}'` : "") +
            "."
        );
      pos++;
    };

    const parseExpr = () => {
      let left = parseTerm();
      while (peek() === "+" || peek() === "-") {
        const op = tokens[pos++];
        const l = left;
        const right = parseTerm();
        left = op === "+" ? (v) => l(v) + right(v) : (v) => l(v) - right(v);
      }
      return left;
    };
    const parseTerm = () => {
      let left = parseUnary();
      while (peek() === "*" || peek() === "/" || peek() === "%") {
        const op = tokens[pos++];
        const l = left;
        const right = parseUnary();
        if (op === "*") left = (v) => l(v) * right(v);
        else if (op === "/") left = (v) => l(v) / right(v);
        else left = (v) => l(v) % right(v);
      }
      return left;
    };
    const parseUnary = () => {
      if (peek() === "-") {
        pos++;
        const operand = parseUnary();
        return (v) => -operand(v);
      }
      if (peek() === "+") {
        pos++;
        return parseUnary();
      }
      return parsePower();
    };
    const parsePower = () => {
      const base = parsePrimary();
      if (peek() === "**" || peek() === "^") {
        pos++;
        const exp = parseUnary(); // right-associative, exponent may be signed
        return (v) => Math.pow(base(v), exp(v));
      }
      return base;
    };
    const parsePrimary = () => {
      const tok = peek();
      if (tok === undefined) throw new Error("The formula ends unexpectedly.");
      if (tok === "(") {
        pos++;
        const inner = parseExpr();
        expect(")");
        return inner;
      }
      if (/^(?:\d|\.\d)/.test(tok)) {
        pos++;
        const num = parseFloat(tok);
        return () => num;
      }
      if (/^[a-z_]/.test(tok)) {
        pos++;
        if (peek() === "(") {
          const spec = FUNCS[tok];
          if (!spec)
            throw new Error(
              `Unknown function '${tok}'. Allowed: ${Object.keys(FUNCS).sort().join(", ")}.`
            );
          pos++;
          const args = [parseExpr()];
          while (peek() === ",") {
            pos++;
            args.push(parseExpr());
          }
          expect(")");
          if (args.length < spec.min || args.length > spec.max)
            throw new Error(
              `${tok}() takes ${spec.min}` +
                (spec.max !== spec.min ? ` to ${spec.max}` : "") +
                " argument(s)."
            );
          return (v) => spec.f(...args.map((a) => a(v)));
        }
        if (tok === "v") {
          usesV = true;
          return (v) => v;
        }
        if (tok in CONSTS) {
          const c = CONSTS[tok];
          return () => c;
        }
        throw new Error(`Unknown name '${tok}' — only v, pi and e are allowed.`);
      }
      throw new Error(`Unexpected '${tok}'.`);
    };

    const fn = parseExpr();
    if (pos !== tokens.length) throw new Error(`Unexpected '${tokens[pos]}'.`);
    if (!usesV)
      throw new Error("The formula must use the variable v (the source value).");
    return (v) => {
      const r = fn(v);
      if (!Number.isFinite(r)) throw new Error("non-finite result");
      return r;
    };
  }

  async _doImport(dryRun = false) {
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

    const scaleEnabled = this._scaleCb.checked;
    let scaleFactor = null;
    let valueFunction = null;
    if (scaleEnabled) {
      if (this._adjustModeCustom.checked) {
        valueFunction = this._customFn.value.trim();
        try {
          this._compileMathExpr(valueFunction);
        } catch (e) {
          alert("Custom function error: " + (e.message || e));
          return;
        }
      } else {
        scaleFactor = parseFloat(this._scaleFactor.value);
        if (!Number.isFinite(scaleFactor) || scaleFactor <= 0) {
          alert(
            "Scaling factor must be a positive number (e.g. 1000 for kWh \u2192 Wh, 0.001 for Wh \u2192 kWh)."
          );
          return;
        }
      }
    }

    if (!dryRun) {
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

      let scaleLine = "";
      if (valueFunction) {
        scaleLine = `\n\nCustom function: f(v) = ${valueFunction} (applied to every imported value)`;
      } else if (scaleFactor !== null && scaleFactor !== 1) {
        scaleLine = `\n\nScaling factor: \u00d7${scaleFactor} (every imported value is multiplied)`;
      }

      if (
        !confirm(
          `Import history for ${validPairs.length} pair(s)?\n\n` +
            pairLines +
            gapsLine +
            scaleLine +
            "\n\nThis will write to your recorder database."
        )
      ) {
        return;
      }
    }

    this._importing = true;
    this._importBtn.disabled = true;
    this._previewBtn.disabled = true;
    if (dryRun) {
      this._previewBtn.innerHTML = '<span class="spinner"></span>Analyzing\u2026';
    } else {
      this._importBtn.innerHTML = '<span class="spinner"></span>Importing\u2026';
    }
    this._resultsContainer.innerHTML = "";

    try {
      const response = await this._hass.callWS({
        type: "merge_sensor_history/import",
        pairs: validPairs,
        fill_gaps: fillGaps,
        gap_threshold_minutes: gapThresholdMinutes,
        dry_run: dryRun,
        scale_factor: scaleFactor,
        value_function: valueFunction,
      });

      this._renderResults(response.results);
    } catch (err) {
      this._resultsContainer.innerHTML = `
        <div class="result-item result-error">
          <div class="result-header">
            <span class="result-icon">&#10060;</span>
            ${dryRun ? "Analysis" : "Import"} failed
          </div>
          <div class="result-details">${err.message || err}</div>
        </div>`;
    } finally {
      this._importing = false;
      this._importBtn.disabled = false;
      this._previewBtn.disabled = false;
      this._importBtn.textContent = "Import History";
      this._previewBtn.textContent = "Preview";
    }
  }

  _downloadDebug(pairKey, kind) {
    const pair = this._debugByPair.get(pairKey);
    if (!pair) return;
    const rows = pair[kind] || [];
    const sanitize = (s) => (s || "").replace(/[^a-zA-Z0-9._-]+/g, "_");
    const stamp = new Date()
      .toISOString()
      .replace(/[:.]/g, "-")
      .slice(0, 19);
    const filename = `merge_history__${sanitize(pair.source)}__to__${sanitize(
      pair.destination
    )}__${kind}__${stamp}.json`;
    const payload = {
      generated_at: new Date().toISOString(),
      source_entity_id: pair.source,
      destination_entity_id: pair.destination,
      kind,
      row_count: rows.length,
      rows,
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.style.display = "none";
    this.shadowRoot.appendChild(a);
    a.click();
    this.shadowRoot.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 1000);
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
    this._debugByPair.clear();
    results.forEach((r, i) => {
      this._debugByPair.set(`${i}`, {
        source: r.source,
        destination: r.destination,
        states: r.debug_states || [],
        stats: r.debug_stats || [],
        stats_short: r.debug_stats_short || [],
      });
    });

    this._resultsContainer.innerHTML = results
      .map((r, i) => {
        const srcName = this._friendlyName(r.source);
        const dstName = this._friendlyName(r.destination);
        const srcLabel = srcName ? `${r.source} (${srcName})` : r.source;
        const dstLabel = dstName ? `${r.destination} (${dstName})` : r.destination;
        const pairLabel = (r.dry_run ? "Preview: " : "") + `${srcLabel} \u2192 ${dstLabel}`;
        const actionVerb = r.dry_run ? "would be imported" : "imported";
        const pairKey = `${i}`;

        if (r.error) {
          return `<div class="result-item result-error">
            <div class="result-header">
              <span class="result-icon">&#10060;</span>
              ${pairLabel}
            </div>
            <div class="result-details">
              ${r.error}<br/>
              <em>No data was written &mdash; ${r.dry_run ? "this was a preview only." : "the import was rolled back."}</em>
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

        const dlBtn = (kind, count) =>
          count > 0
            ? `<button class="debug-dl-btn" data-pair="${pairKey}" data-kind="${kind}" title="Download per-row debug JSON for this section">&#x2B07; debug JSON (${count.toLocaleString()} rows)</button>`
            : "";

        // --- Deleted / statistics-only source notice ---
        if (r.states_source_missing) {
          grid += `<span class="result-stat-range" style="grid-column:1/-1">Source has no raw state history (a deleted entity, or states purged). Only statistics ${r.dry_run ? "would be" : "were"} merged; the History panel stays empty while the Energy dashboard and long-term graphs are filled.</span>`;
        }

        // --- Value adjustment notice ---
        if (r.value_function) {
          const esc = String(r.value_function)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;");
          grid += `<span class="result-stat-range" style="grid-column:1/-1">Custom function ${r.dry_run ? "to be applied" : "applied"}: <strong>f(v) = ${esc}</strong> &mdash; every numeric source value ${r.dry_run ? "will be" : "was"} converted before import.</span>`;
        } else if (r.scale_factor !== null && r.scale_factor !== undefined) {
          grid += `<span class="result-stat-range" style="grid-column:1/-1">Scaling factor ${r.dry_run ? "to be applied" : "applied"}: <strong>&times;${r.scale_factor}</strong> &mdash; every numeric source value ${r.dry_run ? "will be" : "was"} multiplied before import.</span>`;
        }

        // --- States summary ---
        if (r.states_source_total > 0) {
          grid += `<span class="result-stat-label">States ${dlBtn("states", (r.debug_states || []).length)}</span><span class="result-stat-label"></span>`;
          grid += `<span class="result-stat-value">${r.states_source_total.toLocaleString()}</span><span class="result-stat-label">total in source</span>`;
          if (r.states_already_covered > 0)
            grid += `<span class="result-stat-value">${r.states_already_covered.toLocaleString()}</span><span class="result-stat-label">already present in destination</span>`;
          grid += `<span class="result-stat-value">${r.states_imported.toLocaleString()}</span><span class="result-stat-label">${actionVerb}</span>`;
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
          grid += `<span class="result-stat-label" style="margin-top:6px">Long-term statistics (hourly) ${dlBtn("stats", (r.debug_stats || []).length)}</span><span class="result-stat-label"></span>`;
          if (r.stats_source_total > 0)
            grid += `<span class="result-stat-value">${r.stats_source_total.toLocaleString()}</span><span class="result-stat-label">total in source</span>`;
          if (r.stats_already_covered > 0)
            grid += `<span class="result-stat-value">${r.stats_already_covered.toLocaleString()}</span><span class="result-stat-label">already complete in destination</span>`;
          if (r.stats_gap_filled > 0)
            grid += `<span class="result-stat-value">${r.stats_gap_filled.toLocaleString()}</span><span class="result-stat-label">gap-filled (NULL columns in destination)</span>`;
          if (r.stats_skipped_recent > 0)
            grid += `<span class="result-stat-value">${r.stats_skipped_recent.toLocaleString()}</span><span class="result-stat-label">skipped (recent &mdash; not yet compiled by HA)</span>`;
          grid += `<span class="result-stat-value">${(r.stats_imported || 0).toLocaleString()}</span><span class="result-stat-label">total ${actionVerb}</span>`;
          if (r.stats_imported_start && r.stats_imported_end) {
            const range = `${this._formatTs(r.stats_imported_start)} \u2192 ${this._formatTs(r.stats_imported_end)}`;
            grid += `<span class="result-stat-range" style="grid-column:1/-1">${range}</span>`;
          }
          if (r.stats_realigned_by !== null && r.stats_realigned_by !== undefined) {
            const liftStr = this._formatOffset(r.stats_realigned_by, r.stats_unit);
            grid += r.dry_run
              ? `<span class="result-stat-range" style="grid-column:1/-1">Destination running total would be realigned by <strong>${liftStr}</strong> so the imported history and existing data form one continuous energy series.</span>`
              : `<span class="result-stat-range" style="grid-column:1/-1">Destination running total realigned by <strong>${liftStr}</strong> so the imported history and existing data form one continuous energy series. The oldest hour is correct and no manual fix is needed.</span>`;
          } else if (r.stats_sum_offset !== null && r.stats_sum_offset !== undefined) {
            const offsetStr = this._formatOffset(r.stats_sum_offset, r.stats_unit);
            grid += `<span class="result-stat-range" style="grid-column:1/-1">Cumulative-sum offset ${r.dry_run ? "would be applied" : "applied"}: <strong>${offsetStr}</strong> (aligns energy totals at splice point)</span>`;
            grid += `<span class="result-stat-range" style="grid-column:1/-1">The oldest imported hour absorbs this offset, so it can show a one-off value in the Energy dashboard's all-time total. Your hourly/daily usage graph is unaffected; correct that single hour under Developer Tools → Statistics if you want a perfect lifetime total.</span>`;
          }
          if (r.stats_realign_error)
            grid += `<span class="result-stat-error">Realignment failed: ${r.stats_realign_error}</span>`;
          if (r.stats_error)
            grid += `<span class="result-stat-error">Error: ${r.stats_error}</span>`;
        }

        // --- Short-term statistics summary (only shown when backfill ran) ---
        const hasShortInfo =
          (r.stats_short_source_total || 0) > 0 ||
          (r.stats_short_imported || 0) > 0 ||
          r.stats_short_error;
        if (hasShortInfo) {
          grid += `<span class="result-stat-label" style="margin-top:6px">Short-term statistics (5-min) ${dlBtn("stats_short", (r.debug_stats_short || []).length)}</span><span class="result-stat-label"></span>`;
          if (r.stats_short_source_total > 0)
            grid += `<span class="result-stat-value">${r.stats_short_source_total.toLocaleString()}</span><span class="result-stat-label">total in source</span>`;
          if (r.stats_short_already_covered > 0)
            grid += `<span class="result-stat-value">${r.stats_short_already_covered.toLocaleString()}</span><span class="result-stat-label">already complete in destination</span>`;
          if (r.stats_short_skipped_recent > 0)
            grid += `<span class="result-stat-value">${r.stats_short_skipped_recent.toLocaleString()}</span><span class="result-stat-label">skipped (too recent or under threshold)</span>`;
          grid += `<span class="result-stat-value">${(r.stats_short_imported || 0).toLocaleString()}</span><span class="result-stat-label">${actionVerb}</span>`;
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
