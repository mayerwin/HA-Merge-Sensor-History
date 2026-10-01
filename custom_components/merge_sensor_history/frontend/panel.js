/**
 * Merge Sensor History - Custom Panel
 *
 * Provides a UI to select source/destination entity pairs
 * and import historical data between them.
 */

/**
 * What the panel was showing, kept for as long as the page stays open. Home
 * Assistant throws a custom panel away when its browser tab has been hidden
 * for 5 minutes, or when another page is opened, and builds a new one on the
 * way back. The new panel picks up from here, including a run that was still
 * going. Memory only: reloading the page starts afresh.
 */
const panelMemory = {
  form: null, // the form as it was when the last panel was removed
  run: null, // the import or preview still running, if any
  outcome: null, // what the last finished run returned
};

class MergeSensorsHistoryPanel extends HTMLElement {
  constructor() {
    super();
    this._hass = null;
    this._pairs = [{ source: "", destination: "" }];
    this._importing = false;
    this._lastResults = null;
    this._debugByPair = new Map();
    // No-live-state entities: recorder statistics whose entity is gone from
    // the state machine — truly deleted (statistics only), disabled (recent
    // raw states may still be in the recorder), or registered but not loaded.
    // Fetched lazily via recorder/list_statistic_ids when the
    // "Show deleted/disabled entities" toggle is first enabled.
    this._showDeleted = false;
    this._deletedIds = []; // sorted list of no-live-state statistic_ids
    this._deletedNames = new Map(); // id -> stored statistics name (may be "")
    this._deletedKinds = new Map(); // id -> "deleted" | "disabled" | "not loaded"
    this._deletedFetched = false;
    // "Pick by device": two devices, and which destination entity each of the
    // source device's entities goes to. The registry is read fresh each time
    // the mode is switched on.
    this._deviceMode = false;
    this._devData = null; // { devices: Map(id -> device), entities: Map(device id -> [info]) }
    this._devLoadError = "";
    this._devSource = "";
    this._devDest = "";
    this._devChoices = {}; // source entity id -> destination entity id, "" to skip
    this._devChoicesKey = ""; // the device pair _devChoices was made for
    this._devSuggest = new Map(); // source entity id -> { dest, why }
    // The filter fields serve both modes; each mode keeps its own text.
    this._otherFilters = { single: "", source: "", dest: "" };
  }

  set hass(hass) {
    this._hass = hass;
    if (!this.shadowRoot) {
      this._render();
      this._restore();
    }
  }

  disconnectedCallback() {
    // Home Assistant removes the panel when its tab is hidden or another page
    // is opened. Keep the form so the panel it builds next can show it again.
    if (this.shadowRoot) panelMemory.form = this._snapshotForm();
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

  /** True if the id is a known no-live-state entity (deleted or disabled). */
  _isDeleted(entityId) {
    return this._deletedNames.has(entityId);
  }

  /** Escape text for safe interpolation into innerHTML. Friendly names are
   *  arbitrary text; without this a name containing < > & renders wrong. */
  _esc(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
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
        /* A preview writes nothing, so it must not look like a finished import. */
        .result-preview {
          background: color-mix(in srgb, var(--info-color, #039be5) 7%, transparent);
          border: 1px dashed color-mix(in srgb, var(--info-color, #039be5) 65%, transparent);
          color: var(--primary-text-color);
        }
        .result-preview .result-icon { color: var(--info-color, #039be5); }
        .result-partial {
          background: color-mix(in srgb, var(--warning-color, #ff9800) 10%, transparent);
          border: 1px solid color-mix(in srgb, var(--warning-color, #ff9800) 35%, transparent);
          color: var(--primary-text-color);
        }
        .result-header {
          display: flex;
          flex-wrap: wrap;
          align-items: center;
          gap: 4px 8px;
          font-weight: 500;
          margin-bottom: 4px;
        }
        .result-pair {
          flex: 1 1 240px;
          min-width: 0;
          overflow-wrap: anywhere;
        }
        .result-badge {
          flex-shrink: 0;
          padding: 1px 8px;
          border-radius: 10px;
          font-size: 11px;
          font-weight: 600;
          letter-spacing: 0.5px;
          text-transform: uppercase;
          color: #fff;
          white-space: nowrap;
        }
        .badge-preview { background: var(--info-color, #039be5); }
        .badge-imported { background: var(--success-color, #4caf50); }
        .badge-partial { background: var(--warning-color, #ff9800); }
        .badge-nothing { background: var(--secondary-text-color, #727272); }
        .badge-failed { background: var(--error-color, #db4437); }
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
        .repair-notice {
          grid-column: 1/-1;
          margin-top: 6px;
          padding: 10px 12px;
          border-radius: 6px;
          border: 1px solid var(--warning-color, #ff9800);
          background: rgba(255, 152, 0, 0.08);
          font-size: 13px;
          line-height: 1.45;
        }
        .repair-btn {
          margin-top: 8px;
          padding: 6px 12px;
          border-radius: 4px;
          border: 1px solid var(--warning-color, #ff9800);
          background: var(--warning-color, #ff9800);
          color: #fff;
          font-size: 13px;
          cursor: pointer;
        }
        .repair-btn:disabled {
          opacity: 0.6;
          cursor: default;
        }
        .suggest-btn {
          margin-top: 6px;
          padding: 4px 10px;
          border-radius: 4px;
          border: 1px solid var(--primary-color, #03a9f4);
          background: var(--primary-color, #03a9f4);
          color: #fff;
          font-size: 13px;
          font-style: normal;
          cursor: pointer;
        }
        .suggest-btn:disabled {
          opacity: 0.6;
          cursor: default;
        }
        .suggest-outcome {
          color: var(--primary-text-color);
          font-style: normal;
        }
        .repair-outcome {
          margin-top: 8px;
          font-size: 13px;
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
        .option-row.danger input[type="checkbox"] {
          accent-color: var(--error-color, #db4437);
        }
        .option-row.danger .option-label {
          color: var(--error-color, #db4437);
          font-weight: 600;
        }
        .danger-note {
          margin-top: 8px;
          padding: 10px 12px;
          border: 1px solid var(--error-color, #db4437);
          border-left-width: 4px;
          border-radius: 6px;
          background: rgba(219, 68, 55, 0.08);
          color: var(--primary-text-color);
          font-size: 12px;
          line-height: 1.5;
        }
        .danger-note.hidden {
          display: none;
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

        .device-note {
          font-size: 13px;
          color: var(--secondary-text-color);
          margin-bottom: 14px;
          line-height: 1.5;
        }
        .device-note.err {
          color: var(--error-color, #db4437);
        }
        .map-table {
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 10px;
          margin-bottom: 14px;
          overflow: hidden;
        }
        .map-head,
        .map-row {
          display: grid;
          grid-template-columns: minmax(0, 1fr) 24px minmax(0, 1fr);
          gap: 4px 12px;
          align-items: center;
          padding: 9px 14px;
        }
        .map-head {
          background: var(--secondary-background-color, #f5f5f5);
          font-size: 11px;
          font-weight: 600;
          letter-spacing: 0.8px;
          text-transform: uppercase;
          color: var(--secondary-text-color);
        }
        .map-row {
          border-top: 1px solid var(--divider-color, #e0e0e0);
          font-size: 13px;
        }
        .map-row.skipped .map-src {
          opacity: 0.55;
        }
        .map-name {
          font-weight: 500;
          overflow-wrap: anywhere;
        }
        .map-id,
        .map-why {
          font-size: 12px;
          color: var(--secondary-text-color);
          overflow-wrap: anywhere;
        }
        .map-arrow {
          color: var(--primary-color, #03a9f4);
          text-align: center;
        }
        .map-row select {
          width: 100%;
          padding: 7px 10px;
          border: 1px solid var(--divider-color, #e0e0e0);
          border-radius: 6px;
          font-size: 13px;
          background: var(--ha-card-background, var(--card-background-color, white));
          color: var(--primary-text-color);
          cursor: pointer;
        }
        .map-row select:focus {
          outline: none;
          border-color: var(--primary-color, #03a9f4);
          box-shadow: 0 0 0 1px var(--primary-color, #03a9f4);
        }
        .map-unused {
          border-top: 1px solid var(--divider-color, #e0e0e0);
          padding: 9px 14px;
          font-size: 12px;
          color: var(--secondary-text-color);
          overflow-wrap: anywhere;
        }
        .device-actions {
          display: flex;
          align-items: center;
          gap: 12px;
          flex-wrap: wrap;
        }

        @media (max-width: 600px) {
          .map-head {
            display: none;
          }
          .map-row {
            grid-template-columns: minmax(0, 1fr);
          }
          .map-arrow {
            text-align: left;
          }
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
          Only data older than the destination's oldest good record will be imported &mdash; no duplicates.<br/>
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
        <label class="deleted-toggle" id="deleted-toggle" title="Lists entities that have no live state but may still have data in the recorder, so they can be selected as a source. Tagged [deleted]: gone from Home Assistant; only long-term (hourly) statistics remain (raw states are purged per your recorder retention, about 10 days by default). Tagged [disabled] or [not loaded]: still registered; recent raw states and statistics are merged while they remain within the recorder retention window.">
          <input type="checkbox" id="show-deleted-cb" />
          <span>Show deleted/disabled entities <span class="deleted-note">(no live state)</span></span>
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
        <label class="deleted-toggle" id="device-toggle" title="Pick an old and a new device instead of single entities. Each entity of the old device is matched to one of the new device's, which you can change or skip, and the result is added to the list of pairs.">
          <input type="checkbox" id="device-mode-cb" />
          <span>Pick by device <span class="deleted-note">(match all of an old device's entities to a new one's)</span></span>
          <span class="deleted-status" id="device-status"></span>
        </label>
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
        <div id="device-container" style="display:none"></div>
        <div id="pairs-container"></div>
        <div class="pair-actions" id="pair-actions">
          <button class="btn btn-secondary" id="add-pair-btn">+ Add Pair</button>
        </div>
        <div class="options-section">
          <div class="options-title">Options</div>
          <label class="option-row" title="By default, only data older than the destination's oldest good entry is imported, to avoid duplicates (hidden unavailable/unknown rows do not count). Enable this to also fill quiet periods inside the destination's existing time range.">
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
          <label class="option-row danger" style="margin-top:14px" title="Replaces the destination's data with the source's wherever the source has data, instead of only filling holes. Destination state rows inside the source's time span are deleted. This cannot be undone without a database backup.">
            <input type="checkbox" id="overwrite-cb" />
            <span class="option-label">&#9888;&#65039; Overwrite existing destination data (destructive)</span>
          </label>
          <div class="danger-note hidden" id="overwrite-note">
            <strong>This deletes data.</strong> Every destination state row inside the source's time span is removed and replaced by the source's states, and existing statistics values are overwritten wherever the source has data. Values the source does not provide are kept, and the most recent slots Home Assistant may still be compiling are still skipped.<br />
            Use it only when the destination holds data you know is wrong, for example zeros logged while a sensor was being commissioned, or an earlier import made with the wrong unit. <strong>Back up your database first</strong> and run <strong>Preview</strong> to see the row counts. Deleted rows cannot be recovered.
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
    this._overwriteCb = shadow.getElementById("overwrite-cb");
    this._overwriteNote = shadow.getElementById("overwrite-note");
    this._deviceModeCb = shadow.getElementById("device-mode-cb");
    this._deviceStatus = shadow.getElementById("device-status");
    this._deviceContainer = shadow.getElementById("device-container");
    this._pairActions = shadow.getElementById("pair-actions");

    this._showDeletedCb.addEventListener("change", () =>
      this._onShowDeletedChange()
    );

    this._deviceModeCb.addEventListener("change", () =>
      this._setDeviceMode(this._deviceModeCb.checked)
    );
    this._deviceContainer.addEventListener("change", (ev) =>
      this._onDeviceChange(ev.target)
    );
    this._deviceContainer.addEventListener("click", (ev) => {
      if (ev.target.closest("#device-add-btn")) this._addDevicePairs();
    });

    // The full warning only unfolds once the box is ticked, so the panel stays
    // calm for the majority who never need this.
    this._overwriteCb.addEventListener("change", () => {
      this._overwriteNote.classList.toggle(
        "hidden",
        !this._overwriteCb.checked
      );
    });

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
        this._renderLists();
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
      this._renderLists();
    });

    shadow.getElementById("bulk-toggle").addEventListener("click", () => {
      const open = this._bulkBody.classList.toggle("open");
      this._bulkChevron.classList.toggle("open", open);
    });

    shadow.getElementById("bulk-add-btn").addEventListener("click", () => {
      this._handleBulkAdd();
    });

    this._resultsContainer.addEventListener("click", (ev) => {
      const repairBtn = ev.target.closest(".repair-btn");
      if (repairBtn) {
        this._repairSumSeries(repairBtn);
        return;
      }
      const suggestBtn = ev.target.closest(".suggest-btn");
      if (suggestBtn) {
        this._applyUnitSuggestion(suggestBtn);
        return;
      }
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
      // Entity registry lookup distinguishes a DISABLED entity (still
      // registered; its recent raw states may survive in the recorder) from
      // a truly DELETED one (only statistics remain). Best-effort: on failure
      // everything falls back to the generic "deleted" tag.
      let registry = new Map();
      try {
        const regRows = await this._hass.callWS({
          type: "config/entity_registry/list",
        });
        registry = new Map((regRows || []).map((e) => [e.entity_id, e]));
      } catch (regErr) {
        // ignore — tags degrade to "deleted"
      }
      const live = this._hass.states;
      const isLive = (id) => Object.prototype.hasOwnProperty.call(live, id);
      this._deletedIds = [];
      this._deletedNames = new Map();
      this._deletedKinds = new Map();
      for (const r of rows || []) {
        const id = r.statistic_id;
        // Only recorder-sourced sensor stats can be merged as an entity (an
        // entity-form id we can write to); external stats (colon ids) can't.
        // Listed = has stats but no live entity (deleted or disabled).
        if (r.source === "recorder" && id && !isLive(id)) {
          this._deletedIds.push(id);
          this._deletedNames.set(id, r.name || "");
          const reg = registry.get(id);
          this._deletedKinds.set(
            id,
            reg ? (reg.disabled_by ? "disabled" : "not loaded") : "deleted"
          );
        }
      }
      // Registered entities with no live state (disabled, or not loaded) are
      // selectable even without statistics: their raw states may still be in
      // the recorder and can be merged.
      for (const [id, reg] of registry) {
        if (!isLive(id) && !this._deletedNames.has(id)) {
          this._deletedIds.push(id);
          this._deletedNames.set(id, reg.name || reg.original_name || "");
          this._deletedKinds.set(
            id,
            reg.disabled_by ? "disabled" : "not loaded"
          );
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
      n === 0 ? "none found" : `${n} found (tagged in the dropdowns below)`;
  }

  /** Build a dropdown option label, tagging no-live-state ids. A deleted
   *  entity keeps only its statistics; a disabled one may still have recent
   *  raw states in the recorder, so its tag must not say "statistics only". */
  _optionLabel(e) {
    const name = this._esc(this._friendlyName(e));
    if (this._isDeleted(e)) {
      const kind = this._deletedKinds.get(e) || "deleted";
      const tag =
        kind === "deleted" ? "[deleted, statistics only]" : `[${kind}]`;
      return name ? `${e} (${name}) ${tag}` : `${e} ${tag}`;
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

  /** Re-render whichever list the current mode shows. */
  _renderLists() {
    if (this._deviceMode) this._renderDevices();
    else this._renderPairs();
  }

  // --- Keeping the form when Home Assistant rebuilds the panel ---

  _readFilters() {
    return {
      single: this._filterInput.value,
      source: this._sourceFilterInput.value,
      dest: this._destFilterInput.value,
    };
  }

  _writeFilters(f) {
    this._filterInput.value = f.single || "";
    this._sourceFilterInput.value = f.source || "";
    this._destFilterInput.value = f.dest || "";
  }

  /** Everything the user set up, as plain data. */
  _snapshotForm() {
    const current = this._readFilters();
    return {
      pairs: this._pairs.map((p) => ({ ...p })),
      sharedFilter: this._sharedFilterCb.checked,
      entityFilters: this._deviceMode ? { ...this._otherFilters } : current,
      deviceFilters: this._deviceMode ? current : { ...this._otherFilters },
      bulkText: this._bulkTextarea.value,
      fillGaps: this._fillGapsCb.checked,
      gapThreshold: this._gapThreshold.value,
      scale: this._scaleCb.checked,
      customMode: this._adjustModeCustom.checked,
      scaleFactor: this._scaleFactor.value,
      customFn: this._customFn.value,
      overwrite: this._overwriteCb.checked,
      showDeleted: this._showDeletedCb.checked,
      deletedData: this._deletedFetched
        ? {
            ids: [...this._deletedIds],
            names: [...this._deletedNames],
            kinds: [...this._deletedKinds],
          }
        : null,
      device: {
        on: this._deviceMode,
        source: this._devSource,
        dest: this._devDest,
        choices: { ...this._devChoices },
        choicesKey: this._devChoicesKey,
      },
    };
  }

  /** Show what the previous panel was showing, and follow a run that is
   *  still going. */
  _restore() {
    const f = panelMemory.form;
    if (f) {
      this._pairs = f.pairs.length
        ? f.pairs.map((p) => ({ ...p }))
        : [{ source: "", destination: "" }];
      this._sharedFilterCb.checked = f.sharedFilter;
      this._singleFilterRow.style.display = f.sharedFilter ? "" : "none";
      this._sourceFilterRow.style.display = f.sharedFilter ? "none" : "";
      this._destFilterRow.style.display = f.sharedFilter ? "none" : "";
      this._writeFilters(f.entityFilters);
      this._otherFilters = { ...f.deviceFilters };
      this._bulkTextarea.value = f.bulkText;
      this._fillGapsCb.checked = f.fillGaps;
      this._gapThreshold.value = f.gapThreshold;
      this._scaleCb.checked = f.scale;
      this._adjustModeCustom.checked = f.customMode;
      this._adjustModeMultiply.checked = !f.customMode;
      this._scaleFactor.value = f.scaleFactor;
      this._customFn.value = f.customFn;
      this._overwriteCb.checked = f.overwrite;
      // Runs the same enable/disable sync as a manual change.
      for (const cb of [this._fillGapsCb, this._scaleCb, this._overwriteCb]) {
        cb.dispatchEvent(new Event("change"));
      }
      if (f.deletedData) {
        this._deletedIds = [...f.deletedData.ids];
        this._deletedNames = new Map(f.deletedData.names);
        this._deletedKinds = new Map(f.deletedData.kinds);
        this._deletedFetched = true;
      }
      this._renderPairs();
      if (f.showDeleted) {
        this._showDeletedCb.checked = true;
        this._onShowDeletedChange();
      }
      this._devSource = f.device.source;
      this._devDest = f.device.dest;
      this._devChoices = { ...f.device.choices };
      this._devChoicesKey = f.device.choicesKey;
      if (f.device.on) this._setDeviceMode(true);
    }
    if (panelMemory.run) this._followRun(panelMemory.run);
    else if (panelMemory.outcome) this._showOutcome(panelMemory.outcome);
  }

  // --- Pick by device ---

  _setDeviceMode(on) {
    this._deviceModeCb.checked = on;
    if (on === this._deviceMode) return;
    this._deviceMode = on;
    // Each mode keeps its own filter text: a device name typed here would
    // otherwise hide most entities on the way back.
    const current = this._readFilters();
    this._writeFilters(this._otherFilters);
    this._otherFilters = current;
    const what = on ? "devices" : "entities";
    this._filterInput.placeholder = on
      ? "Filter devices by name, model or entity ID..."
      : "Filter entities by name or ID...";
    this._sourceFilterInput.placeholder = `Filter source ${what}...`;
    this._destFilterInput.placeholder = `Filter destination ${what}...`;
    this._deviceContainer.style.display = on ? "" : "none";
    this._pairsContainer.style.display = on ? "none" : "";
    this._pairActions.style.display = on ? "none" : "";
    this._syncActionButtons();
    if (on) {
      this._loadDevices();
    } else {
      this._deviceStatus.textContent = "";
      this._deviceStatus.classList.remove("err");
      this._renderPairs();
    }
  }

  /** Read the device and entity registries afresh, plus the statistics
   *  metadata, which gives the unit of entities that have no live state. */
  async _loadDevices() {
    const token = (this._devLoadToken = (this._devLoadToken || 0) + 1);
    this._devData = null;
    this._devLoadError = "";
    this._devSuggestKey = "";
    this._deviceStatus.classList.remove("err");
    this._deviceStatus.textContent = "loading…";
    this._renderDevices();
    try {
      const [devices, entities, stats] = await Promise.all([
        this._hass.callWS({ type: "config/device_registry/list" }),
        this._hass.callWS({ type: "config/entity_registry/list" }),
        this._hass
          .callWS({ type: "recorder/list_statistic_ids" })
          .catch(() => []),
      ]);
      if (token !== this._devLoadToken || !this._deviceMode) return;
      this._devData = this._buildDeviceData(devices || [], entities || [], stats || []);
      this._deviceStatus.textContent = this._devData.devices.size
        ? ""
        : "no devices with entities found";
    } catch (err) {
      if (token !== this._devLoadToken || !this._deviceMode) return;
      this._devLoadError = String((err && err.message) || err);
      this._deviceStatus.classList.add("err");
      this._deviceStatus.textContent = "could not load";
    }
    this._renderDevices();
  }

  _buildDeviceData(devices, entities, stats) {
    const meta = new Map();
    for (const s of stats) if (s && s.statistic_id) meta.set(s.statistic_id, s);
    const byDevice = new Map();
    for (const e of entities) {
      if (!e || !e.device_id || !e.entity_id) continue;
      if (!byDevice.has(e.device_id)) byDevice.set(e.device_id, []);
      byDevice.get(e.device_id).push(e);
    }
    const devMap = new Map();
    const entMap = new Map();
    for (const dev of devices) {
      const regs = dev && byDevice.get(dev.id);
      if (!regs) continue;
      devMap.set(dev.id, dev);
      entMap.set(
        dev.id,
        regs
          .map((e) => this._entityInfo(e, dev, meta.get(e.entity_id)))
          .sort(
            (a, b) =>
              (a.category ? 1 : 0) - (b.category ? 1 : 0) ||
              a.label.localeCompare(b.label) ||
              a.id.localeCompare(b.id)
          )
      );
    }
    return { devices: devMap, entities: entMap };
  }

  /** What the matching needs to know about one entity of a device. */
  _entityInfo(reg, dev, meta) {
    const id = reg.entity_id;
    const st = this._hass.states[id];
    const attrs = (st && st.attributes) || {};
    // Names without the device's own name, which differs between the two.
    const devNames = [dev.name_by_user, dev.name]
      .map((n) => this._normName(n))
      .filter(Boolean);
    const strip = (n) => {
      for (const p of devNames) {
        if (n === p) return "";
        if (n.startsWith(p + " ")) return n.slice(p.length + 1);
      }
      return n;
    };
    const stripped = [attrs.friendly_name, reg.name, reg.original_name, id.split(".")[1]]
      .filter((n) => typeof n === "string" && n)
      .map((n) => strip(this._normName(n)));
    let label = attrs.friendly_name || reg.name || reg.original_name || "";
    for (const p of [dev.name_by_user, dev.name]) {
      if (p && label.toLowerCase().startsWith(p.toLowerCase() + " ")) {
        label = label.slice(p.length + 1);
        break;
      }
    }
    let hasSum = null;
    if (meta && typeof meta.has_sum === "boolean") hasSum = meta.has_sum;
    else if (attrs.state_class)
      hasSum = attrs.state_class === "total" || attrs.state_class === "total_increasing";
    let unit = null; // null: not known
    if (st) unit = attrs.unit_of_measurement || "";
    else if (meta) unit = meta.statistics_unit_of_measurement || "";
    return {
      id,
      domain: id.split(".")[0],
      label: label || id,
      disabled: !!reg.disabled_by,
      category: reg.entity_category || null,
      tk: reg.translation_key || null,
      names: [...new Set(stripped.filter(Boolean))],
      main: stripped.includes(""), // named after the device only
      deviceClass: attrs.device_class || null,
      unit,
      unitClass: (meta && meta.unit_class) || null,
      hasSum,
    };
  }

  _normName(s) {
    return String(s || "")
      .normalize("NFKD")
      .replace(/[̀-ͯ]/g, "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  }

  /** Whether two entities could hold the same kind of data. A pair that
   *  fails this is never suggested, however alike the names. */
  _kindsCompatible(a, b) {
    if (a.domain !== b.domain) return false;
    if (a.deviceClass && b.deviceClass && a.deviceClass !== b.deviceClass) return false;
    if (a.hasSum !== null && b.hasSum !== null && a.hasSum !== b.hasSum) return false;
    if (a.unitClass && b.unitClass) return a.unitClass === b.unitClass;
    if (a.unit !== null && b.unit !== null) return a.unit === b.unit;
    return true;
  }

  _editDistance(a, b) {
    let prev = Array.from({ length: b.length + 1 }, (_, j) => j);
    for (let i = 1; i <= a.length; i++) {
      const cur = [i];
      for (let j = 1; j <= b.length; j++) {
        cur[j] = Math.min(
          prev[j] + 1,
          cur[j - 1] + 1,
          prev[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1)
        );
      }
      prev = cur;
    }
    return prev[b.length];
  }

  /** How alike two normalized names are, from 0 to 1. */
  _nameSim(a, b) {
    if (a === b) return 1;
    const ta = new Set(a.split(" "));
    const tb = new Set(b.split(" "));
    let shared = 0;
    for (const t of ta) if (tb.has(t)) shared++;
    const jaccard = shared / (ta.size + tb.size - shared);
    // "Power" in "Active power" is a hint, not a match: "Temperature" is also
    // in "Device temperature".
    const contained = shared > 0 && shared === Math.min(ta.size, tb.size) ? 0.6 : 0;
    const edit = 1 - this._editDistance(a, b) / Math.max(a.length, b.length);
    return Math.max(jaccard, contained, edit);
  }

  /** The best likeness between two entities' names, and whether their names
   *  only ever disagree on a number: "Voltage L1" and "Voltage L2", or two
   *  channels, are different things however alike the rest is. */
  _namesSim(s, d) {
    const nums = (n) =>
      n
        .split(" ")
        .filter((x) => /\d/.test(x))
        .sort()
        .join(" ");
    let sim = s.main && d.main ? 0.9 : 0;
    let numbersDiffer = s.names.length > 0 && d.names.length > 0;
    for (const a of s.names) {
      for (const b of d.names) {
        const na = nums(a);
        const nb = nums(b);
        if (na && nb && na !== nb) continue;
        numbersDiffer = false;
        sim = Math.max(sim, this._nameSim(a, b));
      }
    }
    return { sim, numbersDiffer };
  }

  /** Suggest which of the new device's entities each old one goes to. Only
   *  pairs of the same kind are considered, scored on the integration's own
   *  type key, the name without the device's, and being the only one of a
   *  kind on both devices. A pair is suggested only when it clears the bar
   *  and no equally good rival exists: an unclear case is left to the user
   *  rather than guessed. Returns Map(source id -> { dest, why }). */
  _suggestMatches(src, dst) {
    const kind = (e) =>
      e.deviceClass
        ? [e.domain, e.deviceClass, e.unitClass || e.unit || "", e.hasSum].join("|")
        : null;
    const countKinds = (list) => {
      const m = new Map();
      for (const e of list) {
        const k = kind(e);
        if (k) m.set(k, (m.get(k) || 0) + 1);
      }
      return m;
    };
    const srcKinds = countKinds(src);
    const dstKinds = countKinds(dst);
    const cands = [];
    for (const s of src) {
      for (const d of dst) {
        if (!this._kindsCompatible(s, d)) continue;
        const { sim, numbersDiffer } = this._namesSim(s, d);
        if (numbersDiffer) continue;
        const sameType = !!s.tk && s.tk === d.tk;
        const k = kind(s);
        const onlyOne =
          !!k && k === kind(d) && srcKinds.get(k) === 1 && dstKinds.get(k) === 1;
        let score = 60 * sim + (sameType ? 100 : 0) + (onlyOne ? 40 : 0);
        if (s.deviceClass && s.deviceClass === d.deviceClass) score += 5;
        if (s.unit && s.unit === d.unit) score += 3;
        if (score < 45) continue;
        let why = "similar name";
        if (sameType) why = "same type of entity";
        else if (sim >= 1) why = "same name";
        else if (s.main && d.main && sim === 0.9) why = "the device's main entity on both";
        else if (sim < 0.75 && onlyOne) why = "the only one of its kind on both devices";
        cands.push({ s: s.id, d: d.id, score, why });
      }
    }
    cands.sort(
      (a, b) => b.score - a.score || a.s.localeCompare(b.s) || a.d.localeCompare(b.d)
    );
    const takenS = new Set(); // matched, or left to the user
    const takenD = new Set();
    const out = new Map();
    for (let i = 0; i < cands.length; ) {
      let j = i;
      while (j < cands.length && cands[i].score - cands[j].score < 1e-6) j++;
      const group = cands
        .slice(i, j)
        .filter((c) => !takenS.has(c.s) && !takenD.has(c.d));
      const perS = new Map();
      const perD = new Map();
      for (const c of group) {
        perS.set(c.s, (perS.get(c.s) || 0) + 1);
        perD.set(c.d, (perD.get(c.d) || 0) + 1);
      }
      for (const c of group) {
        if (takenS.has(c.s) || takenD.has(c.d)) continue;
        const tieS = perS.get(c.s) > 1;
        const tieD = perD.get(c.d) > 1;
        if (tieS || tieD) {
          if (tieS) takenS.add(c.s);
          if (tieD) takenD.add(c.d);
          continue;
        }
        takenS.add(c.s);
        takenD.add(c.d);
        out.set(c.s, { dest: c.d, why: c.why });
      }
      i = j;
    }
    return out;
  }

  _deviceName(dev) {
    return dev.name_by_user || dev.name || dev.id;
  }

  _deviceLabel(dev) {
    const model = [dev.manufacturer, dev.model].filter(Boolean).join(" ");
    let label = this._deviceName(dev) + (model ? ` (${model})` : "");
    if (dev.disabled_by) label += " [disabled]";
    return this._esc(label);
  }

  _getFilteredDevices(role) {
    const shared = this._sharedFilterCb.checked;
    const input = shared
      ? this._filterInput
      : role === "destination"
        ? this._destFilterInput
        : this._sourceFilterInput;
    const filter = (input.value || "").trim().toLowerCase();
    const matches = (dev) =>
      [dev.name_by_user, dev.name, dev.manufacturer, dev.model, dev.id].some(
        (s) => s && String(s).toLowerCase().includes(filter)
      ) ||
      (this._devData.entities.get(dev.id) || []).some(
        (e) => e.id.includes(filter) || e.label.toLowerCase().includes(filter)
      );
    return [...this._devData.devices.values()]
      .filter((dev) => !filter || matches(dev))
      .sort((a, b) => this._deviceName(a).localeCompare(this._deviceName(b)));
  }

  _buildDeviceOptions(devs, selected) {
    let opts = '<option value="">-- Select device --</option>';
    const sel = selected && this._devData.devices.get(selected);
    if (sel && !devs.includes(sel)) {
      opts += `<option value="${this._esc(sel.id)}" selected>${this._deviceLabel(sel)} [filtered]</option>`;
    }
    for (const dev of devs) {
      opts += `<option value="${this._esc(dev.id)}"${dev.id === selected ? " selected" : ""}>${this._deviceLabel(dev)}</option>`;
    }
    return opts;
  }

  _renderDevices() {
    const c = this._deviceContainer;
    if (!this._devData) {
      c.innerHTML = this._devLoadError
        ? `<div class="device-note err">The device list could not be loaded: ${this._esc(this._devLoadError)}</div>`
        : `<div class="device-note">Loading devices…</div>`;
      return;
    }
    // A device removed since leaves nothing to pick.
    if (!this._devData.devices.has(this._devSource)) this._devSource = "";
    if (!this._devData.devices.has(this._devDest)) this._devDest = "";
    const col = (role, label, selected) => {
      const n = selected ? (this._devData.entities.get(selected) || []).length : 0;
      return `<div class="entity-col">
          <label>${label}</label>
          <select data-device-role="${role}">${this._buildDeviceOptions(this._getFilteredDevices(role), selected)}</select>
          <div class="entity-info">${n ? `${n} entit${n === 1 ? "y" : "ies"}` : ""}</div>
        </div>`;
    };
    c.innerHTML = `<div class="pair-row">
        ${col("source", "Source device (old)", this._devSource)}
        <div class="arrow-col">&#8594;</div>
        ${col("destination", "Destination device (new)", this._devDest)}
      </div>
      ${this._mappingHtml()}`;
    this._updateMappingSummary();
  }

  _mappingHtml() {
    if (!this._devSource || !this._devDest) {
      return `<div class="device-note">Pick the old device and the new one. Each of the old device's entities is then matched to one of the new device's, for you to check before adding them to the list of pairs.</div>`;
    }
    if (this._devSource === this._devDest) {
      return `<div class="device-note err">Pick two different devices.</div>`;
    }
    const src = this._devData.entities.get(this._devSource) || [];
    const dst = this._devData.entities.get(this._devDest) || [];
    const key = `${this._devSource}\n${this._devDest}`;
    if (this._devSuggestKey !== key) {
      this._devSuggest = this._suggestMatches(src, dst);
      this._devSuggestKey = key;
    }
    // A new pair of devices starts from the suggestions; the same pair keeps
    // the user's own choices.
    if (this._devChoicesKey !== key) {
      this._devChoices = {};
      this._devChoicesKey = key;
    }
    const dstIds = new Set(dst.map((d) => d.id));
    for (const s of src) {
      const choice = this._devChoices[s.id];
      if (choice === undefined) {
        const sug = this._devSuggest.get(s.id);
        this._devChoices[s.id] = sug ? sug.dest : "";
      } else if (choice && !dstIds.has(choice)) {
        this._devChoices[s.id] = "";
      }
    }
    const rows = src.map((s) => this._mapRowHtml(s, dst)).join("");
    return `<div class="map-table">
        <div class="map-head"><span>Old device's entity</span><span></span><span>Gets its history into</span></div>
        ${rows}
        <div class="map-unused" id="map-unused"></div>
      </div>
      <div class="device-actions">
        <button class="btn btn-primary" id="device-add-btn">Add pairs</button>
        <span class="device-note" style="margin:0">Adds them to the list of pairs, where Preview and Import work as usual.</span>
      </div>`;
  }

  _mapRowHtml(s, dst) {
    const choice = this._devChoices[s.id] || "";
    let opts = '<option value="">Skip (not imported)</option>';
    for (const d of dst) {
      const label = `${d.label} (${d.id})${d.disabled ? " [disabled]" : ""}`;
      opts += `<option value="${this._esc(d.id)}"${d.id === choice ? " selected" : ""}>${this._esc(label)}</option>`;
    }
    const idLine = s.category ? `${s.id} · ${s.category}` : s.id;
    return `<div class="map-row${choice ? "" : " skipped"}">
        <div class="map-src">
          <div class="map-name">${this._esc(s.label)}${s.disabled ? " [disabled]" : ""}</div>
          <div class="map-id">${this._esc(idLine)}</div>
        </div>
        <div class="map-arrow">&#8594;</div>
        <div>
          <select data-map-src="${this._esc(s.id)}">${opts}</select>
          <div class="map-why">${this._esc(this._whyText(s.id, choice))}</div>
        </div>
      </div>`;
  }

  _whyText(srcId, choice) {
    const sug = this._devSuggest.get(srcId);
    if (sug && sug.dest === choice) return `Suggested: ${sug.why}`;
    if (!sug && !choice) return "No clear match: pick one, or leave it skipped";
    return "";
  }

  _chosenPairs() {
    const src = this._devData.entities.get(this._devSource) || [];
    return src
      .filter((s) => this._devChoices[s.id])
      .map((s) => ({ source: s.id, destination: this._devChoices[s.id] }));
  }

  _updateMappingSummary() {
    const btn = this._deviceContainer.querySelector("#device-add-btn");
    if (!btn) return;
    const n = this._chosenPairs().length;
    btn.textContent = n === 1 ? "Add 1 pair" : `Add ${n} pairs`;
    btn.disabled = n === 0;
    const dst = this._devData.entities.get(this._devDest) || [];
    const used = new Set(Object.values(this._devChoices).filter(Boolean));
    const unused = dst.filter((d) => !used.has(d.id));
    this._deviceContainer.querySelector("#map-unused").textContent = unused.length
      ? `New device's entities left without history: ${unused.map((d) => d.label).join(", ")}`
      : "Every entity of the new device gets history.";
  }

  _onDeviceChange(el) {
    if (el.dataset.deviceRole) {
      if (el.dataset.deviceRole === "source") this._devSource = el.value;
      else this._devDest = el.value;
      this._renderDevices();
      return;
    }
    const srcId = el.dataset.mapSrc;
    if (!srcId) return;
    this._devChoices[srcId] = el.value;
    const row = el.closest(".map-row");
    row.classList.toggle("skipped", !el.value);
    row.querySelector(".map-why").textContent = this._whyText(srcId, el.value);
    this._updateMappingSummary();
  }

  /** Turn the device mapping into ordinary pairs and go back to the list. */
  _addDevicePairs() {
    const add = this._chosenPairs();
    if (!add.length) return;
    if (this._pairs.length === 1 && !this._pairs[0].source && !this._pairs[0].destination) {
      this._pairs = [];
    }
    const have = new Set(this._pairs.map((p) => `${p.source}\n${p.destination}`));
    for (const p of add) {
      if (!have.has(`${p.source}\n${p.destination}`)) this._pairs.push(p);
    }
    this._setDeviceMode(false);
    // A disabled entity has no live state, so the dropdowns list it only with
    // "Show deleted/disabled entities" on.
    const live = this._hass.states;
    const isLive = (id) => Object.prototype.hasOwnProperty.call(live, id);
    if (
      !this._showDeletedCb.checked &&
      add.some((p) => !isLive(p.source) || !isLive(p.destination))
    ) {
      this._showDeletedCb.checked = true;
      this._onShowDeletedChange();
    }
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
    // "Show deleted/disabled entities" toggle is on.
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
    const overwrite = !!this._overwriteCb.checked;
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

      // Overwrite deletes rows, so it gets its own explicit confirmation
      // naming the destinations that lose data.
      if (overwrite) {
        const dests = [...new Set(validPairs.map((p) => p.destination))];
        if (
          !confirm(
            "⚠️ OVERWRITE IS ON. THIS DELETES DATA.\n\n" +
              "For these destination entities, every state row inside the " +
              "source's time span will be deleted and replaced, and existing " +
              "statistics values will be overwritten:\n\n" +
              dests.map((d) => `  ${d}`).join("\n") +
              "\n\nDeleted rows cannot be recovered without a database backup." +
              "\n\nContinue?"
          )
        ) {
          return;
        }
      }
    }

    this._resultsContainer.innerHTML = "";
    panelMemory.outcome = null;
    // The run is kept in panelMemory, not on this panel: if Home Assistant
    // rebuilds the panel meanwhile, the new one follows the same run.
    const run = { dryRun, outcome: null };
    run.done = this._hass
      .callWS({
        type: "merge_sensor_history/import",
        pairs: validPairs,
        fill_gaps: fillGaps,
        gap_threshold_minutes: gapThresholdMinutes,
        dry_run: dryRun,
        overwrite: overwrite,
        scale_factor: scaleFactor,
        value_function: valueFunction,
      })
      .then(
        (response) => ({ dryRun, results: response.results }),
        (err) => ({ dryRun, error: String((err && err.message) || err) })
      )
      .then((outcome) => {
        run.outcome = outcome;
        if (panelMemory.run === run) {
          panelMemory.run = null;
          panelMemory.outcome = outcome;
        }
      });
    panelMemory.run = run;
    await this._followRun(run);
  }

  /** Show a run as busy until it ends, then show what it returned. */
  async _followRun(run) {
    this._importing = true;
    const btn = run.dryRun ? this._previewBtn : this._importBtn;
    btn.innerHTML = `<span class="spinner"></span>${run.dryRun ? "Analyzing" : "Importing"}\u2026`;
    this._syncActionButtons();
    await run.done;
    this._importing = false;
    this._importBtn.textContent = "Import History";
    this._previewBtn.textContent = "Preview";
    this._syncActionButtons();
    this._showOutcome(run.outcome);
  }

  /** Preview and Import are off while a run is going, and in device mode,
   *  whose pairs are not in the list yet. */
  _syncActionButtons() {
    const off = this._importing || this._deviceMode;
    this._importBtn.disabled = off;
    this._previewBtn.disabled = off;
  }

  _showOutcome(outcome) {
    if (!outcome) return;
    if (outcome.error === undefined) {
      try {
        this._renderResults(outcome.results);
      } catch (err) {
        this._resultsContainer.innerHTML = `
          <div class="result-item result-error">
            <div class="result-details">The ${outcome.dryRun ? "preview" : "import"} finished, but its results could not be shown: ${this._esc((err && err.message) || err)}</div>
          </div>`;
      }
      return;
    }
    const badge = outcome.dryRun
      ? '<span class="result-badge badge-preview">Preview</span>'
      : '<span class="result-badge badge-failed">Failed</span>';
    this._resultsContainer.innerHTML = `
      <div class="result-item result-error">
        <div class="result-header">
          <span class="result-icon">&#10060;</span>
          ${badge}
          <span class="result-pair">${outcome.dryRun ? "Preview" : "Import"} failed</span>
        </div>
        <div class="result-details">${this._esc(outcome.error)}</div>
      </div>`;
  }

  /** The rest of the units-differ warning: the usual conversion between the
   *  two units, with a button that fills it in under Options. Only ever a
   *  suggestion: nothing is applied until the user runs the import with it,
   *  and they can change it first. */
  _unitSuggestion(r, pairCount) {
    const sug = r.stats_unit_mismatch && r.stats_unit_mismatch.suggestion;
    const setNow = r.scale_factor || r.value_function;
    if (!sug) {
      return `If they need scaling, enable <strong>Adjust imported values</strong> under Options${setNow ? " (already set for this run)" : ""}.`;
    }
    const isFn = !!sug.value_function;
    const label = isFn
      ? `f(v) = ${this._esc(sug.value_function)}`
      : `&times;${this._esc(String(sug.scale_factor))}`;
    let text = `The usual conversion is <strong>${label}</strong>.`;
    const matches = isFn
      ? r.value_function === sug.value_function
      : !r.value_function && Number(r.scale_factor) === Number(sug.scale_factor);
    if (matches) {
      return `${text} <strong>Adjust imported values</strong> ${r.dry_run ? "is" : "was"} already set to this for this run.`;
    }
    if (setNow) text += ` <strong>Adjust imported values</strong> ${r.dry_run ? "is" : "was"} set to something else for this run.`;
    if (pairCount > 1) {
      return `${text} <strong>Adjust imported values</strong> applies to every pair in a run, so import this pair on its own to use it.`;
    }
    const data = isFn
      ? `data-fn="${this._esc(sug.value_function)}"`
      : `data-scale="${this._esc(String(sug.scale_factor))}"`;
    return `${text}<br/><button class="suggest-btn" ${data}>Use ${label}</button>
      <span class="suggest-outcome"></span>`;
  }

  /** Fill a suggested conversion in under Options, exactly as if typed. */
  _applyUnitSuggestion(btn) {
    const { scale, fn } = btn.dataset;
    this._scaleCb.checked = true;
    if (fn) {
      this._adjustModeCustom.checked = true;
      this._customFn.value = fn;
    } else {
      this._adjustModeMultiply.checked = true;
      this._scaleFactor.value = scale;
    }
    // Runs the same enable/disable and live-preview sync as a manual change.
    this._scaleCb.dispatchEvent(new Event("change"));
    btn.disabled = true;
    const outcome = btn.parentElement.querySelector(".suggest-outcome");
    if (outcome) {
      outcome.textContent =
        " Filled in under Options. Check it, then run Preview again before importing.";
    }
    this._scaleCb.scrollIntoView({ behavior: "smooth", block: "center" });
  }

  /** Explain a destination whose running total restarted from zero, and offer
   *  the repair. Returns "" when the series is fine. */
  _repairNotice(r) {
    const d = r.stats_detached;
    if (!d) return "";
    const liftStr = this._esc(this._formatOffset(d.lift, d.unit || r.stats_unit));
    const when = this._esc(this._formatTs(d.start));
    const dest = this._esc(r.destination);
    return `<div class="repair-notice">
      <strong>This destination's stored running total restarted from zero on ${when}.</strong><br/>
      Home Assistant continues a sensor's running total from that sensor's own most recent
      5-minute statistics row, and starts again from zero when there is none. At some point
      this destination had no statistics of its own to continue from, so everything it has
      recorded since ${when} counts up from zero instead of carrying on from the history
      before it. The history itself is intact; the two halves are simply on different
      baselines, which is what makes the Energy dashboard show a drop there.<br/>
      Repairing adds <strong>${liftStr}</strong> to every statistics row from ${when} onwards,
      so the two halves line up and future readings continue from the corrected total. Rows before
      ${when} are left alone, and per-hour and per-day figures do not change: only the running
      total moves.
      <br/>
      ${
        r.dry_run
          ? `<em>Run the import to get the option to repair this. A preview never writes anything.</em>`
          : r.stats_detached_repaired
            ? `<div class="repair-outcome">${this._repairDoneHtml(r.stats_detached_repaired)}</div>`
            : `<button class="repair-btn" data-statistic-id="${dest}">Repair running total</button>
             <div class="repair-outcome"></div>`
      }
    </div>`;
  }

  _repairDoneHtml(res) {
    const liftStr = this._esc(this._formatOffset(res.lift, res.unit));
    const when = this._esc(this._formatTs(res.start));
    return `✅ Running total repaired: every row from ${when} onwards was lifted by <strong>${liftStr}</strong>. The Energy dashboard may take a few minutes to catch up.`;
  }

  async _repairSumSeries(btn) {
    const statisticId = btn.dataset.statisticId;
    const outcome = btn.parentElement.querySelector(".repair-outcome");
    btn.disabled = true;
    btn.textContent = "Repairing…";
    try {
      const res = await this._hass.callWS({
        type: "merge_sensor_history/repair_sum_series",
        statistic_id: statisticId,
      });
      if (res.repaired) {
        outcome.innerHTML = this._repairDoneHtml(res);
        btn.remove();
        // Remembered on the result, so a rebuilt panel shows it as done.
        const done = { lift: res.lift, start: res.start, unit: res.unit };
        for (const r of this._lastResults || []) {
          if (r.destination === statisticId) r.stats_detached_repaired = done;
        }
      } else {
        outcome.textContent = res.reason || "Nothing to repair.";
        btn.disabled = false;
        btn.textContent = "Repair running total";
      }
    } catch (err) {
      outcome.textContent = `Repair failed: ${err.message || err}`;
      btn.disabled = false;
      btn.textContent = "Repair running total";
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

  /** The top line of a result card. A preview gets its own look and badge so
   *  it cannot be mistaken for a finished import. */
  _resultHeader(r, pairLabel) {
    let icon = "&#9989;";
    let badge;
    if (r.dry_run) {
      if (!r.error) icon = "&#128269;";
      badge = `<span class="result-badge badge-preview" title="A preview writes nothing. Click Import History to run it for real.">Preview</span>`;
    } else if (r.error) {
      badge = `<span class="result-badge badge-failed">Failed</span>`;
    } else if (this._partlyFailed(r)) {
      icon = "&#9888;&#65039;";
      badge = `<span class="result-badge badge-partial">Imported with errors</span>`;
    } else if (
      !r.states_imported &&
      !r.stats_imported &&
      !r.stats_short_imported
    ) {
      badge = `<span class="result-badge badge-nothing">Nothing to import</span>`;
    } else {
      badge = `<span class="result-badge badge-imported">Imported</span>`;
    }
    if (r.error) icon = "&#10060;";
    return `<div class="result-header">
        <span class="result-icon">${icon}</span>
        ${badge}
        <span class="result-pair">${pairLabel}</span>
      </div>`;
  }

  _partlyFailed(r) {
    return !!(
      r.stats_error ||
      r.stats_short_error ||
      r.stats_seed_error ||
      r.stats_realign_error
    );
  }

  _resultCardClass(r) {
    if (r.error) return "result-error";
    if (r.dry_run) return "result-preview";
    return this._partlyFailed(r) ? "result-partial" : "result-success";
  }

  _renderResults(results) {
    this._lastResults = results;
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
        const srcName = this._esc(this._friendlyName(r.source));
        const dstName = this._esc(this._friendlyName(r.destination));
        const srcLabel = srcName ? `${r.source} (${srcName})` : r.source;
        const dstLabel = dstName ? `${r.destination} (${dstName})` : r.destination;
        const pairLabel = `${srcLabel} \u2192 ${dstLabel}`;
        const header = this._resultHeader(r, pairLabel);
        const cardClass = this._resultCardClass(r);
        const actionVerb = r.dry_run ? "would be imported" : "imported";
        const pairKey = `${i}`;

        if (r.error) {
          // A destination can still be carrying a restart-from-zero even when
          // this pair failed (a source whose statistics are long gone, for
          // one), and repairing that does not depend on the source.
          const repairBlock = this._repairNotice(r);
          const aftermath = r.dry_run
            ? "This was a preview only, so nothing was written."
            : "The import was rolled back, so nothing was written and the database is unchanged. Re-run it once the cause is resolved.";
          return `<div class="result-item result-error">
            ${header}
            <div class="result-details">
              ${r.error}<br/>
              <em>${aftermath}</em>
              ${repairBlock}
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
          return `<div class="result-item ${cardClass}">
            ${header}
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

        // --- Overwrite notice ---
        if (r.overwrite) {
          grid += `<span class="result-stat-range" style="grid-column:1/-1;color:var(--error-color,#db4437)">&#9888;&#65039; <strong>Overwrite ${r.dry_run ? "would be" : "was"} used.</strong> Destination data inside the source's time span ${r.dry_run ? "would be" : "was"} replaced by the source's.</span>`;
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
          if (r.states_overwritten > 0) {
            grid += `<span class="result-stat-value">${r.states_overwritten.toLocaleString()}</span><span class="result-stat-label">destination rows ${r.dry_run ? "would be deleted" : "deleted"} and replaced</span>`;
            if (r.states_overwrite_start && r.states_overwrite_end)
              grid += `<span class="result-stat-range" style="grid-column:1/-1">Replaced window: ${this._formatTs(r.states_overwrite_start)} → ${this._formatTs(r.states_overwrite_end)}</span>`;
          }
          if (r.states_already_covered > 0)
            grid += `<span class="result-stat-value">${r.states_already_covered.toLocaleString()}</span><span class="result-stat-label">already present in destination</span>`;
          if (!r.fill_gaps && !r.overwrite && r.states_already_covered > 0)
            grid += `<span class="result-stat-range" style="grid-column:1/-1">Skipped states fall inside the destination's existing range. If part of that range looks empty in the History panel, enable <strong>Fill mid-stream gaps</strong> under Options and run a Preview to see what could be imported.</span>`;
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
          if (r.stats_overwritten > 0)
            grid += `<span class="result-stat-value">${r.stats_overwritten.toLocaleString()}</span><span class="result-stat-label">existing hours ${r.dry_run ? "would be overwritten" : "overwritten"}</span>`;
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
          if (r.stats_unit_mismatch) {
            const su = this._esc(r.stats_unit_mismatch.source || "no unit");
            const du = this._esc(r.stats_unit_mismatch.destination || "no unit");
            grid += `<span class="result-stat-range" style="grid-column:1/-1;color:var(--error-color,#db4437)">&#9888;&#65039; The source's statistics are stored in <strong>${su}</strong> but the destination's in <strong>${du}</strong>. Values ${r.dry_run ? "would be" : "were"} imported exactly as stored, with no conversion. ${this._unitSuggestion(r, results.length)}</span>`;
          }
          if (r.stats_sum_seeded !== null && r.stats_sum_seeded !== undefined) {
            const seedStr = this._formatOffset(r.stats_sum_seeded, r.stats_unit);
            grid += r.dry_run
              ? `<span class="result-stat-range" style="grid-column:1/-1">The destination has no statistics of its own yet, so its running total would be seeded at <strong>${seedStr}</strong>, where its history currently ends. Without that, the statistics Home Assistant compiles from now on would restart at zero.</span>`
              : `<span class="result-stat-range" style="grid-column:1/-1">The destination had no statistics of its own yet, so its running total was seeded at <strong>${seedStr}</strong>, where its history currently ends. The statistics Home Assistant compiles from now on continue from there instead of restarting at zero.</span>`;
          }
          if (r.stats_seed_error)
            grid += `<span class="result-stat-error">Could not seed the running total: ${r.stats_seed_error}</span>`;
          grid += this._repairNotice(r);
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
          if (r.stats_short_overwritten > 0)
            grid += `<span class="result-stat-value">${r.stats_short_overwritten.toLocaleString()}</span><span class="result-stat-label">existing slots ${r.dry_run ? "would be overwritten" : "overwritten"}</span>`;
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

        // --- Utility Meter destination: explain the drop in the History graph ---
        if (r.dest_is_utility_meter) {
          grid += `<span class="result-stat-range" style="grid-column:1/-1">&#8505;&#65039; <strong>The destination is a Utility Meter.</strong> A Utility Meter keeps its own running value inside the helper, counting from when it was created, and no import can change that value. The History graph can therefore show a drop where the imported history ends and the meter's own readings begin. That drop is in the meter's state only: the Energy dashboard reads statistics, not the meter's state, so it is not affected. Avoid lifting the meter with <strong>Utility Meter: Calibrate</strong> after importing: Home Assistant records the jump as consumption, which would count the imported history twice.</span>`;
        }

        return `<div class="result-item ${cardClass}">
          ${header}
          <div class="result-stat-grid">${grid}</div>
        </div>`;
      })
      .join("");
  }
}

customElements.define("merge-sensor-history-panel", MergeSensorsHistoryPanel);
