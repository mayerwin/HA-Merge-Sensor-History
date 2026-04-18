"""Merge Sensor History - Import history from one sensor into another."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any

import voluptuous as vol
from sqlalchemy import func as sql_func

from homeassistant.components import websocket_api
from homeassistant.components.frontend import (
    async_register_built_in_panel,
    async_remove_panel,
)
from homeassistant.components.http import StaticPathConfig
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.history import get_significant_states
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    get_metadata,
    statistics_during_period,
)
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
)

try:
    from homeassistant.components.recorder.models import StatisticMeanType
except ImportError:
    StatisticMeanType = None  # type: ignore[assignment,misc]
from homeassistant.components.recorder.db_schema import (
    States,
    StateAttributes,
    StatesMeta,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

# Epoch used as "beginning of time" for queries
_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Merge Sensor History from a config entry."""
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("_locks", {})

    # Register the panel with cache-busting hash
    panel_path = os.path.join(os.path.dirname(__file__), "frontend", "panel.js")
    with open(panel_path, "rb") as f:
        panel_hash = hashlib.md5(f.read()).hexdigest()[:8]

    await hass.http.async_register_static_paths(
        [StaticPathConfig(f"/{DOMAIN}/panel.js", panel_path, cache_headers=True)]
    )

    async_register_built_in_panel(
        hass,
        component_name="custom",
        sidebar_title="Merge History",
        sidebar_icon="mdi:history",
        frontend_url_path="merge-sensor-history",
        config={
            "_panel_custom": {
                "name": "merge-sensor-history-panel",
                "module_url": f"/{DOMAIN}/panel.js?v={panel_hash}",
                "embed_iframe": False,
            }
        },
        require_admin=True,
    )

    # Register websocket commands
    websocket_api.async_register_command(hass, ws_import_history)
    websocket_api.async_register_command(hass, ws_get_status)

    # Register service
    async def handle_import_history(call: ServiceCall) -> None:
        source = call.data["source_entity_id"]
        dest = call.data["destination_entity_id"]
        result = await _async_import_pair(hass, source, dest)
        if result["error"]:
            _LOGGER.error(
                "Import from %s to %s failed: %s", source, dest, result["error"]
            )
        else:
            _LOGGER.info(
                "Import from %s to %s complete: %d states, %d stats imported",
                source,
                dest,
                result["states_imported"],
                result["stats_imported"],
            )

    hass.services.async_register(
        DOMAIN,
        "import_history",
        handle_import_history,
        schema=vol.Schema(
            {
                vol.Required("source_entity_id"): cv.entity_id,
                vol.Required("destination_entity_id"): cv.entity_id,
            }
        ),
    )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    async_remove_panel(hass, "merge-sensor-history")
    hass.services.async_remove(DOMAIN, "import_history")
    hass.data.pop(DOMAIN, None)
    return True


# ---------------------------------------------------------------------------
# WebSocket API
# ---------------------------------------------------------------------------


@websocket_api.websocket_command(
    {
        vol.Required("type"): "merge_sensor_history/import",
        vol.Required("pairs"): [
            vol.Schema(
                {
                    vol.Required("source"): cv.entity_id,
                    vol.Required("destination"): cv.entity_id,
                }
            )
        ],
    }
)
@websocket_api.async_response
async def ws_import_history(
    hass: HomeAssistant, connection: websocket_api.ActiveConnection, msg: dict
) -> None:
    """Handle import request from the panel."""
    pairs = msg["pairs"]
    results = []

    for pair in pairs:
        result = await _async_import_pair(hass, pair["source"], pair["destination"])
        results.append(
            {
                "source": pair["source"],
                "destination": pair["destination"],
                **result,
            }
        )

    connection.send_result(msg["id"], {"results": results})


@websocket_api.websocket_command(
    {vol.Required("type"): "merge_sensor_history/status"}
)
@websocket_api.async_response
async def ws_get_status(
    hass: HomeAssistant, connection: websocket_api.ActiveConnection, msg: dict
) -> None:
    """Return a simple status check."""
    connection.send_result(msg["id"], {"ready": True})


# ---------------------------------------------------------------------------
# Import logic
# ---------------------------------------------------------------------------


async def _async_import_pair(
    hass: HomeAssistant, source_id: str, dest_id: str
) -> dict[str, Any]:
    """Import all history from source entity into destination entity.

    This function is IDEMPOTENT:
    - It only imports source states strictly older than the destination's
      oldest entry.
    - The state insertion is ATOMIC (single transaction): either all states
      are committed, or none are (full rollback).
    - Re-running after success: destination now has older data, so the cutoff
      moves earlier and nothing new qualifies. Zero states imported.
    - Re-running after failure: the rollback left the DB unchanged, so the
      same states qualify and are imported from scratch.

    Returns a dict with result details for the UI.
    """
    result: dict[str, Any] = {
        # States
        "states_source_total": 0,
        "states_imported": 0,
        "states_already_covered": 0,
        "states_imported_start": None,  # ISO datetime of first imported state
        "states_imported_end": None,  # ISO datetime of last imported state
        # Statistics
        "stats_source_total": 0,
        "stats_imported": 0,
        "stats_already_covered": 0,
        "stats_skipped_recent": 0,
        "stats_imported_start": None,  # ISO datetime (hour start) of first imported stat
        "stats_imported_end": None,  # ISO datetime (hour start) of last imported stat
        "stats_sum_offset": None,  # Applied offset value (or None)
        "stats_unit": None,  # Unit of measurement for display
        "error": None,
    }

    # --- Validate inputs ---
    if source_id == dest_id:
        result["error"] = "Source and destination cannot be the same entity."
        return result

    # --- Per-destination lock to prevent concurrent imports ---
    locks: dict[str, asyncio.Lock] = hass.data[DOMAIN]["_locks"]
    if dest_id not in locks:
        locks[dest_id] = asyncio.Lock()

    if locks[dest_id].locked():
        result["error"] = (
            f"An import into {dest_id} is already in progress. "
            "Please wait for it to finish."
        )
        return result

    async with locks[dest_id]:
        try:
            await _do_import(hass, source_id, dest_id, result)
        except Exception as exc:
            _LOGGER.exception(
                "Error importing history from %s to %s", source_id, dest_id
            )
            result["error"] = f"Import failed: {exc}"

    return result


async def _do_import(
    hass: HomeAssistant,
    source_id: str,
    dest_id: str,
    result: dict[str, Any],
) -> None:
    """Execute the actual import. Separated for clean lock/error handling."""
    recorder = get_instance(hass)

    # --- 1. Read ALL source states ---
    # Use get_significant_states with significant_changes_only=False to capture
    # EVERY state row, including attribute-only changes.
    source_states_dict = await recorder.async_add_executor_job(
        partial(
            get_significant_states,
            hass,
            _EPOCH,
            entity_ids=[source_id],
            significant_changes_only=False,
            include_start_time_state=True,
            no_attributes=False,
        )
    )

    source_states = source_states_dict.get(source_id, [])
    if not source_states:
        result["error"] = (
            f"No history found for source entity '{source_id}'. "
            f"It may have been purged (default: 10 days) or the entity ID is wrong."
        )
        return

    result["states_source_total"] = len(source_states)
    _LOGGER.info(
        "Read %d states from source entity %s (oldest: %s, newest: %s)",
        len(source_states),
        source_id,
        source_states[0].last_updated.isoformat(),
        source_states[-1].last_updated.isoformat(),
    )

    # --- 2. Insert states in a single ATOMIC transaction ---
    # The cutoff (destination's oldest timestamp) is queried INSIDE the same
    # transaction as the insert, so there is no TOCTOU race. The query uses
    # MIN(last_updated_ts) which captures ALL row types (value changes AND
    # attribute-only changes).
    (
        imported,
        already_covered,
        _cutoff_ts,
        imported_min_ts,
        imported_max_ts,
    ) = await recorder.async_add_executor_job(
        _insert_states_atomic, recorder, dest_id, source_states
    )
    result["states_imported"] = imported
    result["states_already_covered"] = already_covered
    if imported_min_ts is not None:
        result["states_imported_start"] = datetime.fromtimestamp(
            imported_min_ts, tz=timezone.utc
        ).isoformat()
        result["states_imported_end"] = datetime.fromtimestamp(
            imported_max_ts, tz=timezone.utc
        ).isoformat()

    # --- 3. Import statistics (gap-fill semantics) ---
    # Only inserts for hours where the destination has no existing LTS row.
    # Applies a cumulative-sum offset for energy sensors (has_sum=True) so
    # that the imported `sum` series joins the destination's existing series
    # smoothly at the splice point. The recent in-progress hour is skipped
    # to avoid colliding with HA's own hourly compile (which uses plain
    # INSERT and would silently roll back the whole compile transaction on
    # unique-index conflict).
    # Done independently: a stats failure should not hide a successful states import.
    try:
        stats_result = await _async_import_statistics_for_pair(
            hass, source_id, dest_id
        )
        result.update(stats_result)
    except Exception as exc:
        _LOGGER.warning(
            "States imported successfully, but statistics import failed for "
            "%s -> %s: %s",
            source_id,
            dest_id,
            exc,
        )
        result["stats_error"] = str(exc)


def _insert_states_atomic(
    recorder_instance: Any,
    dest_entity_id: str,
    source_states: list,
) -> tuple[int, int, float | None, float | None, float | None]:
    """Insert State objects into the recorder database for a destination entity.

    This function is ATOMIC: either ALL states are committed, or NONE are
    (full rollback on any error).

    Idempotency comes from the cutoff rule: only source states strictly older
    than the destination's oldest existing entry are imported. After a successful
    import, the destination's oldest entry IS one of the imported states, so a
    re-run will find nothing new to import.

    Returns (inserted_count, already_covered_count, cutoff_ts, imported_min_ts,
    imported_max_ts). The last two are None if nothing was imported.
    """
    inserted = 0
    imported_min_ts: float | None = None
    imported_max_ts: float | None = None
    session = recorder_instance.get_session()

    try:
        # -- Get or create StatesMeta for destination entity --
        meta = (
            session.query(StatesMeta)
            .filter(StatesMeta.entity_id == dest_entity_id)
            .first()
        )
        if meta is None:
            meta = StatesMeta(entity_id=dest_entity_id)
            session.add(meta)
            session.flush()

        metadata_id = meta.metadata_id

        # -- Query the TRUE oldest timestamp for the destination entity --
        # This runs in the same transaction as the inserts: no TOCTOU race.
        # Uses MIN(last_updated_ts) which captures ALL row types.
        min_ts: float | None = (
            session.query(sql_func.min(States.last_updated_ts))
            .filter(States.metadata_id == metadata_id)
            .scalar()
        )

        # -- Filter source states by cutoff --
        if min_ts is not None:
            # Convert to datetime for consistent comparison (avoids float
            # precision issues from the datetime→float→datetime roundtrip).
            cutoff_dt = datetime.fromtimestamp(min_ts, tz=timezone.utc)
            to_import = [s for s in source_states if s.last_updated < cutoff_dt]
            _LOGGER.info(
                "Destination %s oldest entry: %s — importing %d of %d source states",
                dest_entity_id,
                cutoff_dt.isoformat(),
                len(to_import),
                len(source_states),
            )
        else:
            to_import = source_states
            _LOGGER.info(
                "Destination %s has no history — importing all %d source states",
                dest_entity_id,
                len(source_states),
            )

        already_covered = len(source_states) - len(to_import)

        if not to_import:
            return 0, already_covered, min_ts, None, None

        # source_states comes from get_significant_states in chronological order,
        # so to_import[0] is oldest and to_import[-1] is newest.
        imported_min_ts = to_import[0].last_updated.timestamp()
        imported_max_ts = to_import[-1].last_updated.timestamp()

        # -- Attribute dedup cache: hash -> attributes_id --
        attrs_cache: dict[int, int] = {}

        for i, state in enumerate(to_import):
            last_updated_ts = state.last_updated.timestamp()

            # -- Resolve attributes --
            attributes_id = _get_or_create_attributes(
                session, state.attributes, attrs_cache
            )

            # -- Compute last_changed_ts --
            # HA convention: NULL means "same as last_updated_ts" (saves space).
            if state.last_changed == state.last_updated:
                last_changed_ts = None
            else:
                last_changed_ts = state.last_changed.timestamp()

            # -- Compute last_reported_ts --
            # NULL means "same as last_updated_ts".
            last_reported_ts = None
            last_reported = getattr(state, "last_reported", None)
            if last_reported is not None and last_reported != state.last_updated:
                last_reported_ts = last_reported.timestamp()

            # -- Build the States row --
            db_state = States(
                state=str(state.state)[:255] if state.state is not None else None,
                metadata_id=metadata_id,
                attributes_id=attributes_id,
                last_changed_ts=last_changed_ts,
                last_updated_ts=last_updated_ts,
                last_reported_ts=last_reported_ts,
                old_state_id=None,
                origin_idx=0,  # local origin
                context_id_bin=None,
                context_user_id_bin=None,
                context_parent_id_bin=None,
            )
            session.add(db_state)
            inserted += 1

            # Flush periodically to keep ORM memory bounded.
            # This writes to the DB journal but does NOT commit — the entire
            # batch remains in one transaction.
            if inserted % 1000 == 0:
                session.flush()
                _LOGGER.debug(
                    "Flushed %d/%d states for %s",
                    inserted,
                    len(to_import),
                    dest_entity_id,
                )

        # -- SINGLE commit: all or nothing --
        session.commit()
        _LOGGER.info(
            "Committed %d states for %s (%d source states already covered)",
            inserted,
            dest_entity_id,
            already_covered,
        )

    except Exception:
        session.rollback()
        _LOGGER.error(
            "Rolling back entire import for %s — no states were written",
            dest_entity_id,
        )
        raise
    finally:
        session.close()

    return inserted, already_covered, min_ts, imported_min_ts, imported_max_ts


def _get_or_create_attributes(
    session: Any,
    attributes: dict | None,
    cache: dict[int, int],
) -> int:
    """Return an attributes_id for the given attribute dict.

    Reuses existing rows via hash-based deduplication (same approach as HA core).
    """
    try:
        attrs_dict = dict(attributes) if attributes else {}
        shared_attrs = json.dumps(attrs_dict, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        shared_attrs = "{}"

    shared_attrs_bytes = shared_attrs.encode("utf-8")

    try:
        attr_hash = StateAttributes.hash_shared_attrs_bytes(shared_attrs_bytes)
    except (AttributeError, TypeError):
        # Fallback: HA changed the method signature
        attr_hash = hash(shared_attrs_bytes) & 0xFFFFFFFFFFFFFFFF

    if attr_hash in cache:
        return cache[attr_hash]

    # Check DB for an existing row with this hash AND matching content
    existing = (
        session.query(StateAttributes)
        .filter(StateAttributes.hash == attr_hash)
        .first()
    )
    if existing and existing.shared_attrs == shared_attrs:
        cache[attr_hash] = existing.attributes_id
        return existing.attributes_id

    # Create new attributes row
    new_attrs = StateAttributes(hash=attr_hash, shared_attrs=shared_attrs)
    session.add(new_attrs)
    session.flush()
    cache[attr_hash] = new_attrs.attributes_id
    return new_attrs.attributes_id


# ---------------------------------------------------------------------------
# Statistics import (uses official HA API — already idempotent)
# ---------------------------------------------------------------------------


def _row_start_ts(row: dict) -> float:
    """Normalize a statistics row's `start` to a float epoch timestamp."""
    start = row["start"]
    if isinstance(start, (int, float)):
        return float(start)
    return start.timestamp()


def _compute_sum_offset(
    source_rows: list[dict], dest_rows: list[dict]
) -> float | None:
    """Compute the offset to apply to imported source `sum` values so that the
    imported series joins the destination's existing series smoothly at the
    splice point.

    The splice point is the earliest destination hour that has a non-NULL `sum`.
    The offset is `dest.sum - source.sum` at (or just before) that hour:

      - If the source has a row AT the splice hour: use it directly.
      - Otherwise: use the most recent source row BEFORE the splice hour.
        (Treats any small gap as zero consumption, which is the correct
        approximation when the two sensors ran in parallel.)

    Returns None if no offset is needed (no overlap / no sum data on one side /
    offset is effectively zero).
    """
    dest_sum_rows = [r for r in dest_rows if r.get("sum") is not None]
    if not dest_sum_rows:
        return None

    splice_dest = min(dest_sum_rows, key=_row_start_ts)
    splice_ts = _row_start_ts(splice_dest)

    src_candidates = [
        r
        for r in source_rows
        if r.get("sum") is not None and _row_start_ts(r) <= splice_ts
    ]
    if not src_candidates:
        return None

    splice_src = max(src_candidates, key=_row_start_ts)
    offset = float(splice_dest["sum"]) - float(splice_src["sum"])

    # Don't report a "zero" offset as applied — it's visual noise.
    if abs(offset) < 1e-9:
        return None
    return offset


async def _async_import_statistics_for_pair(
    hass: HomeAssistant,
    source_id: str,
    dest_id: str,
) -> dict[str, Any]:
    """Import long-term statistics from source to destination — gap-fill mode.

    Key behaviors:

    1. **Gap-fill, not overwrite.** Only inserts for hours where the destination
       has no existing LTS row. Existing destination rows are preserved as-is.
       This prevents the previous upsert behavior from accidentally nulling out
       populated columns (e.g. setting `sum=NULL` because the source row only
       had `mean` set — `_update_statistics` uses `.get()` for every column).

    2. **Recent-hour cutoff.** The last fully-compiled hour is `floor_hour(now)`;
       we stop one hour before that to leave a safety margin against HA's own
       hourly compile, which runs plain INSERT (not upsert) and would silently
       roll back its entire compile transaction on a unique-index conflict.

    3. **Cumulative-sum offset for energy sensors.** For sensors with
       `has_sum=True` (total / total_increasing), the imported `sum` values are
       shifted by `dest.sum - source.sum` at the splice point so the imported
       series joins the existing series without a jump or drop.

    4. **Preserve existing destination metadata.** If the destination already has
       stats metadata, we reuse it verbatim (minus `statistic_id`/`source`, which
       are forced). This avoids triggering metadata thrash with HA's sensor
       recorder (which rewrites metadata on every hourly compile from the live
       sensor's attributes).

    Returns a dict that extends the pair's result with `stats_*` fields.
    """
    recorder = get_instance(hass)

    out: dict[str, Any] = {
        "stats_source_total": 0,
        "stats_imported": 0,
        "stats_already_covered": 0,
        "stats_skipped_recent": 0,
        "stats_gap_filled": 0,  # hours where dest had a row but NULL in some column source provides
        "stats_imported_start": None,
        "stats_imported_end": None,
        "stats_sum_offset": None,
        "stats_unit": None,
    }

    # -- Compute recent-hour cutoff (UTC, aligned to hour) --
    # HA compiles hour H to LTS at time H+1:00:05 (during the :55→:00 5-min
    # cycle). To be safe, never write a row whose hour HA might still be about
    # to compile — otherwise our INSERT triggers a unique-index conflict that
    # silently rolls back HA's whole compile transaction (other entities lose
    # their stats too). We require: `now` is at least a few minutes past the
    # boundary that would have triggered the compile of the candidate hour.
    now = datetime.now(timezone.utc)
    floor_hour = now.replace(minute=0, second=0, microsecond=0)
    # If we're in the first ~10 minutes of the hour, HA may still be compiling
    # the just-finished hour, so step back one more.
    safety_offset_hours = 1 if now.minute >= 10 else 2
    recent_cutoff_dt = floor_hour - timedelta(hours=safety_offset_hours)
    recent_cutoff_ts = recent_cutoff_dt.timestamp()

    # -- Query source + destination stats in parallel (single executor call each) --
    source_stats_raw, dest_stats_raw, dest_metadata_map = (
        await recorder.async_add_executor_job(
            _fetch_stats_snapshot, hass, source_id, dest_id
        )
    )

    source_rows = source_stats_raw.get(source_id, [])
    dest_rows = dest_stats_raw.get(dest_id, [])

    out["stats_source_total"] = len(source_rows)
    if not source_rows:
        return out

    # -- Compute sum offset (None if not applicable) --
    sum_offset = _compute_sum_offset(source_rows, dest_rows)

    # -- Build destination row lookup by start_ts --
    # IMPORTANT: a row existing at a given hour does NOT mean it's "covered".
    # It may have NULL for columns the user cares about (e.g. sum=NULL on an
    # energy sensor that lost its totalizer reading), which shows up as a
    # visual gap in the dashboard. We detect those per-column and merge.
    dest_by_start: dict[float, dict] = {_row_start_ts(r): r for r in dest_rows}

    stat_cols = ("mean", "min", "max", "sum", "state")

    # -- Partition source rows: import / merge / skip-covered / skip-recent --
    # Each entry in to_import_rows is (start_ts, data_dict) — the full row to
    # pass to async_import_statistics. For merge cases, data_dict starts with
    # the destination's existing non-NULL values to avoid wiping them (because
    # HA's _update_statistics uses .get() for every column — omitting a column
    # sets it to NULL in the DB).
    to_import_rows: list[tuple[float, dict[str, Any]]] = []
    already_covered = 0
    skipped_recent = 0
    gap_filled = 0

    for src_row in source_rows:
        start_ts = _row_start_ts(src_row)
        if start_ts > recent_cutoff_ts:
            skipped_recent += 1
            continue

        src_values = {k: src_row[k] for k in stat_cols if src_row.get(k) is not None}
        if not src_values:
            # Source has a row for this hour but nothing useful in it.
            already_covered += 1
            continue

        dest_row = dest_by_start.get(start_ts)

        if dest_row is None:
            # No destination row: insert all source values (with sum offset).
            data = dict(src_values)
            if "sum" in data and sum_offset is not None:
                data["sum"] = float(data["sum"]) + sum_offset
            to_import_rows.append((start_ts, data))
            continue

        dest_values = {k: dest_row[k] for k in stat_cols if dest_row.get(k) is not None}
        fillable = {k: v for k, v in src_values.items() if k not in dest_values}

        if not fillable:
            # Destination already has non-NULL values for every column source
            # provides — nothing to fill.
            already_covered += 1
            continue

        # Merge: start with dest's non-NULL values (to preserve them against
        # _update_statistics' full-column overwrite), then layer source's
        # fills for the NULL columns.
        data = dict(dest_values)
        for k, v in fillable.items():
            if k == "sum" and sum_offset is not None:
                v = float(v) + sum_offset
            data[k] = v

        to_import_rows.append((start_ts, data))
        gap_filled += 1

    out["stats_already_covered"] = already_covered
    out["stats_skipped_recent"] = skipped_recent
    out["stats_gap_filled"] = gap_filled

    if not to_import_rows:
        if sum_offset is not None:
            out["stats_sum_offset"] = sum_offset
        return out

    # -- Resolve metadata: prefer destination's existing metadata --
    has_sum = any(r.get("sum") is not None for r in source_rows)
    has_mean = any(r.get("mean") is not None for r in source_rows)

    dest_meta_entry = dest_metadata_map.get(dest_id) if dest_metadata_map else None
    existing_metadata = dest_meta_entry[1] if dest_meta_entry else None

    unit: str | None = None
    if existing_metadata:
        # Reuse the destination's current metadata verbatim, except that we
        # force statistic_id and source (these must match for async_import_statistics).
        metadata = dict(existing_metadata)
        metadata["statistic_id"] = dest_id
        metadata["source"] = "recorder"
        unit = metadata.get("unit_of_measurement")
    else:
        # Destination has no metadata yet — construct from the live sensor.
        state_obj = hass.states.get(dest_id) or hass.states.get(source_id)
        if state_obj:
            unit = state_obj.attributes.get("unit_of_measurement")

        meta_kwargs: dict[str, Any] = {
            "has_sum": has_sum,
            "name": None,
            "source": "recorder",
            "statistic_id": dest_id,
            "unit_of_measurement": unit,
        }
        if StatisticMeanType is not None:
            meta_kwargs["mean_type"] = (
                StatisticMeanType.ARITHMETIC if has_mean else StatisticMeanType.NONE
            )
        else:
            meta_kwargs["has_mean"] = has_mean
        metadata = StatisticMetaData(**meta_kwargs)

    out["stats_unit"] = unit

    # -- Build StatisticData entries --
    # data dicts already have sum_offset applied (during merge/partition) and
    # already include destination's existing non-NULL columns when merging, so
    # _update_statistics' full-column overwrite won't wipe them.
    stats_data = []
    for start_ts, data in to_import_rows:
        start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        entry: dict[str, Any] = {"start": start_dt}
        for key in ("mean", "min", "max", "sum", "state"):
            if key in data:
                entry[key] = data[key]
        stats_data.append(StatisticData(**entry))

    # -- Queue the import (fire-and-forget on the recorder thread) --
    async_import_statistics(hass, metadata, stats_data)

    imported_starts = sorted(start_ts for start_ts, _ in to_import_rows)
    out["stats_imported"] = len(stats_data)
    out["stats_imported_start"] = datetime.fromtimestamp(
        imported_starts[0], tz=timezone.utc
    ).isoformat()
    out["stats_imported_end"] = datetime.fromtimestamp(
        imported_starts[-1], tz=timezone.utc
    ).isoformat()
    if sum_offset is not None:
        out["stats_sum_offset"] = sum_offset

    _LOGGER.info(
        "Queued %d statistics rows for %s "
        "(%d already complete in destination, %d gap-filled (NULL columns), "
        "%d skipped as too recent, sum offset: %s)",
        len(stats_data),
        dest_id,
        already_covered,
        gap_filled,
        skipped_recent,
        sum_offset,
    )
    return out


def _fetch_stats_snapshot(
    hass: HomeAssistant, source_id: str, dest_id: str
) -> tuple[dict, dict, dict]:
    """Fetch source stats, destination stats, and destination metadata in the
    recorder thread (single executor call)."""
    types = {"mean", "min", "max", "sum", "state"}
    source_stats = statistics_during_period(
        hass,
        _EPOCH,
        None,
        statistic_ids={source_id},
        period="hour",
        units=None,
        types=types,
    )
    dest_stats = statistics_during_period(
        hass,
        _EPOCH,
        None,
        statistic_ids={dest_id},
        period="hour",
        units=None,
        types=types,
    )
    dest_metadata = get_metadata(hass, statistic_ids={dest_id})
    return source_stats, dest_stats, dest_metadata
