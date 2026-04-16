"""Merge Sensor History - Import history from one sensor into another."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from functools import partial
from typing import Any

import voluptuous as vol
from sqlalchemy import func as sql_func

from homeassistant.components import websocket_api
from homeassistant.components.frontend import (
    async_register_built_in_panel,
    async_remove_panel,
)
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.history import get_significant_states
from homeassistant.components.recorder.statistics import (
    async_import_statistics,
    statistics_during_period,
)
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
    StatisticMeanType,
)
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

    # Register the panel
    panel_path = os.path.join(os.path.dirname(__file__), "frontend", "panel.js")
    hass.http.register_static_path(
        f"/{DOMAIN}/panel.js", panel_path, cache_headers=True
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
                "module_url": f"/{DOMAIN}/panel.js",
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
        "states_imported": 0,
        "states_already_covered": 0,
        "stats_imported": 0,
        "source_total": 0,
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

    result["source_total"] = len(source_states)
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
    imported, already_covered, cutoff_ts = await recorder.async_add_executor_job(
        _insert_states_atomic, recorder, dest_id, source_states
    )
    result["states_imported"] = imported
    result["states_already_covered"] = already_covered

    # --- 3. Import statistics via official API (already idempotent) ---
    # Done independently: a stats failure should not hide a successful states import.
    cutoff_dt = (
        datetime.fromtimestamp(cutoff_ts, tz=timezone.utc)
        if cutoff_ts is not None
        else None
    )
    try:
        stats_count = await _async_import_statistics_for_pair(
            hass, source_id, dest_id, cutoff_dt
        )
        result["stats_imported"] = stats_count
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
) -> tuple[int, int, float | None]:
    """Insert State objects into the recorder database for a destination entity.

    This function is ATOMIC: either ALL states are committed, or NONE are
    (full rollback on any error).

    Idempotency comes from the cutoff rule: only source states strictly older
    than the destination's oldest existing entry are imported. After a successful
    import, the destination's oldest entry IS one of the imported states, so a
    re-run will find nothing new to import.

    Returns (inserted_count, already_covered_count, cutoff_timestamp_or_none).
    """
    inserted = 0
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
            return 0, already_covered, min_ts

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

    return inserted, already_covered, min_ts


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


async def _async_import_statistics_for_pair(
    hass: HomeAssistant,
    source_id: str,
    dest_id: str,
    cutoff: datetime | None,
) -> int:
    """Import long-term statistics from source to destination.

    Uses the official async_import_statistics API which has built-in
    deduplication via unique constraint on (metadata_id, start_ts).
    """
    recorder = get_instance(hass)

    try:
        source_stats = await recorder.async_add_executor_job(
            partial(
                statistics_during_period,
                hass,
                _EPOCH,
                statistic_ids={source_id},
                period="hour",
                types={"mean", "min", "max", "sum", "state", "last_reset"},
            )
        )
    except Exception:
        _LOGGER.debug("Could not read statistics for %s", source_id, exc_info=True)
        return 0

    stat_rows = source_stats.get(source_id, [])
    if not stat_rows:
        return 0

    if cutoff:
        stat_rows = [r for r in stat_rows if r["start"] < cutoff]
        if not stat_rows:
            return 0

    # Look up unit info from the entity's current state
    state_obj = hass.states.get(dest_id) or hass.states.get(source_id)
    unit = None
    device_class = None
    if state_obj:
        unit = state_obj.attributes.get("unit_of_measurement")
        device_class = state_obj.attributes.get("device_class")

    has_sum = any(r.get("sum") is not None for r in stat_rows)
    has_mean = any(r.get("mean") is not None for r in stat_rows)

    mean_type = StatisticMeanType.ARITHMETIC if has_mean else StatisticMeanType.NONE

    unit_class_map = {
        "temperature": "temperature",
        "humidity": "humidity",
        "pressure": "pressure",
        "energy": "energy",
        "power": "power",
        "voltage": "voltage",
        "current": "current",
        "battery": "battery",
        "illuminance": "illuminance",
        "speed": "speed",
        "wind_speed": "wind_speed",
        "precipitation": "precipitation",
        "precipitation_intensity": "precipitation_intensity",
        "distance": "distance",
        "volume": "volume",
        "weight": "weight",
        "irradiance": "irradiance",
    }
    unit_class = unit_class_map.get(device_class) if device_class else None

    metadata = StatisticMetaData(
        has_sum=has_sum,
        mean_type=mean_type,
        name=None,
        source="recorder",
        statistic_id=dest_id,
        unit_class=unit_class,
        unit_of_measurement=unit,
    )

    stats_data = []
    for row in stat_rows:
        entry: dict[str, Any] = {"start": row["start"]}
        for key in ("mean", "min", "max", "sum", "state", "last_reset"):
            if row.get(key) is not None:
                entry[key] = row[key]
        stats_data.append(StatisticData(**entry))

    async_import_statistics(hass, metadata, stats_data)
    _LOGGER.info("Imported %d statistics rows for %s", len(stats_data), dest_id)
    return len(stats_data)
