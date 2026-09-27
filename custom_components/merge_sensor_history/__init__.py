"""Merge Sensor History - Import history from one sensor into another."""

from __future__ import annotations

import ast
import asyncio
import bisect
import concurrent.futures
import hashlib
import json
import logging
import math
import os
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any

import voluptuous as vol
from sqlalchemy import func as sql_func
from sqlalchemy import text as sql_text

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
)
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
)

try:
    from homeassistant.components.recorder.models import StatisticMeanType
except ImportError:
    StatisticMeanType = None  # type: ignore[assignment,misc]

try:
    from homeassistant.components.recorder.statistics import (
        STATISTIC_UNIT_TO_UNIT_CONVERTER,
    )
except ImportError:  # pragma: no cover - older HA without the converter map
    STATISTIC_UNIT_TO_UNIT_CONVERTER = {}  # type: ignore[assignment]
from homeassistant.components.recorder.db_schema import (
    States,
    StateAttributes,
    StatesMeta,
    Statistics,
    StatisticsMeta,
    StatisticsShortTerm,
)
try:
    from homeassistant.components.recorder.tasks import RecorderTask
except ImportError:  # pragma: no cover - older/newer HA without this module
    RecorderTask = None  # type: ignore[assignment,misc]
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.helpers import config_validation as cv

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

# Epoch used as "beginning of time" for queries
_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

# State values that HA hides in the History panel — also excluded from
# gap-detection so a long unavailable streak registers as a fillable gap.
_NON_GOOD_STATES = frozenset({"unavailable", "unknown"})

# --- Write strategy ---------------------------------------------------------
# The whole import is written in ONE transaction: either all of it lands or
# none of it does. Nothing here trades that away for throughput.
#
# The reason an earlier version had to think about this at all is SQLite, which
# allows a single writer. The recorder commits its own event queue about once a
# second, and if it is writing through a DIFFERENT connection than we are, our
# transaction locks it out. It does not give up quickly (it retries
# `db_max_retries`, 10 by default, waiting `db_retry_wait`, 3s, between
# attempts that each wait out the driver's 5s busy timeout, so roughly 80
# seconds of patience), but once it does give up, `_reopen_event_session`
# rolls its pending events back and those events are gone. That is the data
# loss behind issue #15, where an import written one ORM row at a time held the
# lock far longer than that.
#
# The fix is not to give up atomicity, it is to stop competing for the lock.
# The import runs as a task on the RECORDER'S OWN THREAD, through the same
# queue and the same connection the recorder uses for everything else, exactly
# as Home Assistant's own bulk `ImportStatisticsTask` does. There is then no
# second writer, so "database is locked" cannot happen no matter how long the
# write takes. Events simply queue while the task runs and drain afterwards,
# which is ordinary recorder behaviour: the backlog only becomes a problem past
# 65,000 queued events AND less than 256MB of free memory, and an import
# bounded by the recorder's ~10 day state retention does not come close.
#
# Rows still go in with a multi-row INSERT rather than one ORM object each,
# which is roughly an order of magnitude faster and keeps that queueing window
# short: about 1 second per 100k states, so a large import measured in the
# hundreds of thousands costs a few seconds of queued events.
#
# Rows per INSERT. Purely a memory/round-trip trade-off within the one
# transaction, and also how often cancellation is checked.
_WRITE_BATCH_ROWS = 10_000
# Only used on the fallback path below, where we are not on the recorder
# thread and therefore do compete for the lock. Waiting beats failing fast.
_SQLITE_BUSY_TIMEOUT_MS = 120_000


def _set_sqlite_busy_timeout(session: Any) -> None:
    """Make this connection wait for the write lock instead of failing fast.

    No-op on any engine other than SQLite. A failure here is not fatal: the
    import still works with the driver default, it is just less tolerant of a
    momentarily busy recorder.
    """
    try:
        bind = session.get_bind()
        if bind is None or bind.dialect.name != "sqlite":
            return
        session.execute(
            sql_text(f"PRAGMA busy_timeout = {_SQLITE_BUSY_TIMEOUT_MS}")
        )
    except Exception:  # pragma: no cover - best effort tuning only
        _LOGGER.debug(
            "Could not set busy_timeout on the import connection", exc_info=True
        )


# --- Safe value-adjustment expressions ------------------------------------
# A custom function is a plain math formula of `v` (the source value), e.g.
# "v / 1000 + 3" or "v * 9/5 + 32". It is NEVER executed as code: the string
# is parsed with ast.parse and only a strict whitelist of node types survives
# (numbers, v/pi/e, arithmetic operators, calls to the functions below). The
# validated AST is then interpreted directly — no eval/exec, no attribute
# access, no subscripting, no strings — and every operand is coerced to float
# so pathological inputs like 9**9**9**9 overflow immediately instead of
# allocating unbounded big-ints.

_VALUE_FUNCTION_MAX_LEN = 200

# name -> (callable, min_args, max_args)
_MATH_FUNCS: dict[str, tuple[Callable[..., float], int, int]] = {
    "abs": (lambda x: abs(x), 1, 1),
    "round": (lambda x: float(round(x)), 1, 1),
    "floor": (lambda x: float(math.floor(x)), 1, 1),
    "ceil": (lambda x: float(math.ceil(x)), 1, 1),
    "sqrt": (math.sqrt, 1, 1),
    "log": (math.log, 1, 2),  # log(x) natural, log(x, base)
    "log10": (math.log10, 1, 1),
    "log2": (math.log2, 1, 1),
    "exp": (math.exp, 1, 1),
    "min": (lambda *xs: float(min(xs)), 2, 8),
    "max": (lambda *xs: float(max(xs)), 2, 8),
    "pow": (lambda a, b: float(a) ** float(b), 2, 2),
}
_MATH_CONSTS = {"pi": math.pi, "e": math.e}


def _compile_value_function(expr: str) -> Callable[[float], float]:
    """Compile a restricted math expression into a float -> float callable.

    Raises ValueError with a user-readable message on anything outside the
    whitelisted grammar. `^` is accepted as power and a `Math.` prefix is
    tolerated so JavaScript-style formulas work.
    """
    normalized = str(expr).strip().lower().replace("math.", "").replace("^", "**")
    if not normalized:
        raise ValueError("The formula is empty.")
    if len(normalized) > _VALUE_FUNCTION_MAX_LEN:
        raise ValueError(
            f"The formula is too long (max {_VALUE_FUNCTION_MAX_LEN} characters)."
        )
    try:
        tree = ast.parse(normalized, mode="eval")
    except (SyntaxError, ValueError, RecursionError, MemoryError) as exc:
        raise ValueError(f"Not a valid math formula: {exc}") from exc

    uses_v = False

    def build(node: ast.AST) -> Callable[[float], float]:
        nonlocal uses_v
        if isinstance(node, ast.Expression):
            return build(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or not isinstance(
                node.value, (int, float)
            ):
                raise ValueError("Only plain numbers are allowed as constants.")
            const_val = float(node.value)
            return lambda v: const_val
        if isinstance(node, ast.Name):
            if node.id == "v":
                uses_v = True
                return lambda v: v
            if node.id in _MATH_CONSTS:
                named_const = _MATH_CONSTS[node.id]
                return lambda v: named_const
            raise ValueError(
                f"Unknown name '{node.id}' — only v, pi and e are allowed."
            )
        if isinstance(node, ast.UnaryOp) and isinstance(
            node.op, (ast.UAdd, ast.USub)
        ):
            operand = build(node.operand)
            if isinstance(node.op, ast.USub):
                return lambda v: -operand(v)
            return operand
        if isinstance(node, ast.BinOp) and isinstance(
            node.op,
            (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.Pow, ast.FloorDiv),
        ):
            left, right = build(node.left), build(node.right)
            op = type(node.op)
            if op is ast.Add:
                return lambda v: left(v) + right(v)
            if op is ast.Sub:
                return lambda v: left(v) - right(v)
            if op is ast.Mult:
                return lambda v: left(v) * right(v)
            if op is ast.Div:
                return lambda v: left(v) / right(v)
            if op is ast.FloorDiv:
                return lambda v: float(left(v) // right(v))
            if op is ast.Mod:
                # math.fmod matches the sign behavior of the % operator in
                # JavaScript/C, which is what formula authors expect.
                return lambda v: math.fmod(left(v), right(v))
            return lambda v: float(left(v)) ** float(right(v))  # ast.Pow
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in _MATH_FUNCS:
                raise ValueError(
                    "Only these functions are allowed: "
                    + ", ".join(sorted(_MATH_FUNCS))
                )
            if node.keywords:
                raise ValueError("Keyword arguments are not allowed.")
            func, min_args, max_args = _MATH_FUNCS[node.func.id]
            if not (min_args <= len(node.args) <= max_args):
                raise ValueError(
                    f"{node.func.id}() takes {min_args}"
                    + (f" to {max_args}" if max_args != min_args else "")
                    + " argument(s)."
                )
            arg_fns = [build(a) for a in node.args]
            return lambda v: float(func(*(a(v) for a in arg_fns)))
        raise ValueError(
            f"'{type(node).__name__}' is not allowed — only plain math "
            "formulas are supported."
        )

    fn = build(tree)
    if not uses_v:
        raise ValueError("The formula must use the variable v (the source value).")
    return fn


def _build_transform(
    scale_factor: float | None, value_function: str | None
) -> Callable[[float], float] | None:
    """Build the value transform from the user's options (or None for off).

    Raises ValueError on an invalid combination or formula.
    """
    if value_function is not None and not str(value_function).strip():
        value_function = None
    if scale_factor is not None and value_function is not None:
        raise ValueError(
            "Provide either a scaling factor or a custom function, not both."
        )
    if value_function is not None:
        return _compile_value_function(value_function)
    if scale_factor is not None and scale_factor != 1.0:
        factor = float(scale_factor)
        return lambda v: v * factor
    return None


def _scale_state_value(
    value: str | None, transform: Callable[[float], float]
) -> str | None:
    """Apply the value transform to a numeric state string; pass others through.

    Non-numeric states (text sensors, unavailable/unknown) are returned
    unchanged. A transform error or non-finite result raises ValueError so the
    import fails loudly instead of writing corrupted history. %.10g keeps
    enough precision for energy counters while avoiding float-repr noise like
    0.30000000000000004.
    """
    if value is None or value in _NON_GOOD_STATES:
        return value
    try:
        numeric = float(value)
    except (ValueError, TypeError):
        return value
    try:
        result = float(transform(numeric))
    except (ValueError, ZeroDivisionError, OverflowError, TypeError) as exc:
        raise ValueError(
            f"Value adjustment failed for state value {value!r}: {exc}"
        ) from exc
    if not math.isfinite(result):
        raise ValueError(
            f"Value adjustment produced a non-finite result for state value "
            f"{value!r}."
        )
    return f"{result:.10g}"


def _scale_stat_rows(
    rows: list[dict], transform: Callable[[float], float] | None
) -> list[dict]:
    """Return copies of statistics rows with numeric columns transformed
    (mean/min/max/sum/state). Timestamps and last_reset untouched.

    The transform happens BEFORE the sum-offset / splice computations so that
    the offset joining the imported series to the destination is computed in
    the destination's (converted) value space. If the transform is decreasing,
    min/max are re-ordered so min <= max still holds.
    """
    if transform is None:
        return rows
    scaled = []
    for row in rows:
        row2 = dict(row)
        for key in ("mean", "min", "max", "sum", "state"):
            value = row2.get(key)
            if value is None:
                continue
            try:
                result = float(transform(float(value)))
            except (ValueError, ZeroDivisionError, OverflowError, TypeError) as exc:
                raise ValueError(
                    f"Value adjustment failed for statistics value {value}: {exc}"
                ) from exc
            if not math.isfinite(result):
                raise ValueError(
                    f"Value adjustment produced a non-finite result for "
                    f"statistics value {value}."
                )
            row2[key] = result
        if (
            row2.get("min") is not None
            and row2.get("max") is not None
            and row2["min"] > row2["max"]
        ):
            row2["min"], row2["max"] = row2["max"], row2["min"]
        scaled.append(row2)
    return scaled


def _unit_class_for(unit: str | None) -> str | None:
    """The unit class Home Assistant groups a unit under, or None.

    Best effort, and only used to fill in metadata this integration creates:
    the mapping covers HA's primary converters, so a unit belonging to one of
    the secondary ones (ozone, temperature delta and friends) resolves to the
    primary class that shares it. HA derives the real class from the device
    class, which is not available here, and leaves any `unit_class` already in
    metadata alone.
    """
    converter = STATISTIC_UNIT_TO_UNIT_CONVERTER.get(unit)
    return converter.UNIT_CLASS if converter is not None else None


def _format_number(value: float) -> str:
    """A factor as a person would type it: 1000, 0.001, 1.8."""
    return f"{value:.12g}"


def _suggest_unit_adjustment(
    from_unit: str | None,
    to_unit: str | None,
    declared_classes: tuple[str | None, ...] = (),
) -> dict[str, Any] | None:
    """The value adjustment that converts `from_unit` into `to_unit`, or None.

    Only a suggestion for the panel to offer: nothing is applied unless the
    user fills it in, and they can change it. Offered only when both units
    belong to the same Home Assistant converter, and when no `unit_class`
    recorded in either sensor's metadata names a different class (HA's
    unit-keyed map resolves a unit to one class even where it is shared, such
    as temperature and temperature difference, which convert differently).

    Returns {"scale_factor": a} for a plain factor, {"value_function": ...}
    when the conversion also has an offset (temperature), or None.
    """
    try:
        converter = STATISTIC_UNIT_TO_UNIT_CONVERTER.get(from_unit)
        if (
            converter is None
            or STATISTIC_UNIT_TO_UNIT_CONVERTER.get(to_unit) is not converter
        ):
            return None
        valid = getattr(converter, "VALID_UNITS", ())
        if from_unit not in valid or to_unit not in valid:
            return None
        if any(c is not None and c != converter.UNIT_CLASS for c in declared_classes):
            return None
        offset = float(converter.convert(0.0, from_unit, to_unit))
        factor = float(converter.convert(1.0, from_unit, to_unit)) - offset
        check = float(converter.convert(1000.0, from_unit, to_unit))
    except Exception:
        return None
    if not all(math.isfinite(x) for x in (offset, factor, check)) or factor <= 0:
        return None
    # Only a straight line can be expressed as an adjustment.
    if abs(check - (factor * 1000.0 + offset)) > 1e-9 * max(1.0, abs(check)):
        return None
    if abs(offset) < 1e-12:
        if abs(factor - 1.0) < 1e-12:
            return None
        return {"scale_factor": float(_format_number(factor))}
    sign = "+" if offset > 0 else "-"
    scaled = "v" if abs(factor - 1.0) < 1e-12 else f"v * {_format_number(factor)}"
    return {"value_function": f"{scaled} {sign} {_format_number(abs(offset))}"}


def _ensure_unit_class(metadata: dict[str, Any]) -> None:
    """Populate ``unit_class`` on import metadata when it is absent.

    Home Assistant 2026.11 makes ``unit_class`` mandatory for
    ``async_import_statistics``; omitting it currently emits a deprecation
    warning. We derive it from the unit of measurement using the same converter
    mapping HA applies internally, falling back to ``None`` for units with no
    associated converter (matching HA's own behaviour).
    """
    if "unit_class" in metadata:
        return
    metadata["unit_class"] = _unit_class_for(metadata.get("unit_of_measurement"))


def _resolve_target_unit(
    hass: HomeAssistant,
    dest_id: str,
    dest_metadata_map: dict | None,
    source_unit: str | None,
    allow_source_fallback: bool,
) -> str | None:
    """The unit imported statistics are written in.

    Statistics are STORED in the unit recorded in `statistics_meta`, which is
    fixed for the life of a statistic: `sensor/recorder.py` pins it ("We have
    seen this sensor before, use the unit from metadata") and converts
    incoming states into it. The unit on the entity is only a DISPLAY unit,
    which a user can change in the entity settings, and reads convert storage
    to display on the way out.

    So the destination's stored unit is the answer whenever it has one. A
    destination with no statistics yet is about to have metadata created, and
    that takes its current display unit, or the source's unit when the
    destination has no state either (a disabled or not-yet-loaded entity).
    That last fallback is skipped when a value adjustment is in force, since
    the adjusted values are no longer in the source's unit.
    """
    entry = dest_metadata_map.get(dest_id) if dest_metadata_map else None
    if entry:
        return entry[1].get("unit_of_measurement")

    state = hass.states.get(dest_id)
    if state:
        return state.attributes.get("unit_of_measurement")

    return source_unit if allow_source_fallback else None


def _is_utility_meter(hass: HomeAssistant, entity_id: str) -> bool:
    """Whether the entity belongs to the Utility Meter integration.

    Display only. A Utility Meter keeps its running value inside the helper,
    so its state carries on from there whatever history is imported, and the
    panel explains the resulting drop in the History graph. Nothing else reads
    this, and any failure here only means the note is not shown.
    """
    try:
        from homeassistant.helpers import entity_registry as er

        entry = er.async_get(hass).async_get(entity_id)
        return entry is not None and entry.platform == "utility_meter"
    except Exception:
        return False


def _hash_panel_file(panel_path: str) -> str:
    """Compute a short cache-busting hash of the panel.js file."""
    with open(panel_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()[:8]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Merge Sensor History from a config entry."""
    domain_data = hass.data.setdefault(DOMAIN, {})
    domain_data.setdefault("_locks", {})
    # Cancel events for imports currently writing to the database. Without
    # these there is no way to reach a running import at all: unloading or
    # reloading a config entry does not stop work already handed to the
    # recorder, so an import that outlived the user's patience could only be
    # stopped by restarting Home Assistant. They are set on unload and on
    # shutdown, and the writer checks between batches.
    cancel_events: set[threading.Event] = domain_data.setdefault(
        "_cancel_events", set()
    )

    if not domain_data.get("_stop_listener_registered"):

        @callback
        def _cancel_on_stop(_event: Any) -> None:
            """Stop any running import so shutdown is not held up by it."""
            for cancel_event in list(cancel_events):
                cancel_event.set()

        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _cancel_on_stop)
        domain_data["_stop_listener_registered"] = True

    # Register the static asset path + sidebar panel. If either step fails we
    # let the exception propagate so the config entry fails to set up — HA shows
    # "Failed to set up" and logs the full traceback, prompting the user to
    # report it — rather than loading in a degraded, panel-less state.
    frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
    panel_path = os.path.join(frontend_dir, "panel.js")

    # Serve the whole frontend directory (HA's documented pattern) so panel.js
    # is reachable at /<DOMAIN>/panel.js. Static paths can't be unregistered, so
    # register at most once per process to avoid stacking a duplicate route on a
    # config-entry reload.
    if not domain_data.get("_static_path_registered"):
        await hass.http.async_register_static_paths(
            [StaticPathConfig(f"/{DOMAIN}", frontend_dir, cache_headers=True)]
        )
        domain_data["_static_path_registered"] = True

    # Cache-busting hash so browsers reload panel.js after an update.
    # File I/O runs in an executor — HA flags a sync open() in the event loop
    # as a blocking call.
    panel_hash = await hass.async_add_executor_job(_hash_panel_file, panel_path)

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
    domain_data["_panel_registered"] = True

    # Register websocket commands
    websocket_api.async_register_command(hass, ws_import_history)
    websocket_api.async_register_command(hass, ws_get_status)
    websocket_api.async_register_command(hass, ws_repair_sum_series)

    # Register service
    async def handle_import_history(call: ServiceCall) -> None:
        source = call.data["source_entity_id"]
        dest = call.data["destination_entity_id"]
        fill_gaps = bool(call.data.get("fill_gaps", False))
        gap_threshold_minutes = int(call.data.get("gap_threshold_minutes", 60))
        scale_factor = call.data.get("scale_factor")
        if scale_factor is not None and scale_factor == 1.0:
            scale_factor = None
        value_function = call.data.get("value_function")
        overwrite = bool(call.data.get("overwrite", False))
        result = await _async_import_pair(
            hass,
            source,
            dest,
            fill_gaps=fill_gaps,
            gap_threshold_minutes=gap_threshold_minutes,
            scale_factor=scale_factor,
            value_function=value_function,
            overwrite=overwrite,
        )
        if result["error"]:
            _LOGGER.error(
                "Import from %s to %s failed: %s", source, dest, result["error"]
            )
        else:
            _LOGGER.info(
                "Import from %s to %s complete: %d states, %d stats imported "
                "(overwrite=%s, %d destination state rows replaced)",
                source,
                dest,
                result["states_imported"],
                result["stats_imported"],
                overwrite,
                result.get("states_overwritten", 0),
            )

    hass.services.async_register(
        DOMAIN,
        "import_history",
        handle_import_history,
        schema=vol.Schema(
            {
                vol.Required("source_entity_id"): cv.entity_id,
                vol.Required("destination_entity_id"): cv.entity_id,
                vol.Optional("fill_gaps", default=False): cv.boolean,
                vol.Optional("overwrite", default=False): cv.boolean,
                vol.Optional("gap_threshold_minutes", default=60): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=1440)
                ),
                vol.Optional("scale_factor"): vol.All(
                    vol.Coerce(float), vol.Range(min=1e-12)
                ),
                vol.Optional("value_function"): vol.All(
                    cv.string, vol.Length(min=1, max=_VALUE_FUNCTION_MAX_LEN)
                ),
            }
        ),
    )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    domain_data = hass.data.get(DOMAIN, {})

    # Mirror setup: remove the panel we registered (and clear its flag so the
    # next setup re-registers it).
    if domain_data.pop("_panel_registered", None):
        async_remove_panel(hass, "merge-sensor-history")

    hass.services.async_remove(DOMAIN, "import_history")

    # Stop any import still writing. The writer checks between chunks, keeps
    # what it has already committed and returns; without this, unloading or
    # reloading the integration left the import running to completion.
    for cancel_event in list(domain_data.get("_cancel_events", ())):
        cancel_event.set()

    # Intentionally keep hass.data[DOMAIN]: the static asset path registered in
    # async_setup_entry cannot be unregistered (no HA/aiohttp API), so it lives
    # for the process lifetime. Preserving the `_static_path_registered` guard
    # stops a later reload from stacking a duplicate route.
    return True


# ---------------------------------------------------------------------------
# WebSocket API
# ---------------------------------------------------------------------------


@websocket_api.require_admin
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
        vol.Optional("fill_gaps", default=False): bool,
        vol.Optional("gap_threshold_minutes", default=60): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=1440)
        ),
        vol.Optional("dry_run", default=False): bool,
        vol.Optional("overwrite", default=False): bool,
        vol.Optional("scale_factor", default=None): vol.Any(
            None, vol.All(vol.Coerce(float), vol.Range(min=1e-12))
        ),
        vol.Optional("value_function", default=None): vol.Any(
            None,
            vol.All(cv.string, vol.Length(min=1, max=_VALUE_FUNCTION_MAX_LEN)),
        ),
    }
)
@websocket_api.async_response
async def ws_import_history(
    hass: HomeAssistant, connection: websocket_api.ActiveConnection, msg: dict
) -> None:
    """Handle import request from the panel."""
    pairs = msg["pairs"]
    fill_gaps = bool(msg.get("fill_gaps", False))
    gap_threshold_minutes = int(msg.get("gap_threshold_minutes", 60))
    dry_run = bool(msg.get("dry_run", False))
    overwrite = bool(msg.get("overwrite", False))
    scale_factor = msg.get("scale_factor")
    value_function = msg.get("value_function")
    # A factor of exactly 1 is a no-op — treat it as disabled.
    if scale_factor is not None and scale_factor == 1.0:
        scale_factor = None
    results = []

    for pair in pairs:
        result = await _async_import_pair(
            hass,
            pair["source"],
            pair["destination"],
            fill_gaps=fill_gaps,
            gap_threshold_minutes=gap_threshold_minutes,
            dry_run=dry_run,
            scale_factor=scale_factor,
            value_function=value_function,
            overwrite=overwrite,
        )
        results.append(
            {
                "source": pair["source"],
                "destination": pair["destination"],
                "dry_run": dry_run,
                "fill_gaps": fill_gaps,
                **result,
            }
        )

    connection.send_result(msg["id"], {"results": results})


@websocket_api.require_admin
@websocket_api.websocket_command(
    {
        vol.Required("type"): "merge_sensor_history/repair_sum_series",
        vol.Required("statistic_id"): cv.entity_id,
    }
)
@websocket_api.async_response
async def ws_repair_sum_series(
    hass: HomeAssistant, connection: websocket_api.ActiveConnection, msg: dict
) -> None:
    """Put a restarted cumulative series back on top of its own history.

    Offered, never automatic: an import can only stop this happening from now
    on, so repairing a destination that an earlier version already detached is
    the user's call. The cliff is re-detected here from the live database
    rather than trusted from the client, so the caller cannot choose where or
    by how much the series moves.
    """
    statistic_id = msg["statistic_id"]
    recorder = get_instance(hass)

    try:
        outcome = await recorder.async_add_executor_job(
            _repair_sum_series, recorder, statistic_id
        )
    except Exception as exc:
        _LOGGER.error(
            "Could not repair the running total for %s: %s", statistic_id, exc
        )
        connection.send_result(
            msg["id"], {"repaired": False, "reason": f"Repair failed: {exc}"}
        )
        return

    if not outcome["repaired"]:
        connection.send_result(msg["id"], outcome)
        return

    start_dt = datetime.fromtimestamp(outcome["start_ts"], tz=timezone.utc)
    _LOGGER.warning(
        "Repaired %s: lifted every statistics row from %s onwards by %s so the "
        "series continues from its own history instead of restarting at zero",
        statistic_id,
        start_dt.isoformat(),
        outcome["lift"],
    )
    connection.send_result(
        msg["id"],
        {
            "repaired": True,
            "lift": outcome["lift"],
            "start": start_dt.isoformat(),
            "unit": outcome["unit"],
        },
    )


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
    hass: HomeAssistant,
    source_id: str,
    dest_id: str,
    *,
    fill_gaps: bool = False,
    gap_threshold_minutes: int = 60,
    dry_run: bool = False,
    scale_factor: float | None = None,
    value_function: str | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Import all history from source entity into destination entity.

    This function is IDEMPOTENT:
    - It only imports source states strictly older than the destination's
      oldest GOOD entry — hidden unavailable/unknown rows don't count as
      coverage — (unless `fill_gaps` is set, which also fills mid-stream
      and trailing gaps in the destination's state history).
    - The state insertion commits in paced chunks rather than one long
      transaction, so it never locks the recorder out of the database. Chunks
      are ordered so an interrupted run is resumable.
    - Re-running after success: destination now has older data, so the cutoff
      moves earlier and nothing new qualifies. Zero states imported.
    - Re-running after failure or cancellation: committed chunks are kept, the
      remaining states still qualify, and the re-run completes the import.

    When `fill_gaps` is True, also:
    - Imports source states falling inside any gap in the destination's
      existing state history where the gap width is >= `gap_threshold_minutes`.
    - Imports source states newer than the destination's newest state if
      (now - dest_newest) >= `gap_threshold_minutes`.
    - Backfills short-term statistics for hours/5-min slots where the
      destination has no short-term stats row.

    When `dry_run` is True:
    - Calculates all changes but does not write to the database.

    When `overwrite` is True (DESTRUCTIVE, opt-in):
    - Destination states inside the source's time span are deleted and
      replaced by the source's states, regardless of `fill_gaps`.
    - Statistics (hourly and 5-minute) take the source's values wherever the
      source has data, instead of only filling holes. Columns the source does
      not provide keep the destination's existing values, and the recent
      slots HA may still be compiling are still skipped.
    - Intended for a destination holding known-bad values (zeros logged during
      commissioning, or an earlier import made with the wrong unit). Deleted
      state rows are not recoverable without a database backup.

    When `scale_factor` is set (a positive float), every numeric value read
    from the source (state strings and statistics mean/min/max/sum/state) is
    multiplied by it before being considered for import — for merging sensors
    that record the same quantity in different units (e.g. 1000 for kWh -> Wh,
    0.001 for Wh -> kWh). `value_function` is the general form: a restricted
    math formula of `v` (see _compile_value_function) for conversions a plain
    factor can't express, e.g. "v * 9/5 + 32" for °C -> °F. The two are
    mutually exclusive. Either way the conversion happens before the
    cumulative-sum splice offset is computed, so energy series join correctly
    in the destination's value space.

    Returns a dict with result details for the UI.
    """
    result: dict[str, Any] = {
        "scale_factor": scale_factor,  # echoed for display (None = off)
        "value_function": value_function,  # echoed for display (None = off)
        "overwrite": overwrite,  # echoed for display
        # States
        "states_source_total": 0,
        "states_overwritten": 0,  # dest rows deleted and replaced (overwrite)
        "states_overwrite_start": None,  # ISO start of the replaced window
        "states_overwrite_end": None,  # ISO end of the replaced window
        "states_source_missing": False,  # True: source had no raw states (stats-only)
        "states_source_skipped_non_good": 0,  # unavailable/unknown source rows
        "states_imported": 0,
        "states_already_covered": 0,
        "states_mid_stream_filled": 0,  # source states imported inside dest-range gaps
        "states_trailing_filled": 0,  # source states imported after dest's newest
        "states_dest_total_rows": 0,  # diagnostic: rows in dest before this import
        "states_dest_good_rows": 0,  # diagnostic: rows used for gap detection
        "states_gap_intervals_count": 0,  # diagnostic: # of qualifying gaps detected
        "states_imported_start": None,  # ISO datetime of first imported state
        "states_imported_end": None,  # ISO datetime of last imported state
        # Long-term statistics (hourly)
        "stats_source_total": 0,
        "stats_imported": 0,
        "stats_already_covered": 0,
        "stats_skipped_recent": 0,
        "stats_gap_filled": 0,
        "stats_overwritten": 0,  # dest rows whose values were replaced
        "stats_imported_start": None,  # ISO datetime (hour start) of first imported stat
        "stats_imported_end": None,  # ISO datetime (hour start) of last imported stat
        "stats_sum_offset": None,  # Applied splice offset (or None) — set only when NOT realigned
        "stats_realigned_by": None,  # Amount the dest running total was lifted (or None)
        "stats_sum_seeded": None,  # Running total seeded for the destination's future rows
        "stats_detached": None,  # Detected restart-from-zero cliff, repairable on request
        "stats_unit_mismatch": None,  # Set when source and destination units differ
        "stats_unit": None,  # Unit of measurement for display
        "dest_is_utility_meter": False,  # Display only: explains the History drop
        # Short-term statistics (5-minute) — populated only when fill_gaps=True
        "stats_short_source_total": 0,
        "stats_short_imported": 0,
        "stats_short_already_covered": 0,
        "stats_short_skipped_recent": 0,
        "stats_short_overwritten": 0,
        "stats_short_imported_start": None,
        "stats_short_imported_end": None,
        # Per-row debug records, for client-side download as JSON.
        # Each is a list of dicts; populated even when nothing was imported.
        "debug_states": [],
        "debug_stats": [],
        "debug_stats_short": [],
        "error": None,
    }

    # --- Validate inputs ---
    if source_id == dest_id:
        result["error"] = "Source and destination cannot be the same entity."
        return result

    # Build the value transform (validates the custom function's restricted
    # math grammar — see _compile_value_function; never executed as code).
    try:
        transform = _build_transform(scale_factor, value_function)
    except ValueError as exc:
        result["error"] = str(exc)
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

    cancel_events: set[threading.Event] = hass.data[DOMAIN]["_cancel_events"]
    cancel_event = threading.Event()

    async with locks[dest_id]:
        cancel_events.add(cancel_event)
        try:
            await _do_import(
                hass,
                source_id,
                dest_id,
                result,
                fill_gaps=fill_gaps,
                gap_threshold_minutes=gap_threshold_minutes,
                dry_run=dry_run,
                transform=transform,
                overwrite=overwrite,
                cancel_event=cancel_event,
            )
        except Exception as exc:
            _LOGGER.exception(
                "Error importing history from %s to %s (dry_run=%s)",
                source_id,
                dest_id,
                dry_run,
            )
            result["error"] = f"Import failed: {exc}"
        finally:
            cancel_events.discard(cancel_event)

    return result


async def _do_import(
    hass: HomeAssistant,
    source_id: str,
    dest_id: str,
    result: dict[str, Any],
    *,
    fill_gaps: bool = False,
    gap_threshold_minutes: int = 60,
    dry_run: bool = False,
    transform: Callable[[float], float] | None = None,
    overwrite: bool = False,
    cancel_event: threading.Event | None = None,
) -> None:
    """Execute the actual import. Separated for clean lock/error handling."""
    recorder = get_instance(hass)
    result["dest_is_utility_meter"] = _is_utility_meter(hass, dest_id)

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
    have_states = bool(source_states)
    result["states_source_total"] = len(source_states)
    result["states_source_missing"] = not have_states

    if not have_states:
        # A deleted entity typically has no raw states left (purged after
        # ~10 days) but its long-term statistics persist orphaned, keyed by the
        # old statistic_id. So don't bail here — skip the states import and let
        # the statistics path (which queries purely by statistic_id) run. If it
        # turns out there are no statistics either, the "no history" error is
        # set at the end of this function.
        _LOGGER.info(
            "Source %s has no raw states — attempting statistics-only import "
            "(e.g. a deleted entity whose states were purged)",
            source_id,
        )
    else:
        _LOGGER.info(
            "Read %d states from source entity %s (oldest: %s, newest: %s)",
            len(source_states),
            source_id,
            source_states[0].last_updated.isoformat(),
            source_states[-1].last_updated.isoformat(),
        )

        # --- 2. Insert states in a single transaction ---
        # All-or-nothing, at any size. It runs on the recorder's own thread so
        # it shares the recorder's connection and never competes with it for
        # SQLite's write lock. The destination's cutoff is read inside the same
        # transaction as the insert, so there is no TOCTOU race, and in
        # overwrite mode the rows being replaced are deleted in it too.
        states_out = await _async_write_states(
            recorder,
            cancel_event,
            dest_entity_id=dest_id,
            source_states=source_states,
            fill_gaps=fill_gaps,
            gap_threshold_minutes=gap_threshold_minutes,
            dry_run=dry_run,
            transform=transform,
            overwrite=overwrite,
        )
        result["states_imported"] = states_out["inserted"]
        result["states_already_covered"] = states_out["already_covered"]
        result["states_mid_stream_filled"] = states_out["mid_stream_filled"]
        result["states_trailing_filled"] = states_out["trailing_filled"]
        result["states_source_skipped_non_good"] = states_out[
            "source_skipped_non_good"
        ]
        result["states_dest_total_rows"] = states_out["dest_total_rows"]
        result["states_dest_good_rows"] = states_out["dest_good_rows"]
        result["states_gap_intervals_count"] = states_out["gap_intervals_count"]
        result["states_overwritten"] = states_out["overwritten"]
        span = states_out.get("overwrite_span")
        if span:
            result["states_overwrite_start"] = datetime.fromtimestamp(
                span[0], tz=timezone.utc
            ).isoformat()
            result["states_overwrite_end"] = datetime.fromtimestamp(
                span[1], tz=timezone.utc
            ).isoformat()
        result["debug_states"] = states_out["debug_records"]
        imported_min_ts = states_out["imported_min_ts"]
        if imported_min_ts is not None:
            result["states_imported_start"] = datetime.fromtimestamp(
                imported_min_ts, tz=timezone.utc
            ).isoformat()
            result["states_imported_end"] = datetime.fromtimestamp(
                states_out["imported_max_ts"], tz=timezone.utc
            ).isoformat()

        if states_out.get("cancelled"):
            # Home Assistant is shutting down, or the integration was
            # reloaded or disabled mid-import. The transaction rolled back, so
            # the database is untouched and the statistics steps are skipped.
            result["error"] = (
                f"Import into {dest_id} was cancelled before it finished, so "
                "nothing was written and the database is unchanged. Run the "
                "same import again when you are ready."
            )
            return

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
            hass,
            source_id,
            dest_id,
            dry_run=dry_run,
            transform=transform,
            overwrite=overwrite,
        )
        result.update(stats_result)
    except Exception as exc:
        _LOGGER.warning(
            "Statistics import failed for %s -> %s: %s",
            source_id,
            dest_id,
            exc,
        )
        result["stats_error"] = str(exc)

    # --- 4. Optionally backfill short-term statistics (5-min) ---
    # Short-term stats are what HA's graphs render for recent data (< ~10 days).
    # We only do this when the user opts in via fill_gaps, because short-term
    # cells are dense (12/hour) and the 5-min compile cycle makes the race
    # window tighter than for hourly LTS.
    # Overwrite also runs it: replacing known-bad recent data is exactly the
    # case it exists for, and requiring a second unrelated checkbox to reach
    # the 5-minute rows would leave the recent graph half-corrected.
    if fill_gaps or overwrite:
        try:
            short_result = await _async_import_short_term_statistics_for_pair(
                hass,
                source_id,
                dest_id,
                gap_threshold_minutes=gap_threshold_minutes,
                dry_run=dry_run,
                transform=transform,
                overwrite=overwrite,
            )
            result.update(short_result)
        except Exception as exc:
            _LOGGER.warning(
                "Short-term statistics backfill failed for %s -> %s: %s",
                source_id,
                dest_id,
                exc,
            )
            result["stats_short_error"] = str(exc)

    # --- 4b. Seed the destination's running total when it has no chain yet ---
    # Planned by the statistics step (see the note there). Queued here so it
    # lands after the imported rows, and skipped when the short-term backfill
    # already wrote recent rows for the destination — the newest of those is
    # then the seed HA reads.
    seed = result.pop("_sum_seed", None)
    if seed and result.get("stats_short_imported", 0):
        seed = None
        result["stats_sum_seeded"] = None
    if seed:
        if dry_run:
            _LOGGER.info(
                "Dry run: would seed %s's running total at %s so its future "
                "statistics continue from the imported history",
                dest_id,
                seed["sum"],
            )
        else:
            try:
                # Re-check against a fresh reading. The plan was made before
                # the statistics steps ran, and those can take a while on a
                # long history; HA compiles a 5-minute row every 5 minutes, so
                # the destination may have grown a chain of its own since. If
                # it has, that chain is what HA seeds from and writing an older
                # row would have no effect.
                anchor_now = await recorder.async_add_executor_job(
                    partial(
                        _fetch_dest_sum_anchor,
                        recorder,
                        dest_id,
                        want_earliest_sum=False,
                    )
                )
                if anchor_now["has_rows"]:
                    _LOGGER.info(
                        "Skipped seeding %s's running total: it compiled "
                        "statistics of its own while this import ran, and "
                        "Home Assistant continues from those",
                        dest_id,
                    )
                    result["stats_sum_seeded"] = None
                else:
                    # The most recent 5-minute slot HA has certainly finished
                    # compiling, taken now rather than when the plan was made,
                    # so nothing collides with an in-flight compile.
                    seed_now = datetime.now(timezone.utc)
                    seed_start = seed_now.replace(
                        minute=(seed_now.minute // 5) * 5,
                        second=0,
                        microsecond=0,
                    ) - timedelta(minutes=10)
                    recorder.async_import_statistics(
                        seed["metadata"],
                        [StatisticData(start=seed_start, sum=seed["sum"])],
                        StatisticsShortTerm,
                    )
                    _LOGGER.info(
                        "Seeded %s's running total at %s (5-minute slot %s) so "
                        "the statistics Home Assistant compiles from now on "
                        "continue from the imported history instead of "
                        "restarting at zero",
                        dest_id,
                        seed["sum"],
                        seed_start.isoformat(),
                    )
            except Exception as exc:
                _LOGGER.warning(
                    "Could not seed the running total for %s: %s", dest_id, exc
                )
                result["stats_seed_error"] = str(exc)
                result["stats_sum_seeded"] = None

    # --- 5. Realign the destination series for a clean head-fill energy import ---
    # See the "series realignment" note in _async_import_statistics_for_pair.
    # Queued last so it runs on the recorder thread AFTER the import tasks commit,
    # lifting every row (imported + existing + future) by the same constant.
    #
    # This runs whatever the short-term step did. The lift starts at the oldest
    # imported hour, so every row in both tables moves by the same amount and no
    # two rows change relative to each other, gap-filled 5-minute slots included.
    # Their level is right for the same reason: the realignment is only ever
    # planned for a pure head-fill, where the destination's earliest hourly sum
    # row and its earliest 5-minute sum row are the same instant, so both paths
    # spliced against the same point with the same offset. Skipping the lift
    # here used to leave the destination shifted down to meet the source, which
    # made the lifetime total wrong for exactly the people re-running with
    # Overwrite or Fill gaps after a bad import.
    realign = result.pop("_realign", None)
    if realign:
        original_offset = -float(realign["adjustment"])
        if dry_run:
            # Preview: stats_realigned_by is already set for display ("would be
            # realigned"); skip the actual adjustment queueing.
            _LOGGER.info(
                "Dry run: would realign %s by lifting cumulative sum by %s",
                dest_id,
                realign["adjustment"],
            )
        else:
            try:
                start_dt = datetime.fromtimestamp(
                    realign["start_ts"], tz=timezone.utc
                )
                recorder.async_adjust_statistics(
                    dest_id,
                    start_dt,
                    float(realign["adjustment"]),
                    realign["unit"],
                )
                _LOGGER.info(
                    "Realigned %s: lifted cumulative sum by %s from %s so the "
                    "imported history joins the existing series with a correct "
                    "first hour",
                    dest_id,
                    realign["adjustment"],
                    start_dt.isoformat(),
                )
            except Exception as exc:
                _LOGGER.warning(
                    "Sum realignment failed for %s (splice offset remains "
                    "applied): %s",
                    dest_id,
                    exc,
                )
                result["stats_realign_error"] = str(exc)
                result["stats_realigned_by"] = None
                result["stats_sum_offset"] = original_offset

    # --- 6. If the source had neither states nor statistics, it is a bad or
    # fully-purged id. Report the familiar "no history" error (unless a stats
    # error already explains the empty result). ---
    if (
        not have_states
        and result.get("stats_source_total", 0) == 0
        and result.get("stats_short_source_total", 0) == 0
        and not result.get("stats_error")
    ):
        result["error"] = (
            f"No history found for source entity '{source_id}'. Its states may "
            f"have been purged (default: 10 days) and it has no long-term "
            f"statistics, or the entity ID is wrong."
        )


async def _async_write_states(
    recorder: Any, cancel_event: threading.Event, **kwargs: Any
) -> dict[str, Any]:
    """Run the state write and wait for it, preferring the recorder's thread.

    Going through the recorder's own task queue means the write shares the
    recorder's connection, so it cannot compete with the recorder for SQLite's
    single write lock however long it takes. That is what lets the import stay
    in one transaction at any size. Home Assistant's own bulk statistics
    import uses the same queue.

    If that queue is not available (a Home Assistant version that moved
    `RecorderTask`), the write falls back to a database worker thread. It is
    still one transaction, but it then does compete for the lock, so the
    connection is told to wait rather than fail.
    """
    if RecorderTask is None:  # pragma: no cover - depends on the HA version
        _LOGGER.debug(
            "Recorder task queue unavailable; writing states from a worker "
            "thread instead"
        )
        return await recorder.async_add_executor_job(
            partial(
                _insert_states,
                recorder,
                set_busy_timeout=True,
                cancel_event=cancel_event,
                **kwargs,
            )
        )

    future: concurrent.futures.Future = concurrent.futures.Future()
    recorder.queue_task(
        _ImportStatesTask(future, cancel_event, kwargs)
    )
    return await asyncio.wrap_future(future)


class _ImportStatesTask(RecorderTask if RecorderTask is not None else object):  # type: ignore[misc]
    """Write the states on the recorder's own thread.

    `commit_before` (inherited, True) makes the recorder flush its pending
    events before this runs, so the connection this shares with it starts
    clean.
    """

    # Not a dataclass: HA's RecorderTask subclasses are, but slots plus a
    # plain __init__ keeps this working whichever way the base class is
    # declared in the installed version.
    __slots__ = ("_future", "_cancel_event", "_kwargs")

    def __init__(
        self,
        future: concurrent.futures.Future,
        cancel_event: threading.Event,
        kwargs: dict[str, Any],
    ) -> None:
        self._future = future
        self._cancel_event = cancel_event
        self._kwargs = kwargs

    def run(self, instance: Any) -> None:
        """Do the import, handing the result (or the error) back to the caller."""
        if self._future.set_running_or_notify_cancel() is False:
            return
        try:
            self._future.set_result(
                _insert_states(
                    instance,
                    set_busy_timeout=False,
                    cancel_event=self._cancel_event,
                    **self._kwargs,
                )
            )
        except BaseException as exc:  # noqa: BLE001 - relayed to the caller
            # Never let this escape into the recorder's task loop, which would
            # make it tear down and reopen its event session.
            self._future.set_exception(exc)


def _insert_states(
    recorder_instance: Any,
    dest_entity_id: str,
    source_states: list,
    *,
    fill_gaps: bool = False,
    gap_threshold_minutes: int = 60,
    dry_run: bool = False,
    transform: Callable[[float], float] | None = None,
    overwrite: bool = False,
    cancel_event: threading.Event | None = None,
    set_busy_timeout: bool = False,
) -> dict[str, Any]:
    """Insert State objects into the recorder database for a destination entity.

    **Atomic.** Everything is written in one transaction: either every state
    (and, in overwrite mode, every deletion) lands, or none of it does and the
    database is exactly as it was. There is no size at which this degrades.

    Normally called on the recorder's own thread, through `_ImportStatesTask`,
    so it shares the recorder's connection and cannot contend with it for
    SQLite's single write lock however long it runs. See the write-strategy
    note near the top of this module.

    **Overwrite mode (`overwrite=True`) is destructive.** Every destination row
    inside the source's time span is DELETED and replaced by the source's
    states, regardless of `fill_gaps`. Rows outside that span are untouched.
    This is the only mode that removes existing data; it exists for
    destinations holding known-bad values (a sensor that logged zeros while it
    was being commissioned, or an earlier import made with the wrong unit).
    Deleted rows cannot be recovered without a database backup.

    Import rules (when `overwrite` is False):
    - **Head fill (always):** any source state strictly older than the
      destination's oldest GOOD entry is imported. Hidden rows (unavailable/
      unknown) do not count as coverage — a destination whose earliest rows
      are just unavailable markers has no visible history there, so source
      states in that region are still head-filled (minus exact-timestamp
      duplicates).
    - **Mid-stream fill (when `fill_gaps` is True):** for each pair of adjacent
      destination state timestamps whose delta is >= `gap_threshold_minutes`,
      import all source states strictly between them.
    - **Trailing fill (when `fill_gaps` is True):** if (now - destination_max_ts)
      is >= `gap_threshold_minutes`, import all source states strictly newer
      than the destination's newest entry.

    Deduplication: source states whose `last_updated` timestamp exactly matches
    an existing destination timestamp are skipped (prevents introducing
    same-timestamp twins). This always applies in the head region (where the
    destination can only hold hidden rows) and, with `fill_gaps`, across the
    destination's whole range.

    Idempotency: head-fill moves the cutoff earlier on re-run; gap-fill modes
    close the gaps they fill, so a re-run sees the same (or no remaining) gaps.
    Overwrite re-runs are also stable: the second run deletes the rows the
    first one wrote and rewrites identical values.

    Cancellation: `cancel_event` is checked between INSERT batches. Because
    nothing is committed until the end, a cancelled import rolls back and
    leaves the database untouched, returning with `cancelled` True.

    Returns a dict of counters plus `imported_min_ts` / `imported_max_ts`
    (None if nothing was imported) and `debug_records`, one entry per source
    state with its decision and the adjacent destination context.
    """
    inserted = 0
    imported_min_ts: float | None = None
    imported_max_ts: float | None = None
    mid_stream_filled = 0
    trailing_filled = 0
    source_skipped_non_good = 0
    dest_total_rows = 0
    dest_good_rows = 0
    gap_intervals_count = 0
    overwritten = 0
    overwrite_span: tuple[float, float] | None = None
    cancelled = False
    debug_records: list[dict] = []
    session = recorder_instance.get_session()

    try:
        if set_busy_timeout:
            # Only on the fallback path, where this is not the recorder's own
            # connection and so must not be left with an altered pragma... it
            # is a worker connection, and waiting beats failing fast.
            _set_sqlite_busy_timeout(session)

        # ============== PHASE 1: plan the import ==============
        # Nothing below writes. It shares a transaction with the write phase,
        # so the destination's cutoff is read and acted on atomically: no
        # TOCTOU race.

        # -- Look up StatesMeta for the destination entity --
        # Creating it when missing is deferred to the write phase.
        metadata_id = (
            session.query(StatesMeta.metadata_id)
            .filter(StatesMeta.entity_id == dest_entity_id)
            .scalar()
        )
        if metadata_id is None:
            # An entity with no metadata row can have no state rows, so the
            # classification below reads -1 as "destination has no history".
            metadata_id = -1

        # -- Query the destination's oldest GOOD timestamp --
        # Non-good rows (unavailable/unknown) are excluded from the cutoff:
        # HA hides them in the History panel, so a destination whose earliest
        # rows are just unavailable markers (e.g. a ghost/restored entity that
        # logged a row at every restart) has no VISIBLE history there — source
        # states in that region must still be head-fillable. Destination rows
        # sitting before the cutoff (all non-good by construction) are loaded
        # for same-timestamp dedup instead, which also keeps head fill
        # idempotent when the imported head states are themselves non-good.
        head_dedup_ts: set[float] = set()
        if metadata_id == -1 or overwrite:
            # Overwrite replaces the whole source span, so neither the cutoff
            # nor the head-dedup set is consulted.
            min_ts = None
        else:
            min_ts = (
                session.query(sql_func.min(States.last_updated_ts))
                .filter(
                    States.metadata_id == metadata_id,
                    States.state.isnot(None),
                    States.state.notin_(list(_NON_GOOD_STATES)),
                )
                .scalar()
            )
            head_dedup_q = session.query(States.last_updated_ts).filter(
                States.metadata_id == metadata_id
            )
            if min_ts is not None:
                head_dedup_q = head_dedup_q.filter(
                    States.last_updated_ts < min_ts
                )
            head_dedup_ts = {row[0] for row in head_dedup_q.all()}

        # -- Decide which source states to import --
        # Single classification pass: each source state is either head /
        # mid_stream / trailing / various skip reasons. The same pass produces
        # the per-row debug_records list returned to the UI.
        if overwrite:
            # DESTRUCTIVE path: delete every destination row inside the
            # source's span, then import the full source series. The delete
            # happens window by window in the write phase, each window in the
            # same transaction as the inserts that replace it, so no committed
            # window is ever left deleted-but-not-replaced.
            to_import = list(source_states)
            # The span is the source's own data range, so the blast radius is
            # exactly what the source can replace. It is reported back to the
            # UI so the user sees the window before (and after) committing.
            span_lo = min(s.last_updated.timestamp() for s in source_states)
            span_hi = max(s.last_updated.timestamp() for s in source_states)
            overwrite_span = (span_lo, span_hi)

            if metadata_id != -1:
                overwritten = (
                    session.query(sql_func.count(States.state_id))
                    .filter(
                        States.metadata_id == metadata_id,
                        States.last_updated_ts >= span_lo,
                        States.last_updated_ts <= span_hi,
                    )
                    .scalar()
                    or 0
                )

            for s in source_states:
                src_val = str(s.state) if s.state is not None else None
                rec = {
                    "ts": s.last_updated.isoformat(),
                    "ts_epoch": s.last_updated.timestamp(),
                    "source_value": src_val,
                    "dest_has_row_at_same_ts": None,
                    "prev_dest_good_ts": None,
                    "next_dest_good_ts": None,
                    "gap_minutes": None,
                    "decision": "overwrite_imported",
                    "reason": (
                        "Overwrite mode: destination rows in the source's time "
                        "span were removed and replaced by the source series."
                    ),
                }
                if transform is not None:
                    rec["scaled_value"] = _scale_state_value(src_val, transform)
                debug_records.append(rec)

            _LOGGER.warning(
                "OVERWRITE %s: %s %d destination state rows between %s and %s, "
                "%s %d source states (dry_run=%s)",
                dest_entity_id,
                "would delete" if dry_run else "deleted",
                overwritten,
                datetime.fromtimestamp(span_lo, tz=timezone.utc).isoformat(),
                datetime.fromtimestamp(span_hi, tz=timezone.utc).isoformat(),
                "would import" if dry_run else "importing",
                len(to_import),
                dry_run,
            )
        elif min_ts is None and not head_dedup_ts:
            to_import = list(source_states)
            for s in source_states:
                src_val = str(s.state) if s.state is not None else None
                rec = {
                    "ts": s.last_updated.isoformat(),
                    "ts_epoch": s.last_updated.timestamp(),
                    "source_value": src_val,
                    "dest_has_row_at_same_ts": False,
                    "prev_dest_good_ts": None,
                    "next_dest_good_ts": None,
                    "gap_minutes": None,
                    "decision": "imported_no_destination_history",
                    "reason": "Destination had no prior history; full import.",
                }
                if transform is not None:
                    rec["scaled_value"] = _scale_state_value(
                        src_val, transform
                    )
                debug_records.append(rec)
            _LOGGER.info(
                "Destination %s has no history — %s %d source states",
                dest_entity_id,
                "would import" if dry_run else "importing all",
                len(source_states),
            )
        else:
            # None here means the destination has rows but none of them are
            # good — every source state is head-fillable (minus exact-ts twins).
            cutoff_dt = (
                datetime.fromtimestamp(min_ts, tz=timezone.utc)
                if min_ts is not None
                else None
            )

            head: list = []
            mid_stream: list = []
            trailing: list = []

            dest_ts_set: set[float] = set()
            good_dest_ts_list: list[float] = []
            dest_max_good_ts: float | None = None
            trailing_allowed = False
            threshold_sec = gap_threshold_minutes * 60.0

            if fill_gaps:
                # Load every destination row's timestamp + state value (ordered).
                # We need the state value to filter `unavailable` / `unknown` out
                # of gap detection: HA hides those in the History panel so a long
                # unavailable streak LOOKS like a gap, even though the rows are
                # physically present in the DB.
                dest_rows = (
                    session.query(States.last_updated_ts, States.state)
                    .filter(States.metadata_id == metadata_id)
                    .order_by(States.last_updated_ts.asc())
                    .all()
                )
                dest_ts_set = {row[0] for row in dest_rows}
                good_dest_ts_list = [
                    row[0]
                    for row in dest_rows
                    if row[1] is not None and row[1] not in _NON_GOOD_STATES
                ]
                dest_total_rows = len(dest_rows)
                dest_good_rows = len(good_dest_ts_list)

                # Gap-interval count is purely diagnostic (shown in result panel).
                for i in range(len(good_dest_ts_list) - 1):
                    if (
                        good_dest_ts_list[i + 1] - good_dest_ts_list[i]
                    ) >= threshold_sec:
                        gap_intervals_count += 1

                if good_dest_ts_list:
                    dest_max_good_ts = good_dest_ts_list[-1]
                    now_ts = datetime.now(timezone.utc).timestamp()
                    trailing_allowed = (
                        now_ts - dest_max_good_ts
                    ) >= threshold_sec

            for s in source_states:
                ts = s.last_updated.timestamp()
                src_val = str(s.state) if s.state is not None else None
                rec: dict[str, Any] = {
                    "ts": s.last_updated.isoformat(),
                    "ts_epoch": ts,
                    "source_value": src_val,
                    "dest_has_row_at_same_ts": ts in dest_ts_set
                    or ts in head_dedup_ts,
                }
                if transform is not None:
                    rec["scaled_value"] = _scale_state_value(
                        src_val, transform
                    )

                if good_dest_ts_list:
                    i_left = bisect.bisect_left(good_dest_ts_list, ts)
                    i_right = bisect.bisect_right(good_dest_ts_list, ts)
                    prev_good = (
                        good_dest_ts_list[i_left - 1] if i_left > 0 else None
                    )
                    next_good = (
                        good_dest_ts_list[i_right]
                        if i_right < len(good_dest_ts_list)
                        else None
                    )
                else:
                    prev_good = None
                    next_good = None

                rec["prev_dest_good_ts"] = (
                    datetime.fromtimestamp(
                        prev_good, tz=timezone.utc
                    ).isoformat()
                    if prev_good is not None
                    else None
                )
                rec["next_dest_good_ts"] = (
                    datetime.fromtimestamp(
                        next_good, tz=timezone.utc
                    ).isoformat()
                    if next_good is not None
                    else None
                )
                rec["gap_minutes"] = (
                    round((next_good - prev_good) / 60.0, 3)
                    if (prev_good is not None and next_good is not None)
                    else None
                )

                if cutoff_dt is None or s.last_updated < cutoff_dt:
                    if ts in head_dedup_ts or ts in dest_ts_set:
                        rec["decision"] = "skipped_dest_has_same_ts"
                        rec["reason"] = (
                            "Destination already has a row at this exact "
                            "timestamp."
                        )
                    else:
                        head.append(s)
                        rec["decision"] = "head_imported"
                        rec["reason"] = (
                            "Older than destination's oldest good entry — "
                            "head fill."
                            if cutoff_dt is not None
                            else "Destination has only hidden "
                            "(unavailable/unknown) rows — head fill."
                        )
                elif not fill_gaps:
                    rec["decision"] = "skipped_fill_gaps_disabled"
                    rec["reason"] = (
                        "Inside destination's existing range; "
                        "Fill Gaps option is off."
                    )
                elif ts in dest_ts_set:
                    rec["decision"] = "skipped_dest_has_same_ts"
                    rec["reason"] = (
                        "Destination already has a row at this exact timestamp."
                    )
                elif s.state is None or s.state in _NON_GOOD_STATES:
                    in_qualifying_gap = (
                        prev_good is not None
                        and next_good is not None
                        and (next_good - prev_good) >= threshold_sec
                        and prev_good < ts < next_good
                    )
                    in_trailing_gap = (
                        dest_max_good_ts is not None
                        and ts > dest_max_good_ts
                        and trailing_allowed
                    )
                    if in_qualifying_gap or in_trailing_gap:
                        source_skipped_non_good += 1
                        rec["decision"] = "skipped_source_non_good"
                        rec["reason"] = (
                            "Source value is unavailable/unknown; would just "
                            "add another hidden row without closing the gap."
                        )
                    else:
                        rec["decision"] = "skipped_no_qualifying_gap"
                        rec["reason"] = (
                            "Source value is unavailable/unknown and not in "
                            "a qualifying gap."
                        )
                elif dest_max_good_ts is not None and ts > dest_max_good_ts:
                    if trailing_allowed:
                        trailing.append(s)
                        rec["decision"] = "trailing_imported"
                        rec["reason"] = (
                            "Past destination's newest good entry; trailing "
                            "gap meets threshold."
                        )
                    else:
                        rec["decision"] = "skipped_trailing_below_threshold"
                        rec["reason"] = (
                            "Past destination's newest good but trailing gap "
                            f"is below {gap_threshold_minutes} min threshold."
                        )
                elif (
                    prev_good is not None
                    and next_good is not None
                    and (next_good - prev_good) >= threshold_sec
                    and prev_good < ts < next_good
                ):
                    mid_stream.append(s)
                    rec["decision"] = "mid_stream_imported"
                    rec["reason"] = (
                        f"Inside a {rec['gap_minutes']} min gap between "
                        "destination good entries."
                    )
                else:
                    rec["decision"] = "skipped_no_qualifying_gap"
                    if rec["gap_minutes"] is not None:
                        rec["reason"] = (
                            f"Adjacent good entries are {rec['gap_minutes']} "
                            f"min apart (below {gap_threshold_minutes} min "
                            "threshold)."
                        )
                    elif not good_dest_ts_list:
                        rec["reason"] = (
                            "Destination has no good entries to define a gap."
                        )
                    else:
                        rec["reason"] = (
                            "No surrounding good destination entries to "
                            "define a gap."
                        )

                debug_records.append(rec)

            to_import = head + mid_stream + trailing
            mid_stream_filled = len(mid_stream)
            trailing_filled = len(trailing)

            _LOGGER.info(
                "Destination %s oldest good entry: %s — head: %d, mid-stream: %d, "
                "trailing: %d, source_skipped_non_good: %d "
                "(fill_gaps=%s, threshold=%dmin, %d source states, "
                "dest rows: %d total / %d good, %d gap intervals, dry_run=%s)",
                dest_entity_id,
                cutoff_dt.isoformat() if cutoff_dt else "none (only hidden rows)",
                len(head),
                mid_stream_filled,
                trailing_filled,
                source_skipped_non_good,
                fill_gaps,
                gap_threshold_minutes,
                len(source_states),
                dest_total_rows,
                dest_good_rows,
                gap_intervals_count,
                dry_run,
            )

        already_covered = len(source_states) - len(to_import)

        if not to_import or dry_run:
            # Nothing to write (or only previewing): drop the read snapshot.
            session.rollback()  # noqa: ERA001 - releases the transaction
            if dry_run:
                inserted = len(to_import)
                if to_import:
                    imported_min_ts = to_import[0].last_updated.timestamp()
                    imported_max_ts = to_import[-1].last_updated.timestamp()

            return {
                "inserted": inserted,
                "already_covered": already_covered,
                "mid_stream_filled": mid_stream_filled,
                "trailing_filled": trailing_filled,
                "source_skipped_non_good": source_skipped_non_good,
                "dest_total_rows": dest_total_rows,
                "dest_good_rows": dest_good_rows,
                "gap_intervals_count": gap_intervals_count,
                "overwritten": overwritten,
                "overwrite_span": overwrite_span,
                "cutoff_ts": min_ts,
                "imported_min_ts": imported_min_ts,
                "imported_max_ts": imported_max_ts,
                "cancelled": False,
                "debug_records": debug_records,
            }

        # to_import is chronological (head is oldest, then mid-stream, then
        # trailing — each group already sorted and disjoint), which the
        # chunking below relies on to carve contiguous time windows.

        # ====================== PHASE 2: write ======================
        # Same transaction as the plan above, committed once at the end. On
        # the recorder thread there is no other writer to hold up, so this can
        # take as long as it needs.

        # -- Create the destination's StatesMeta row if it does not exist --
        if metadata_id == -1:
            meta = StatesMeta(entity_id=dest_entity_id)
            session.add(meta)
            session.flush()
            metadata_id = meta.metadata_id

        if overwrite and overwrite_span is not None:
            # Same transaction as the inserts that replace these rows, so a
            # failure can never leave the span deleted but not rewritten.
            _delete_states_in_span(
                session, metadata_id, overwrite_span[0], overwrite_span[1]
            )

        attrs_cache: dict[int, int] = {}
        for i in range(0, len(to_import), _WRITE_BATCH_ROWS):
            if cancel_event is not None and cancel_event.is_set():
                session.rollback()
                cancelled = True
                inserted = 0
                imported_min_ts = imported_max_ts = None
                _LOGGER.warning(
                    "Import into %s cancelled before it committed; the "
                    "database is unchanged",
                    dest_entity_id,
                )
                break

            batch = to_import[i : i + _WRITE_BATCH_ROWS]
            session.execute(
                States.__table__.insert(),
                _build_state_rows(
                    session, batch, metadata_id, transform, attrs_cache
                ),
            )
            inserted += len(batch)
        else:
            # -- One commit: all or nothing --
            session.commit()
            imported_min_ts = to_import[0].last_updated.timestamp()
            imported_max_ts = to_import[-1].last_updated.timestamp()
            _LOGGER.info(
                "Committed %d states for %s in a single transaction "
                "(%d source states already covered, %d destination rows "
                "replaced)",
                inserted,
                dest_entity_id,
                already_covered,
                overwritten,
            )

    except Exception:
        session.rollback()
        _LOGGER.error(
            "Rolling back the entire import for %s; no states were written "
            "and no rows were deleted",
            dest_entity_id,
        )
        raise
    finally:
        session.close()

    return {
        "inserted": inserted,
        "already_covered": already_covered,
        "mid_stream_filled": mid_stream_filled,
        "trailing_filled": trailing_filled,
        "source_skipped_non_good": source_skipped_non_good,
        "dest_total_rows": dest_total_rows,
        "dest_good_rows": dest_good_rows,
        "gap_intervals_count": gap_intervals_count,
        "overwritten": overwritten,
        "overwrite_span": overwrite_span,
        "cutoff_ts": min_ts,
        "imported_min_ts": imported_min_ts,
        "imported_max_ts": imported_max_ts,
        "cancelled": cancelled,
        "debug_records": debug_records,
    }


def _build_state_rows(
    session: Any,
    states: list,
    metadata_id: int,
    transform: Callable[[float], float] | None,
    attrs_cache: dict[int, int],
) -> list[dict[str, Any]]:
    """Turn source State objects into rows for a multi-row INSERT.

    One INSERT for the batch rather than an ORM object per state: far less
    time spent holding the write lock, and bounded memory on imports of
    hundreds of thousands of rows.
    """
    rows: list[dict[str, Any]] = []
    for state in states:
        attributes_id = _get_or_create_attributes(
            session, state.attributes, attrs_cache
        )

        # HA convention: NULL last_changed_ts/last_reported_ts mean "same as
        # last_updated_ts".
        last_changed_ts = (
            None
            if state.last_changed == state.last_updated
            else state.last_changed.timestamp()
        )
        last_reported_ts = None
        last_reported = getattr(state, "last_reported", None)
        if last_reported is not None and last_reported != state.last_updated:
            last_reported_ts = last_reported.timestamp()

        if state.state is None:
            state_val = None
        else:
            state_val = str(state.state)
            if transform is not None:
                state_val = _scale_state_value(state_val, transform)
            state_val = state_val[:255]

        rows.append(
            {
                "state": state_val,
                "metadata_id": metadata_id,
                "attributes_id": attributes_id,
                "last_changed_ts": last_changed_ts,
                "last_updated_ts": state.last_updated.timestamp(),
                "last_reported_ts": last_reported_ts,
                "old_state_id": None,
                "origin_idx": 0,  # local origin
                "context_id_bin": None,
                "context_user_id_bin": None,
                "context_parent_id_bin": None,
            }
        )
    return rows


def _delete_states_in_span(
    session: Any,
    metadata_id: int,
    span_lo: float,
    span_hi: float,
) -> None:
    """Delete a destination entity's state rows inside the source's time span.

    Used by overwrite mode, in the same transaction as the inserts that replace
    them. Rows OUTSIDE the span can still reference a deleted row through
    `old_state_id` (a self-referencing FK), so those references are nulled
    first, exactly as HA's own purge does, or the DELETE fails on engines that
    enforce the constraint.
    """
    doomed_ids = [
        row[0]
        for row in session.query(States.state_id)
        .filter(
            States.metadata_id == metadata_id,
            States.last_updated_ts >= span_lo,
            States.last_updated_ts <= span_hi,
        )
        .all()
    ]
    if not doomed_ids:
        return

    # Chunked to stay under SQLite's bound-parameter limit.
    id_chunk = 500
    for i in range(0, len(doomed_ids), id_chunk):
        batch = doomed_ids[i : i + id_chunk]
        session.query(States).filter(States.old_state_id.in_(batch)).update(
            {States.old_state_id: None}, synchronize_session=False
        )
    for i in range(0, len(doomed_ids), id_chunk):
        batch = doomed_ids[i : i + id_chunk]
        session.query(States).filter(States.state_id.in_(batch)).delete(
            synchronize_session=False
        )
    session.flush()


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


# A restarted chain shows the whole running total vanishing: the first row
# after the drop holds one period's worth of consumption, not a lifetime total.
# Requiring the drop to be this steep keeps a genuinely bidirectional `total`
# sensor (whose stored sum legitimately goes down) from being mistaken for one.
_DETACHMENT_MAX_RESIDUAL_FRACTION = 0.01


def _find_sum_detachment(rows: list[dict]) -> dict[str, Any] | None:
    """Find where a stored cumulative series restarted below itself.

    Home Assistant seeds the running total of each compile from the sensor's
    latest short-term row and falls back to 0.0 when there is none. A
    destination that had history imported into it while having no chain of its
    own therefore keeps its imported history but starts compiling its own rows
    from zero, leaving the stored series with a single cliff: a lifetime total,
    then a first hour's worth of consumption.

    Returns the cliff as `{"start_ts", "lift", "before", "after"}` where `lift`
    is the constant that would put the detached tail back on top of the
    history, or None when the series has no such cliff.

    Deliberately conservative, because this decides what to OFFER the user:
    - exactly one drop in the whole series (several means something else is
      going on and the right repair is ambiguous),
    - the value before the drop must be positive,
    - the value after it must be a tiny fraction of the value before, which is
      what "restarted from zero" looks like and what an ordinary decrease in a
      bidirectional `total` sensor does not.
    """
    series = sorted(
        (
            (_row_start_ts(r), float(r["sum"]))
            for r in rows
            if r.get("sum") is not None
        ),
        key=lambda item: item[0],
    )
    if len(series) < 2:
        return None

    drops = [
        i for i in range(len(series) - 1) if series[i + 1][1] < series[i][1]
    ]
    if len(drops) != 1:
        return None

    i = drops[0]
    before = series[i][1]
    after = series[i + 1][1]
    if before <= 0:
        return None
    if not (0 <= after <= before * _DETACHMENT_MAX_RESIDUAL_FRACTION):
        return None

    return {
        "start_ts": series[i + 1][0],
        "lift": before,
        "before": before,
        "after": after,
    }


def _detachment_notice(
    rows: list[dict], unit: str | None
) -> dict[str, Any] | None:
    """Describe a restarted cumulative series for the UI, or None if it is fine."""
    detachment = _find_sum_detachment(rows)
    if not detachment:
        return None
    return {
        "start": datetime.fromtimestamp(
            detachment["start_ts"], tz=timezone.utc
        ).isoformat(),
        "lift": detachment["lift"],
        "before": detachment["before"],
        "after": detachment["after"],
        "unit": unit,
    }


def _compute_sum_offset(
    source_rows: list[dict],
    dest_rows: list[dict],
    fallback_dest_rows: list[dict] | None = None,
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

    `fallback_dest_rows` is consulted when the destination has no long-term row
    with a `sum` yet. That is the case for a destination created shortly before
    the import: HA has already compiled 5-minute rows for it (its running total
    starting at zero) but not the first hourly row. Without the fallback no
    splice point is found, the source series is imported at its own absolute
    values, and the destination's live series stays anchored at zero — which is
    what makes the meter look like it restarts from zero right after the
    imported history.

    Returns None if no offset is needed (no overlap / no sum data on one side /
    offset is effectively zero).
    """
    dest_sum_rows = [r for r in dest_rows if r.get("sum") is not None]
    if not dest_sum_rows and fallback_dest_rows:
        dest_sum_rows = [
            r for r in fallback_dest_rows if r.get("sum") is not None
        ]
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


def _build_stats_debug_records(
    source_rows: list[dict],
    dest_by_start: dict[float, dict],
    stat_cols: tuple[str, ...],
    recent_cutoff_ts: float,
    sum_offset: float | None,
    *,
    dest_max_ts: float | None = None,
    trailing_allowed: bool = True,
    gap_threshold_minutes: int | None = None,
    overwrite: bool = False,
) -> list[dict]:
    """Per-source-row classification used for the downloadable debug JSON.

    Mirrors the import partitioning logic exactly. For short-term stats, pass
    `dest_max_ts`, `trailing_allowed`, and `gap_threshold_minutes` so the
    "skipped because past dest's newest below threshold" branch is reported.
    For LTS, leave those defaults — there is no trailing-threshold gate.
    """
    records: list[dict] = []
    for src_row in source_rows:
        start_ts = _row_start_ts(src_row)
        rec: dict[str, Any] = {
            "start": datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat(),
            "start_epoch": start_ts,
        }
        for k in stat_cols:
            rec[f"source_{k}"] = src_row.get(k)

        dest_row = dest_by_start.get(start_ts)
        for k in stat_cols:
            rec[f"dest_{k}"] = dest_row.get(k) if dest_row else None

        src_values = {
            k: src_row[k] for k in stat_cols if src_row.get(k) is not None
        }

        if start_ts > recent_cutoff_ts:
            rec["decision"] = "skipped_recent"
            rec["reason"] = (
                "Within the recent-compile safety window; HA may still be "
                "compiling this slot."
            )
        elif (
            dest_max_ts is not None
            and start_ts > dest_max_ts
            and not trailing_allowed
        ):
            rec["decision"] = "skipped_trailing_below_threshold"
            rec["reason"] = (
                "Past destination's newest stats slot but trailing gap is "
                f"below {gap_threshold_minutes} min threshold."
            )
        elif not src_values:
            rec["decision"] = "skipped_source_empty"
            rec["reason"] = "Source has a row for this slot but no useful values."
        elif dest_row is None:
            rec["decision"] = "imported_no_dest_row"
            rec["reason"] = (
                "Destination has no row for this slot — full insert from source."
            )
            if sum_offset is not None and "sum" in src_values:
                rec["applied_sum_offset"] = sum_offset
        elif overwrite:
            dest_values = {
                k: dest_row[k] for k in stat_cols if dest_row.get(k) is not None
            }
            merged = dict(dest_values)
            merged.update(src_values)
            if merged == dest_values:
                rec["decision"] = "already_complete"
                rec["reason"] = (
                    "Destination already holds exactly these values."
                )
            else:
                rec["decision"] = "overwritten"
                rec["reason"] = (
                    "Overwrite mode: destination values replaced by the "
                    "source's for column(s) "
                    + ", ".join(sorted(src_values))
                )
                if sum_offset is not None and "sum" in src_values:
                    rec["applied_sum_offset"] = sum_offset
        else:
            dest_values = {
                k: dest_row[k] for k in stat_cols if dest_row.get(k) is not None
            }
            fillable = {
                k: v for k, v in src_values.items() if k not in dest_values
            }
            if not fillable:
                rec["decision"] = "already_complete"
                rec["reason"] = (
                    "Destination already has non-NULL values for every column "
                    "the source provides."
                )
            else:
                rec["decision"] = "imported_gap_filled"
                rec["reason"] = (
                    "Destination row exists but is missing column(s): "
                    + ", ".join(sorted(fillable))
                )
                if sum_offset is not None and "sum" in fillable:
                    rec["applied_sum_offset"] = sum_offset
        records.append(rec)
    return records


async def _async_import_statistics_for_pair(
    hass: HomeAssistant,
    source_id: str,
    dest_id: str,
    *,
    dry_run: bool = False,
    transform: Callable[[float], float] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Import long-term statistics from source to destination — gap-fill mode.

    Key behaviors:

    1. **Gap-fill, not overwrite.** Only inserts for hours where the destination
       has no existing LTS row. Existing destination rows are preserved as-is.
       This prevents the previous upsert behavior from accidentally nulling out
       populated columns (e.g. setting `sum=NULL` because the source row only
       had `mean` set — `_update_statistics` uses `.get()` for every column).
       When `overwrite` is True this inverts: the source's values win for every
       column it provides, and only columns the source lacks keep the
       destination's values. The splice offset is then computed against the
       destination rows that SURVIVE (those outside the source's coverage),
       since the overwritten ones no longer define the join point.

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
        "stats_overwritten": 0,  # hours whose existing values were replaced
        "stats_imported_start": None,
        "stats_imported_end": None,
        "stats_sum_offset": None,
        "stats_sum_seeded": None,
        "stats_detached": None,
        "stats_unit": None,
        "stats_unit_mismatch": None,
        "debug_stats": [],
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
    source_stats_raw, dest_stats_raw, dest_metadata_map, target = (
        await recorder.async_add_executor_job(
            _fetch_stats_snapshot,
            hass,
            recorder,
            source_id,
            dest_id,
            transform is None,
        )
    )
    out["stats_unit"] = target["unit"]
    if target["units_differ"]:
        # Not converted for you: a factor can mean a unit change or something
        # else entirely, and silently guessing would double up with any
        # adjustment already set, and would disagree with the raw-state import,
        # which has no unit handling of its own.
        out["stats_unit_mismatch"] = {
            "source": target["source_unit"],
            "destination": target["unit"],
            # Offered in the panel for the user to fill in; never applied here.
            "suggestion": target["suggestion"],
        }
        _LOGGER.warning(
            "Source %s stores its statistics in %s but the destination %s "
            "stores %s. The values are imported exactly as stored; use the "
            "value adjustment option if they need scaling",
            source_id,
            target["source_unit"],
            dest_id,
            target["unit"],
        )

    source_rows = source_stats_raw.get(source_id, [])
    dest_rows = dest_stats_raw.get(dest_id, [])

    # -- Look at the destination's own statistics --
    # Three things, all about how HA will continue the destination's running
    # total after this import:
    #
    #  - Does it have a 5-minute row carrying a sum? A destination created
    #    shortly before the import has no hourly row yet, so there would be no
    #    splice point and the imported series would be written at its own
    #    absolute values while the destination's live total stays anchored at
    #    zero. Its 5-minute rows provide the splice point instead. Only asked
    #    when there is no hourly row to splice against.
    #  - Does it have ANY 5-minute row? If not, HA has no chain to continue and
    #    seeds every compile at 0.0, so the running total has to be seeded (see
    #    further below). This is independent of whether the destination has
    #    hourly rows: a destination that was imported into before, but has
    #    never compiled statistics of its own, has hourly rows and no chain.
    #  - Does its stored hourly series already restart from zero partway
    #    through? That is damage an earlier import left behind, which this run
    #    can only report and offer to repair.
    dest_has_hourly_sum = any(r.get("sum") is not None for r in dest_rows)
    dest_anchor = await recorder.async_add_executor_job(
        partial(
            _fetch_dest_sum_anchor,
            recorder,
            dest_id,
            want_earliest_sum=not dest_has_hourly_sum,
            want_hourly=True,
        )
    )
    # Read straight off the stored column, so the reported amount is in the
    # unit the repair will actually add.
    out["stats_detached"] = _detachment_notice(
        dest_anchor["hourly"], dest_anchor["unit"]
    )

    out["stats_source_total"] = len(source_rows)
    if not source_rows:
        # Nothing left to import from this source, but the destination may
        # still be carrying a restart-from-zero from an earlier import, and
        # repairing that does not need the source at all.
        return out

    # -- Apply the user's value adjustment BEFORE any splice math --
    # The sum offset and column merges below must operate in the destination's
    # value space, so the source rows are adjusted first. This is the only
    # thing that rescales values, for statistics and raw states alike, which
    # is what keeps the two halves of an import consistent with each other.
    source_rows = _scale_stat_rows(source_rows, transform)

    source_has_sum_values = any(r.get("sum") is not None for r in source_rows)
    anchor_rows: list[dict] = (
        [dest_anchor["earliest_sum"]] if dest_anchor["earliest_sum"] else []
    )

    # -- Compute sum offset (None if not applicable) --
    # In overwrite mode the destination rows the source is about to replace no
    # longer define where the two series join: the splice point is the earliest
    # SURVIVING destination row, so those are the only ones considered.
    if overwrite:
        replaced_ts = {
            _row_start_ts(r)
            for r in source_rows
            if _row_start_ts(r) <= recent_cutoff_ts
            and any(r.get(k) is not None for k in ("mean", "min", "max", "sum", "state"))
        }
        surviving_dest_rows = [
            r for r in dest_rows if _row_start_ts(r) not in replaced_ts
        ]
    else:
        surviving_dest_rows = dest_rows
    sum_offset = _compute_sum_offset(source_rows, surviving_dest_rows, anchor_rows)

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
    overwritten = 0
    debug_stats = _build_stats_debug_records(
        source_rows,
        dest_by_start,
        stat_cols,
        recent_cutoff_ts,
        sum_offset,
        overwrite=overwrite,
    )

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

        if overwrite:
            # Source wins for every column it provides; columns it does not
            # provide keep the destination's value rather than being nulled.
            data = dict(dest_values)
            for k, v in src_values.items():
                if k == "sum" and sum_offset is not None:
                    v = float(v) + sum_offset
                data[k] = v
            if data == dest_values:
                # Identical values already stored (e.g. a repeat run).
                already_covered += 1
                continue
            to_import_rows.append((start_ts, data))
            overwritten += 1
            continue

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
    out["stats_overwritten"] = overwritten
    out["debug_stats"] = debug_stats

    # -- Resolve metadata: prefer destination's existing metadata --
    # Resolved before the "nothing to import" exit below, because the running
    # total may still need seeding on a run that imports no new hourly row.
    has_sum = any(r.get("sum") is not None for r in source_rows)
    has_mean = any(r.get("mean") is not None for r in source_rows)

    dest_meta_entry = dest_metadata_map.get(dest_id) if dest_metadata_map else None
    existing_metadata = dest_meta_entry[1] if dest_meta_entry else None

    # The unit every value in this function is expressed in: the destination's
    # stored unit, which is what the rows were read in and what will be
    # written back. Never the entity's display unit (see _resolve_target_unit).
    unit: str | None = target["unit"]
    if existing_metadata:
        # Reuse the destination's current metadata verbatim, except that we
        # force statistic_id and source (these must match for async_import_statistics).
        metadata = dict(existing_metadata)
        metadata["statistic_id"] = dest_id
        metadata["source"] = "recorder"
    else:
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

    _ensure_unit_class(metadata)

    # --- Seed the destination's running total when it has no chain to continue ---
    # HA seeds `_sum` for every compile from the destination's latest short-term
    # row and uses 0.0 when there is none (sensor/recorder.py). A destination
    # with no short-term row at all — a helper or meter created for this import
    # — would therefore start its own series at zero right after the history we
    # just imported, which reads as "the meter restarted from zero".
    #
    # The seed value is the running total at the newest point the destination's
    # history reaches once this import lands, counting both the rows being
    # imported and the rows already there.
    #
    # Only planned here; it is queued (and the "no chain" check repeated
    # against a fresh reading) in _do_import, since HA may compile the
    # destination's first 5-minute row while the rest of this import runs.
    if source_has_sum_values and not dest_anchor["has_rows"]:
        merged_sums: dict[float, float] = {
            _row_start_ts(r): float(r["sum"])
            for r in dest_rows
            if r.get("sum") is not None
        }
        # Imported values win at any hour they cover.
        merged_sums.update(
            {ts: float(data["sum"]) for ts, data in to_import_rows if "sum" in data}
        )
        if merged_sums:
            out["_sum_seed"] = {
                "sum": merged_sums[max(merged_sums)],
                "metadata": dict(metadata),
            }
            out["stats_sum_seeded"] = merged_sums[max(merged_sums)]

    if not to_import_rows:
        # No offset is reported: with nothing imported, nothing is shifted, and
        # the panel would otherwise announce an offset "would be applied".
        return out

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
    if dry_run:
        _LOGGER.info(
            "Dry run: skipping queueing of %d statistics rows for %s",
            len(stats_data),
            dest_id,
        )
    else:
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

    # --- Plan a series realignment for a clean head-fill energy import ---
    # When every imported sum row is OLDER than the destination's oldest existing
    # sum row, we've extended the cumulative series backwards. HA anchors the very
    # first point of a sum series against 0 and does NO reset detection when reading
    # stored statistics, so the oldest imported hour would otherwise display the
    # whole splice offset as a one-off value (and skew the lifetime total in the
    # Energy sources table). Instead of leaving the imported series shifted down to
    # meet the destination (which pushes that offset onto the first hour), we lift
    # the ENTIRE series — imported rows, the destination's existing rows, and all
    # future compiled rows — by -offset via HA's official adjust API. The oldest
    # hour then reads its true value and old/new join seamlessly. The adjust itself
    # is queued in _do_import, after the imports, and it runs whatever the
    # short-term step did: both tables are spliced against the same instant in a
    # head-fill, so one constant is right for both (see the note there). A re-run
    # recomputes a ~zero offset against the now-aligned destination, so this
    # stays idempotent.
    #
    # Restart-safe by construction (verified against HA's compile source): the lift
    # is a constant added to the `sum` column of both the short-term and long-term
    # tables (async_import_statistics writes both). On every compile HA re-seeds
    # `_sum` from the latest short-term row read back from the DB and then only adds
    # STATE deltas (sensor/recorder.py: _sum mutations are seed + `new_state -
    # old_state`), so a constant sum offset is preserved across restarts and never
    # re-derived from absolute state. It does NOT fix or cause the separate
    # "sensor reports 0 during a restart" spike (that is source-side, state-only).
    # Do not revert this to touch only imported rows without re-checking that trace.
    if sum_offset is not None:
        dest_sum_ts = [
            _row_start_ts(r)
            for r in surviving_dest_rows
            if r.get("sum") is not None
        ]
        if not dest_sum_ts and anchor_rows:
            # The join point came from the destination's 5-minute rows. The lift
            # reaches those too: adjust_statistics updates both the short-term
            # and the long-term table, so the destination's next compile re-seeds
            # its running total from a lifted row and the series stays continuous.
            dest_sum_ts = [_row_start_ts(anchor_rows[0])]
        imported_sum_ts = [ts for ts, data in to_import_rows if "sum" in data]
        if (
            dest_sum_ts
            and imported_sum_ts
            and max(imported_sum_ts) < min(dest_sum_ts)
        ):
            out["_realign"] = {
                "start_ts": min(imported_sum_ts),
                "adjustment": -sum_offset,  # positive lift for the common case
                "unit": unit,
            }
            # The splice offset is undone by the lift, so report the net effect
            # (a positive lift of the running total), not the raw offset, and drop
            # the "first hour may be off" caveat.
            out["stats_sum_offset"] = None
            out["stats_realigned_by"] = -sum_offset

    _LOGGER.info(
        "%s %d statistics rows for %s "
        "(%d already complete in destination, %d gap-filled (NULL columns), "
        "%d overwritten, %d skipped as too recent, sum offset: %s)",
        "Would queue" if dry_run else "Queued",
        len(stats_data),
        dest_id,
        already_covered,
        gap_filled,
        overwritten,
        skipped_recent,
        sum_offset,
    )
    return out


def _fetch_dest_sum_anchor(
    recorder_instance: Any,
    statistic_id: str,
    want_earliest_sum: bool = True,
    want_hourly: bool = False,
) -> dict[str, Any]:
    """Look at a destination's short-term (5-minute) statistics for splicing.

    Returns `has_rows` (does the destination have ANY short-term row?) and
    `earliest_sum` (the oldest row carrying a non-NULL `sum`, as a
    `{"start", "sum"}` dict, or None).

    Both matter for cumulative sensors, because HA seeds the running total of
    every new compile from the destination's LATEST short-term row
    (sensor/recorder.py: `_sum = last_stat.get("sum") or 0.0`) and falls back
    to 0.0 when there is no such row:

      - `earliest_sum` gives a splice point when the destination is too young
        to have an hourly row yet, so the imported series can be offset onto
        the destination's own series (and then lifted as a whole by the
        realignment step).
      - `has_rows` False means the destination has no compile chain at all, so
        a seed row has to be written for the running total to continue from the
        end of the imported history instead of from zero.

    Only the two rows we need are queried, not the destination's whole
    short-term history (up to ~2900 rows per day).
    """
    session = recorder_instance.get_session()
    try:
        metadata_id = (
            session.query(StatisticsMeta.id)
            .filter(StatisticsMeta.statistic_id == statistic_id)
            .scalar()
        )
        if metadata_id is None:
            return {
                "has_rows": False,
                "earliest_sum": None,
                "hourly": [],
                "unit": None,
            }

        has_rows = (
            session.query(StatisticsShortTerm.id)
            .filter(StatisticsShortTerm.metadata_id == metadata_id)
            .first()
            is not None
        )
        earliest = (
            session.query(
                StatisticsShortTerm.start_ts, StatisticsShortTerm.sum
            )
            .filter(
                StatisticsShortTerm.metadata_id == metadata_id,
                StatisticsShortTerm.sum.isnot(None),
            )
            .order_by(StatisticsShortTerm.start_ts.asc())
            .first()
            if want_earliest_sum
            else None
        )
        earliest_sum = (
            {"start": earliest[0], "sum": earliest[1]}
            if earliest is not None
            else None
        )

        # The hourly series is read RAW, straight off the column. Going through
        # statistics_during_period would convert it to the entity's display
        # unit, and a lift measured there cannot be applied to the stored
        # column without converting it back.
        hourly: list[dict] = []
        unit: str | None = None
        if want_hourly:
            unit = (
                session.query(StatisticsMeta.unit_of_measurement)
                .filter(StatisticsMeta.statistic_id == statistic_id)
                .scalar()
            )
            hourly = [
                {"start": row[0], "sum": row[1]}
                for row in session.query(Statistics.start_ts, Statistics.sum)
                .filter(
                    Statistics.metadata_id == metadata_id,
                    Statistics.sum.isnot(None),
                )
                .order_by(Statistics.start_ts.asc())
                .all()
            ]

        return {
            "has_rows": has_rows,
            "earliest_sum": earliest_sum,
            "hourly": hourly,
            "unit": unit,
        }
    finally:
        session.close()


def _repair_sum_series(
    recorder_instance: Any, statistic_id: str
) -> dict[str, Any]:
    """Lift a restarted cumulative series back on top of its own history.

    Detection and the lift happen in one transaction against the stored
    columns, which is what makes the repair safe to trigger twice:

    - Reading raw keeps the lift in the unit the column is stored in.
      `statistics_during_period` converts to the entity's display unit, which
      is not necessarily the same, and HA's `adjust_statistics` only converts
      when told which display unit the adjustment is expressed in.
    - `async_adjust_statistics` merely queues a task on the recorder thread,
      so a second click (or a re-run of the import) could read a database that
      did not yet reflect the first one and queue the same lift again. Doing
      the update here means that once this returns, the cliff really is gone
      and any later detection sees that.

    Returns `{"repaired": bool, ...}` describing what happened.
    """
    session = recorder_instance.get_session()
    try:
        _set_sqlite_busy_timeout(session)

        meta_row = (
            session.query(StatisticsMeta.id, StatisticsMeta.unit_of_measurement)
            .filter(StatisticsMeta.statistic_id == statistic_id)
            .first()
        )
        if meta_row is None:
            return {
                "repaired": False,
                "reason": f"{statistic_id} has no statistics to repair.",
            }
        metadata_id, unit = meta_row[0], meta_row[1]

        rows = [
            {"start": row[0], "sum": row[1]}
            for row in session.query(Statistics.start_ts, Statistics.sum)
            .filter(
                Statistics.metadata_id == metadata_id,
                Statistics.sum.isnot(None),
            )
            .order_by(Statistics.start_ts.asc())
            .all()
        ]
        detachment = _find_sum_detachment(rows)
        if not detachment:
            return {
                "repaired": False,
                "reason": (
                    f"{statistic_id} no longer has a restarted running total. "
                    "Nothing to repair."
                ),
            }

        lift = float(detachment["lift"])
        start_ts = float(detachment["start_ts"])
        for table in (Statistics, StatisticsShortTerm):
            session.query(table).filter(
                table.metadata_id == metadata_id,
                table.start_ts >= start_ts,
                table.sum.isnot(None),
            ).update({table.sum: table.sum + lift}, synchronize_session=False)
        session.commit()

        return {
            "repaired": True,
            "lift": lift,
            "start_ts": start_ts,
            "unit": unit,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _read_stored_statistics(
    session: Any, statistic_id: str, table: Any
) -> list[dict]:
    """Read one statistic's rows exactly as they are stored.

    Deliberately not `statistics_during_period`, which always converts to the
    entity's display unit. Asking it for a specific unit instead means
    matching the unit-class key HA derives internally, which comes from the
    class declared in metadata before the unit, and whose unit-keyed map omits
    HA's secondary converters. A key that does not match fails silently, back
    to display units. Reading the columns has no key to get wrong, and puts
    these rows in the same space as the detachment detector and the repair.
    """
    metadata_id = (
        session.query(StatisticsMeta.id)
        .filter(StatisticsMeta.statistic_id == statistic_id)
        .scalar()
    )
    if metadata_id is None:
        return []
    return [
        {
            "start": row[0],
            "mean": row[1],
            "min": row[2],
            "max": row[3],
            "sum": row[4],
            "state": row[5],
        }
        for row in session.query(
            table.start_ts,
            table.mean,
            table.min,
            table.max,
            table.sum,
            table.state,
        )
        .filter(table.metadata_id == metadata_id)
        .order_by(table.start_ts.asc())
        .all()
    ]


def _fetch_stats_snapshot(
    hass: HomeAssistant,
    recorder_instance: Any,
    source_id: str,
    dest_id: str,
    allow_source_fallback: bool,
) -> tuple[dict, dict, dict, dict]:
    """Hourly source and destination rows, as stored, plus the unit context."""
    return _fetch_period_snapshot(
        hass, recorder_instance, source_id, dest_id, Statistics,
        allow_source_fallback,
    )


def _fetch_short_term_stats_snapshot(
    hass: HomeAssistant,
    recorder_instance: Any,
    source_id: str,
    dest_id: str,
    allow_source_fallback: bool,
) -> tuple[dict, dict, dict, dict]:
    """Same as `_fetch_stats_snapshot` for the 5-minute table."""
    return _fetch_period_snapshot(
        hass, recorder_instance, source_id, dest_id, StatisticsShortTerm,
        allow_source_fallback,
    )


def _fetch_period_snapshot(
    hass: HomeAssistant,
    recorder_instance: Any,
    source_id: str,
    dest_id: str,
    table: Any,
    allow_source_fallback: bool,
) -> tuple[dict, dict, dict, dict]:
    """Read one period's source and destination rows in their stored units.

    Returns the source rows, the destination rows, the destination's metadata
    and the unit context: the unit the import will write in, and whether the
    source's stored unit differs from it (in which case nothing is converted
    and the user has to say what to do about it).
    """
    dest_metadata = get_metadata(hass, statistic_ids={dest_id})
    source_metadata = get_metadata(hass, statistic_ids={source_id})
    source_entry = source_metadata.get(source_id) if source_metadata else None
    source_unit = (
        source_entry[1].get("unit_of_measurement") if source_entry else None
    )

    target_unit = _resolve_target_unit(
        hass, dest_id, dest_metadata, source_unit, allow_source_fallback
    )

    session = recorder_instance.get_session()
    try:
        source_rows = _read_stored_statistics(session, source_id, table)
        dest_rows = _read_stored_statistics(session, dest_id, table)
    finally:
        session.close()

    # Both sides are read as stored, so a difference here is a real difference
    # in the numbers, not a display setting.
    units_differ = bool(source_entry and source_unit != target_unit)
    dest_entry = dest_metadata.get(dest_id) if dest_metadata else None
    target = {
        "unit": target_unit,
        "source_unit": source_unit,
        "units_differ": units_differ,
        "suggestion": (
            _suggest_unit_adjustment(
                source_unit,
                target_unit,
                (
                    source_entry[1].get("unit_class") if source_entry else None,
                    dest_entry[1].get("unit_class") if dest_entry else None,
                ),
            )
            if units_differ
            else None
        ),
    }
    return {source_id: source_rows}, {dest_id: dest_rows}, dest_metadata, target


async def _async_import_short_term_statistics_for_pair(
    hass: HomeAssistant,
    source_id: str,
    dest_id: str,
    *,
    gap_threshold_minutes: int,
    dry_run: bool = False,
    transform: Callable[[float], float] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Backfill short-term (5-minute) statistics from source to destination.

    Opt-in via the `fill_gaps` flag (or `overwrite`, which targets existing
    data by definition). Behaviors:

    1. **Gap-fill, not overwrite.** Only inserts for 5-min slots where the
       destination has no existing short-term row, using the same column-merge
       semantics as the LTS path to avoid nulling out existing columns.

    2. **Tight recent-slot cutoff.** HA's 5-min compile runs at HH:MM:10 UTC
       (MM in {0,5,…,55}) and writes the just-finished 5-min slot. To stay
       well clear of an in-flight compile, we skip the most recent two 5-min
       boundaries: only slots ending <= floor_5min(now) - 10min are considered.

    3. **Trailing-edge threshold.** Source rows newer than the destination's
       newest short-term row are only imported if (now - dest_newest) >=
       `gap_threshold_minutes`. Mid-stream missing slots are always filled
       (bounded by the recent-cutoff rule).

    4. **Cumulative-sum offset for energy sensors.** Reuses the same splice-
       point offset the LTS path computes.

    5. **Preserve existing destination metadata.** Same reasoning as LTS.

    Returns a dict with `stats_short_*` fields.
    """
    recorder = get_instance(hass)

    out: dict[str, Any] = {
        "stats_short_source_total": 0,
        "stats_short_imported": 0,
        "stats_short_already_covered": 0,
        "stats_short_skipped_recent": 0,
        "stats_short_overwritten": 0,
        "stats_short_imported_start": None,
        "stats_short_imported_end": None,
        "debug_stats_short": [],
    }

    # -- Recent-slot cutoff: floor_5min(now) - 10 min --
    now = datetime.now(timezone.utc)
    floor_5min = now.replace(
        minute=(now.minute // 5) * 5, second=0, microsecond=0
    )
    recent_cutoff_dt = floor_5min - timedelta(minutes=10)
    recent_cutoff_ts = recent_cutoff_dt.timestamp()

    source_stats_raw, dest_stats_raw, dest_metadata_map, target = (
        await recorder.async_add_executor_job(
            _fetch_short_term_stats_snapshot,
            hass,
            recorder,
            source_id,
            dest_id,
            transform is None,
        )
    )

    source_rows = source_stats_raw.get(source_id, [])
    dest_rows = dest_stats_raw.get(dest_id, [])

    out["stats_short_source_total"] = len(source_rows)
    if not source_rows:
        return out

    # -- Apply the unit scaling factor BEFORE any splice math (see LTS path) --
    source_rows = _scale_stat_rows(source_rows, transform)

    # -- Reuse LTS splice-offset logic (works identically on 5-min rows) --
    # Same surviving-rows rule as the LTS path in overwrite mode.
    if overwrite:
        replaced_ts = {
            _row_start_ts(r)
            for r in source_rows
            if _row_start_ts(r) <= recent_cutoff_ts
            and any(r.get(k) is not None for k in ("mean", "min", "max", "sum", "state"))
        }
        surviving_dest_rows = [
            r for r in dest_rows if _row_start_ts(r) not in replaced_ts
        ]
    else:
        surviving_dest_rows = dest_rows
    sum_offset = _compute_sum_offset(source_rows, surviving_dest_rows)

    dest_by_start: dict[float, dict] = {_row_start_ts(r): r for r in dest_rows}

    # Trailing-edge threshold: only fill slots after dest_max_ts if the gap
    # from there to now() meets the user's threshold. Overwrite bypasses the
    # gate: it replaces the source's whole span, trailing slots included.
    threshold_sec = gap_threshold_minutes * 60.0
    dest_max_ts: float | None = None
    trailing_allowed = True
    if dest_rows and not overwrite:
        dest_max_ts = max(_row_start_ts(r) for r in dest_rows)
        trailing_allowed = (now.timestamp() - dest_max_ts) >= threshold_sec

    stat_cols = ("mean", "min", "max", "sum", "state")

    to_import_rows: list[tuple[float, dict[str, Any]]] = []
    already_covered = 0
    skipped_recent = 0
    overwritten = 0
    debug_stats_short = _build_stats_debug_records(
        source_rows,
        dest_by_start,
        stat_cols,
        recent_cutoff_ts,
        sum_offset,
        dest_max_ts=dest_max_ts,
        trailing_allowed=trailing_allowed,
        gap_threshold_minutes=gap_threshold_minutes,
        overwrite=overwrite,
    )

    for src_row in source_rows:
        start_ts = _row_start_ts(src_row)
        if start_ts > recent_cutoff_ts:
            skipped_recent += 1
            continue

        # Trailing gate: source rows beyond dest's newest are only taken if the
        # trailing gap meets threshold.
        if (
            dest_max_ts is not None
            and start_ts > dest_max_ts
            and not trailing_allowed
        ):
            skipped_recent += 1
            continue

        src_values = {k: src_row[k] for k in stat_cols if src_row.get(k) is not None}
        if not src_values:
            already_covered += 1
            continue

        dest_row = dest_by_start.get(start_ts)

        if dest_row is None:
            data = dict(src_values)
            if "sum" in data and sum_offset is not None:
                data["sum"] = float(data["sum"]) + sum_offset
            to_import_rows.append((start_ts, data))
            continue

        dest_values = {k: dest_row[k] for k in stat_cols if dest_row.get(k) is not None}

        if overwrite:
            data = dict(dest_values)
            for k, v in src_values.items():
                if k == "sum" and sum_offset is not None:
                    v = float(v) + sum_offset
                data[k] = v
            if data == dest_values:
                already_covered += 1
                continue
            to_import_rows.append((start_ts, data))
            overwritten += 1
            continue

        fillable = {k: v for k, v in src_values.items() if k not in dest_values}
        if not fillable:
            already_covered += 1
            continue

        data = dict(dest_values)
        for k, v in fillable.items():
            if k == "sum" and sum_offset is not None:
                v = float(v) + sum_offset
            data[k] = v

        to_import_rows.append((start_ts, data))

    out["stats_short_already_covered"] = already_covered
    out["stats_short_skipped_recent"] = skipped_recent
    out["stats_short_overwritten"] = overwritten
    out["debug_stats_short"] = debug_stats_short

    if not to_import_rows:
        return out

    # -- Resolve metadata (prefer destination's existing) --
    has_sum = any(r.get("sum") is not None for r in source_rows)
    has_mean = any(r.get("mean") is not None for r in source_rows)

    dest_meta_entry = dest_metadata_map.get(dest_id) if dest_metadata_map else None
    existing_metadata = dest_meta_entry[1] if dest_meta_entry else None

    if existing_metadata:
        metadata = dict(existing_metadata)
        metadata["statistic_id"] = dest_id
        metadata["source"] = "recorder"
    else:
        # Same stored-unit rule as the hourly path.
        meta_kwargs: dict[str, Any] = {
            "has_sum": has_sum,
            "name": None,
            "source": "recorder",
            "statistic_id": dest_id,
            "unit_of_measurement": target["unit"],
        }
        if StatisticMeanType is not None:
            meta_kwargs["mean_type"] = (
                StatisticMeanType.ARITHMETIC if has_mean else StatisticMeanType.NONE
            )
        else:
            meta_kwargs["has_mean"] = has_mean
        metadata = StatisticMetaData(**meta_kwargs)

    _ensure_unit_class(metadata)

    # -- Build StatisticData entries --
    stats_data = []
    for start_ts, data in to_import_rows:
        start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
        entry: dict[str, Any] = {"start": start_dt}
        for key in stat_cols:
            if key in data:
                entry[key] = data[key]
        stats_data.append(StatisticData(**entry))

    # -- Queue via the recorder instance method (accepts a `table` arg) --
    # This path runs under HA's unique-constraint integrity-error filter and
    # correctly updates ShortTermStatisticsRunCache — much safer than direct
    # ORM inserts.
    if dry_run:
        _LOGGER.info(
            "Dry run: skipping queueing of %d short-term stats rows for %s",
            len(stats_data),
            dest_id,
        )
    else:
        recorder.async_import_statistics(metadata, stats_data, StatisticsShortTerm)

    imported_starts = sorted(start_ts for start_ts, _ in to_import_rows)
    out["stats_short_imported"] = len(stats_data)
    out["stats_short_imported_start"] = datetime.fromtimestamp(
        imported_starts[0], tz=timezone.utc
    ).isoformat()
    out["stats_short_imported_end"] = datetime.fromtimestamp(
        imported_starts[-1], tz=timezone.utc
    ).isoformat()

    _LOGGER.info(
        "%s %d short-term (5-min) stats rows for %s "
        "(%d already complete in destination, %d skipped as too recent)",
        "Would queue" if dry_run else "Queued",
        len(stats_data),
        dest_id,
        already_covered,
        skipped_recent,
    )
    return out
