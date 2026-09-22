# HA Merge Sensor History

A Home Assistant custom component to import historical sensor data from one entity into another.

Built for migrating sensor data between integrations — for example, when replacing an Ecowitt integration with a different one and you want the new sensors to carry the old sensors' history.

## Features

- **Sidebar panel** with a simple UI: select source/destination pairs, click Import
- **Imports both states and long-term statistics** (hourly aggregates for energy dashboard / long-term graphs)
- **Atomic**: an import is written in a single transaction, so either all of it lands or none of it does
- **Never blocks the recorder**: Home Assistant keeps recording normally throughout, even during a very large import
- **Idempotent**: safe to re-run; a successful import shifts the cutoff so nothing is re-imported
- **Optional overwrite mode** for destinations holding known-bad data (opt-in, destructive, clearly warned)
- **Entity filter** to quickly find sensors by keyword
- **Also available as a service** (`merge_sensor_history.import_history`) for use in automations or Developer Tools
- **HACS compatible**

## How it works

1. Reads **all** historical states from the source entity via the recorder API
2. Queries the destination entity's **oldest good entry**. Hidden `unavailable`/`unknown` rows are not counted as coverage: a destination whose earliest rows are just unavailable markers (common when the entity id existed before, e.g. as a ghost of a removed integration) has no visible history there
3. Imports only source states that are **strictly older** than that oldest good entry, skipping any exact-timestamp duplicates. This prevents overlap or duplication
4. Imports **long-term statistics** (hourly mean/min/max/sum) via the official `async_import_statistics` API, which is inherently deduplicated by the database schema
5. Commits everything in a **single transaction**, so if anything fails the whole import is rolled back and you can safely retry. The write is fast enough that the recorder never notices it: an ordinary import holds the database for a fraction of a second

### What gets imported

| Data | Source | Granularity | Retention |
|---|---|---|---|
| **States** | `states` table | Every state change | Limited by recorder purge (default ~10 days) |
| **Statistics** | `statistics` table | Hourly aggregates | Kept indefinitely |

HA does **not** regenerate statistics from states retroactively. Both are imported separately to ensure complete history coverage — including long-term statistics from periods whose raw states have already been purged.

## Installation

### HACS (recommended)

1. Open HACS in your Home Assistant instance
2. Click the three dots in the top right corner, select **Custom repositories**
3. Add `https://github.com/mayerwin/HA-Merge-Sensor-History` as an **Integration**
4. Search for "Merge Sensor History" and install it
5. Restart Home Assistant

### Manual

1. Copy the `custom_components/merge_sensor_history` folder into your Home Assistant `config/custom_components/` directory
2. Restart Home Assistant

## Setup

1. Go to **Settings > Devices & Services > Add Integration**
2. Search for **Merge Sensor History**
3. Click Submit — this enables the integration and adds the sidebar panel

## Usage

### Sidebar panel

1. Click **Merge History** in the sidebar
2. Select a **source** entity (the old sensor with historical data)
3. Select a **destination** entity (the new sensor you want the history imported into)
4. Use **+ Add Pair** to queue multiple imports at once, or **Bulk add pairs** to paste a whole list of `source, destination` lines in one go
5. To merge from a **deleted or disabled** entity (one with no live state but whose data is still in the recorder), enable **Show deleted/disabled entities** above Bulk add pairs. Those ids then appear in the dropdowns and Bulk add, tagged by kind. A *deleted* entity keeps only its hourly statistics (its raw states are purged per your recorder retention, about 10 days by default), so the merge fills the Energy dashboard and long-term graphs, not the History panel. A *disabled* entity may still have recent raw states in the recorder, and those are merged too. Since raw states keep aging out daily, merge from a disabled entity as soon as possible
6. Use the **filter** field to narrow down entities by keyword (e.g., `ecowitt`, `temperature`). Uncheck **Same filter for both** to filter the source and destination lists separately — handy when only a serial number differs between the old and new sensors (filter source on the old serial, destination on the new one)
7. If the sensors record the same quantity in **different units**, enable **Adjust imported values** under Options. **Multiply by** covers simple factors (e.g. `1000` for kWh → Wh, `0.001` for Wh → kWh); **Custom function** takes a math formula of `v` for anything else (e.g. `v * 9/5 + 32` for °C → °F), with a live preview of sample values. Formulas are parsed as pure math (numbers, `+ - * / % ^ ( )`, `abs round floor ceil sqrt log log10 exp min max pow pi e`) and are never executed as code. Every numeric source value (states and statistics) is converted before import, and energy totals are spliced after conversion; for cumulative energy sensors keep the formula linear (`a*v + b`) so hourly deltas stay correct
8. If the destination holds data you know is **wrong**, enable **⚠️ Overwrite existing destination data** under Options. See [Overwrite mode](#overwrite-mode-destructive) below before using it
9. Click **Preview** to see exactly what would be imported (including the per-row debug JSON) without writing anything to the database
10. Click **Import History**
11. Review the results — each pair shows how many states and statistics were imported

### Overwrite mode (destructive)

By default the import never touches data the destination already has: it fills in front of the oldest good entry, and (with gap-fill) inside empty stretches. That is the right behavior almost always, but it cannot help when the destination's existing data is itself wrong: a new sensor that logged **zeros** while it was being commissioned alongside the old one, or an earlier import made with the **wrong unit**. Gap-fill treats a stored `0` as a real value, so those hours look covered and stay wrong.

**⚠️ Overwrite existing destination data** flips that rule: wherever the source has data, the source wins.

- Destination **state rows inside the source's time span are deleted** and replaced by the source's states, regardless of the gap-fill setting.
- Destination **statistics values are overwritten** (hourly and 5-minute) for every column the source provides. Columns the source does not provide keep their existing values.
- The recent slots Home Assistant may still be compiling are still skipped, and data **outside the source's time span is never touched**.
- For energy sensors, the splice offset is computed against the destination rows that survive, so the corrected series joins the good data instead of the values being replaced.

**Deleted state rows cannot be recovered without a database backup.** Back up first, run **Preview** to see the exact row counts and the time window that would be replaced, and confirm the window matches what you intend. The panel asks for a second confirmation, listing the destination entities, before anything is deleted.

Also available on the service call as `overwrite: true`.

> **Energy & cost are separate sensors.** In the Energy dashboard, a sensor's consumption and its *cost* are tracked by two different entities. If you migrate only the energy sensor, the new sensor's past **cost will show `0`**. To bring the cost history across too, add a **second pair** for the cost sensors (old cost → new cost). The same applies to any other derived sensor (e.g. compensated/return energy).

### Service call

You can also call the service directly from Developer Tools or automations:

```yaml
service: merge_sensor_history.import_history
data:
  source_entity_id: sensor.ecowitt_outdoor_temperature
  destination_entity_id: sensor.outdoor_temperature
```

## Important notes

- **Back up your database** before importing. The integration writes directly to the recorder database. This matters most with [Overwrite mode](#overwrite-mode-destructive), the one option that deletes existing rows.
- Only data **still in the recorder** can be imported. States are purged by default after ~10 days. Long-term statistics (hourly) are kept indefinitely.
- After importing, the new history will appear in the **History** panel. You may need to refresh the page or wait for the next recorder cycle.
- **The source entity is never modified or deleted.** The import only writes to the destination. Once you have verified the merge (compare the graphs, check the Energy dashboard), you can remove the old entity yourself: delete it in **Settings > Devices & Services > Entities** (or disable it if you prefer to keep it around), and optionally call the `recorder.purge_entities` service to drop its leftover data from the database.
- The import is a **one-time operation**, not a continuous sync. Run it once after setting up your new sensors.
- **Cost is a separate sensor from energy.** Pair the cost sensors too if you want their history (see the note under [Usage](#sidebar-panel)) — migrating only the energy sensor leaves past cost at `0`.
- **Energy sensors are stitched into one continuous series automatically.** When you import older energy history in front of a destination that *already has* statistics, the two cumulative `sum` series (old and new) start from different baselines. The integration lifts the destination's running total, its existing and future statistics, by a constant so the imported history and the existing data join seamlessly, using Home Assistant's own statistics-adjustment mechanism. The result is correct hourly, daily and lifetime energy totals, with no spike and no manual first-hour correction. Per-hour and per-day consumption values are unchanged by the lift (it is a constant offset), and the lift is preserved across restarts (Home Assistant re-reads it from the database). The import stays safe to re-run: a second run detects the series is already aligned and does nothing.
- **A brand-new destination keeps counting from the imported history.** Home Assistant continues a sensor's running total from the sensor's own most recent 5-minute statistics row, and starts again from zero when there is none. A destination that has never compiled statistics of its own, such as a helper or meter created for the import, would therefore restart at zero right after the imported history: the history would be there, but the meter would look like it reset. The import seeds the destination's running total at the end of the imported history so the new readings continue from it, and the import summary reports the value it seeded.
- **A destination that already restarted from zero can be repaired from the panel.** If an earlier import left the destination with its history intact but its own later rows counting up from zero, the import result says so and offers a **Repair running total** button. It lifts every statistics row from the restart point onwards by the total the series had reached, using Home Assistant's own statistics-adjustment mechanism, so the two halves line up and future readings continue from the corrected total. Rows before the restart point are untouched and per-hour and per-day figures do not change. Nothing is repaired unless you click it, and the offer only appears when the series shows a single unambiguous restart (a sensor whose total legitimately goes down, such as a bidirectional one, is not flagged).
- **Spikes that reappear after a restart are a separate sensor issue, not this integration.** Some energy sensors (solar inverters especially) briefly report `0` while Home Assistant restarts, for example during a HAOS or core update. Home Assistant then counts the jump from `0` back up to the real reading as an hour of consumption, which shows as a spike. This happens on every restart, with or without this integration, and no statistics adjustment can prevent it (the realignment above neither causes nor fixes it). The durable fix is at the source: make the sensor report `unavailable` (which Home Assistant ignores) instead of `0` during restarts, usually with a template sensor that has an `availability` condition, then point the Energy dashboard at that clean sensor. See the community write-ups on [energy dashboard spikes](https://community.home-assistant.io/t/data-spikes-in-the-energy-dashboard/469843).

### Very large imports

Home Assistant purges raw states after about 10 days by default, so most imports are well under a hundred thousand states and are written in one transaction in about a second.

SQLite, unlike PostgreSQL and MariaDB/MySQL, allows only one writer at a time, so on SQLite a very large import is the one case where a single transaction cannot work: holding the database for that long would stop the recorder writing, and a recorder that cannot write builds up a backlog that a restart would discard. The import measures its own speed as it goes and, if it can see it will not finish in time, rolls back before anything is committed and rewrites itself in batches instead. On a fast disk that threshold is somewhere above a quarter of a million states, and it scales down automatically on slower hardware.

A batched import is the one case that is not all-or-nothing, so it is written to be resumable instead: batches are ordered so that if one is interrupted, whatever landed is kept and re-running the same import continues from there. The import summary says when this happened. PostgreSQL and MariaDB/MySQL have row-level locking and are never batched.

## Requirements

- Home Assistant 2024.1.0 or newer
- The source entity must still have history in the recorder database

## Disclaimer

**Use this integration at your own risk.** Always create a full backup of your Home Assistant instance (including the database) before using this tool.

This integration manipulates internal Home Assistant recorder data (states and statistics tables) using internal APIs that are not part of Home Assistant's public API surface. These internals may change without notice in future Home Assistant releases, which could cause this integration to malfunction or produce unexpected results. The authors are not responsible for any data loss, corruption, or other issues arising from its use.

## License

MIT — see [LICENSE](LICENSE)
