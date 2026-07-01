# HA Merge Sensor History

A Home Assistant custom component to import historical sensor data from one entity into another.

Built for migrating sensor data between integrations — for example, when replacing an Ecowitt integration with a different one and you want the new sensors to carry the old sensors' history.

## Features

- **Sidebar panel** with a simple UI: select source/destination pairs, click Import
- **Imports both states and long-term statistics** (hourly aggregates for energy dashboard / long-term graphs)
- **Atomic transactions**: either all states are imported or none are — no partial imports that leave gaps
- **Idempotent**: safe to re-run; a successful import shifts the cutoff so nothing is re-imported, and a failed import is fully rolled back
- **Entity filter** to quickly find sensors by keyword
- **Also available as a service** (`merge_sensor_history.import_history`) for use in automations or Developer Tools
- **HACS compatible**

## How it works

1. Reads **all** historical states from the source entity via the recorder API
2. Queries the destination entity's **oldest existing entry** (inside the same DB transaction)
3. Imports only source states that are **strictly older** than that oldest entry — this prevents any overlap or duplication
4. Imports **long-term statistics** (hourly mean/min/max/sum) via the official `async_import_statistics` API, which is inherently deduplicated by the database schema
5. Commits everything in a **single transaction** — if anything fails, the entire import is rolled back and you can safely retry

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
4. Use **+ Add Pair** to queue multiple imports at once
5. Use the **filter** field to narrow down entities by keyword (e.g., `ecowitt`, `temperature`)
6. Click **Import History**
7. Review the results — each pair shows how many states and statistics were imported

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

- **Back up your database** before importing. The integration writes directly to the recorder database.
- Only data **still in the recorder** can be imported. States are purged by default after ~10 days. Long-term statistics (hourly) are kept indefinitely.
- After importing, the new history will appear in the **History** panel. You may need to refresh the page or wait for the next recorder cycle.
- The import is a **one-time operation**, not a continuous sync. Run it once after setting up your new sensors.
- **Cost is a separate sensor from energy.** Pair the cost sensors too if you want their history (see the note under [Usage](#sidebar-panel)) — migrating only the energy sensor leaves past cost at `0`.
- **Energy sensors — the very first imported hour.** When you import older energy history in front of a destination that *already has* statistics, the integration shifts the imported cumulative `sum` so it lines up with your existing data at the boundary (no jump at the splice point). Home Assistant measures each hour's energy as the difference between consecutive cumulative totals and anchors the very first hour against zero — and it does **no** meter-reset detection when reading stored statistics — so the single **oldest imported hour** ends up absorbing that alignment offset. Your **hourly and daily energy graphs are unaffected** (Home Assistant clamps that one hour to zero), but the **all-time total in the Energy _sources table_** can be off by the offset amount. If you want a perfect lifetime total, correct that one hour under **Developer Tools → Statistics** (use *Adjust a statistic* / *Fix* on the first data point). Importing into a brand-new sensor *before* it has compiled any statistics avoids this entirely, since there is nothing to align against.

## Requirements

- Home Assistant 2024.1.0 or newer
- The source entity must still have history in the recorder database

## Disclaimer

**Use this integration at your own risk.** Always create a full backup of your Home Assistant instance (including the database) before using this tool.

This integration manipulates internal Home Assistant recorder data (states and statistics tables) using internal APIs that are not part of Home Assistant's public API surface. These internals may change without notice in future Home Assistant releases, which could cause this integration to malfunction or produce unexpected results. The authors are not responsible for any data loss, corruption, or other issues arising from its use.

## License

MIT — see [LICENSE](LICENSE)
