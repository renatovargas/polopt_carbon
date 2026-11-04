# POLoPT Carbon

A command-line tool that prepares carbon storage products for the FAO PolOpT
project. It combines a land cover raster with Ruesch & Gibbs IPCC Tier 1 biomass
carbon values and a bundled copy of the FAO Global Ecological Zones, then writes:

- a carbon table by land cover class and carbon zone, which is the main product
- a carbon density map
- tables and a matching land cover raster for InVEST Carbon Storage, included for
  completeness

The carbon table uses each zone's own value, so it's more detailed than InVEST
results, which allow only one value per land cover class.

## Install

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then run:

```bash
uv tool install git+https://github.com/renatovargas/polopt_carbon
```

You don't need to install GDAL or conda separately. If you prefer conda, create
the environment in `environment.yml`, activate it, and run `pip install .` from a
clone of this repository.

## Quick start

Check your inputs first. This previews the results without writing any files:

```bash
polopt-carbon check lulc.tif boundary.shp
```

Then create the products:

```bash
polopt-carbon run lulc.tif boundary.shp --iso UGA --year 2023 -o out/uga
```

To keep your settings in a file, run `polopt-carbon init`, or copy
`config.example.yaml`, and then use:

```bash
polopt-carbon run --config uga.yaml
```

Command-line options override the settings file.

## Commands

| Command | What it does |
|---|---|
| `run` | Creates all the products |
| `check` | Checks inputs and previews results without writing files |
| `init` | Creates a settings file from a few questions |
| `coefficients` | Shows the value each class receives in a carbon zone |
| `compare` | Compares two output folders |

Run any command with `--help` to see its options. `run`, `check`, `coefficients`
and `compare` accept `--json`, which prints results on stdout for use in scripts.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Unexpected error, or `compare` found differences |
| 2 | Invalid command-line usage |
| 3 | Inputs or settings can't be used |
| 4 | The output file already exists; use `--overwrite` or another folder (`init`: `--force`) |

## Outputs

Files are named `{iso}_{year}_*`:

| File | Contents |
|---|---|
| `carbon_density.tif` | Carbon density in t C/ha, in EPSG:6933 |
| `carbon_table.xlsx` | The main product, with an `Aggregated` sheet and a `By polygon` sheet ([details](#carbon-table)) |
| `invest_carbon_table_dominant.csv` | InVEST table using each class's largest zone polygon |
| `invest_carbon_table_weighted.csv` | InVEST table using each class's pixel-weighted mean |
| `lulc_6933.tif` | Land cover on the same grid as the density map, for InVEST |
| `gez.gpkg` | Ecological zones clipped to the boundary |
| `manifest.json` | Settings, software versions, warnings and file hashes for the run |

### Carbon table

The file opens on the `Aggregated` sheet. It combines the rows in `By polygon` that
differ only by zone polygon, grouping by every column except these:

| Column | Combined with |
|---|---|
| `PIXEL_COUNT` | Sum |
| `CARBON_VALUE` | Maximum |
| `CARBON_VALUE_USED` | Maximum |
| `TOTAL_CARBON` | Sum |

Blank values are kept as groups, so savannas stay in the table even though they have
no GLC2000 class. Total carbon is the same on both sheets. `By polygon` remains the
first sheet, so scripts that read the first sheet get the same rows as before.

## Things to know about the results

- **Croplands and barren land receive 0 by default.** The lookup table has values
  for them (5 and 1 t C/ha), but in rows labeled `Global`, which the region-based
  join doesn't match. Runs warn about this. `--use-global-classes` applies these
  values, and also fixes a lookup typo that gives grassland and permanent wetlands
  0 in `Temperate oceanic forest` zones. It's off by default so that results
  reproduce earlier deliveries. For Uganda 2023 it adds 14.75 Mt C (1.4%).
- **`c_above` holds total biomass carbon.** Ruesch & Gibbs values combine
  aboveground and belowground carbon, so InVEST totals are correct but the split
  between pools isn't available.
- **InVEST tables have two columns by default.** Current InVEST versions read four
  pools. If yours rejects the table, use `--invest-all-pools` to add `c_below`,
  `c_soil` and `c_dead` as zeros. `c_above` doesn't change.

See the [method notes](https://renatovargas.github.io/polopt_carbon/method.html)
for details.

## Upgrading from 0.1

Results don't change. With the same inputs, 1.0 reproduces the 0.1 density map,
carbon table totals and InVEST tables. To confirm this for a country you've already
delivered, run:

```bash
polopt-carbon compare path/to/delivered_outputs path/to/new_outputs
```

Existing config files keep working, and entries that are no longer used produce a
warning. These behaviors did change:

- `run` no longer overwrites existing outputs unless you pass `--overwrite` or set
  `project.overwrite: true`.
- `--method` and `--force-wetland-overrides` are gone. They had no effect, and both
  InVEST tables are always written.
- `validate` is replaced by `check`.
- The `rules` section of a config file now takes effect. It used to be ignored, so
  check any config that sets non-default values.
- The density map now declares NaN as nodata, and is compressed.
- The carbon table has a new final column, `VALUE_SOURCE`, and a new `Aggregated`
  sheet. The first sheet is now named `By polygon`.

## Development

```bash
uv sync --group dev
uv run --group dev pytest
uv run --group dev ruff check .
```

`tests/golden/` holds output captured from the validated 0.1 code, and tests fail
if results change. If `testdata/uga/` is present, which it isn't in the public
repository, the tests also compare against delivered Uganda outputs.

`scripts/process_rg_lookup*.py` are kept as a record of how the lookup table was
built once. The table isn't meant to be regenerated.

To run the tool from QGIS, use the
[POLoPT Carbon plugin](https://github.com/renatovargas/polopt_carbon_qgis).

## License

The code is under the [MIT License](LICENSE). The bundled ecological zone and
coefficient data come from third parties and keep their own terms.
