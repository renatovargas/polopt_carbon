Here is the complete `README.md` formatted specifically for your project. I’ve refined the **Configuration** section to match the specific logic found in your `cli.py` and `core.py`.

```markdown
# POLoPT Carbon CLI

A specialized command-line tool to compute carbon coefficients and generate carbon density maps. It works by intersecting Land Use/Land Cover (LULC) rasters with Carbon Zone vector layers, applying expert-validated rules to determine carbon sequestration potential.

---

## 🛠 Prerequisites

This project relies on the **Geospatial Data Abstraction Library (GDAL)** and other C-based libraries (`PROJ`, `GEOS`). 

### Recommended: Conda / Micromamba
To avoid manual installation of complex system dependencies, use the provided `environment.yml` file:

```bash
# Create the environment
conda env create -f environment.yml

# Activate it
conda activate polopt-carbon

```

---

## 🚀 Installation

Once your environment is set up, you can install the package using the Wheel file or directly from the source.

### From a Wheel file

```bash
pip install polopt_carbon-0.1.0-py3-none-any.whl

```

### From Source (Development mode)

```bash
git clone [https://github.com/your-username/polopt_carbon.git](https://github.com/your-username/polopt_carbon.git)
cd polopt_carbon
pip install -e .

```

---

## 📖 Usage

The tool is accessible via the `polopt-carbon` command.

### 1. Run the Pipeline

The `run` command executes the full intersection and mapping process.

```bash
polopt-carbon run --country GTM --config config.yaml --method dominant

```

**Key Options:**

* `--config`: Path to your YAML configuration file.
* `--country`: ISO3 code for the project area (overrides config).
* `--method`: Choose between `dominant` (takes the most frequent class) or `weighted` (takes an average). Default is `dominant`.
* `--force-wetland-overrides`: Force specific GEZ logic for wetland areas.
* `-v, --verbose`: Enable detailed logging.

### 2. Validate Inputs

Check your datasets for compatibility before running the full pipeline:

```bash
polopt-carbon validate --lulc path/to/lulc.tif --zones path/to/zones.shp --boundary path/to/bnd.shp

```

---

## 📁 Configuration File (`config.yaml`)

Your configuration file organizes paths and project metadata. Below is a standard template:

```yaml
project:
  country: "GTM"
  method: "dominant"
  overwrite: true

paths:
  # Mandatory Inputs
  lulc: "data/input/lulc_2020.tif"
  zones: "data/input/carbon_zones.shp"
  boundary: "data/input/national_boundary.shp"
  
  # Optional Lookups
  coeff_lookup: "data/lookups/custom_coeffs.csv"
  expert_rules: "data/lookups/rules.json"

outputs:
  table: "out/carbon_results.csv"
  geopackage: "out/spatial_results.gpkg"
  invest_table: "out/invest_format.csv"

```

---

## 🔬 Core Logic Note

* **Projection:** All data is automatically reprojected to **EPSG:6933** (WGS 84 / NSIDC EASE-Grid 2.0 Global) to ensure accurate equal-area calculations for carbon density ().
* **Bundled Data:** This tool includes a built-in MODIS GLC crosswalk used for automatic class mapping.

---

## 📄 License

MIT License

