# POLoPT Carbon CLI

A command-line tool for computing carbon coefficients and density maps.

## Step 1: Prepare your Environment
This tool requires geospatial C-libraries (GDAL, PROJ). We strongly recommend using **Micromamba** or **Conda**.

1. **Clone the repository:**

```bash
   git clone [https://github.com/renatovargas/polopt_carbon.git](https://github.com/renatovargas/polopt_carbon.git)
   cd polopt_carbon
```

2. **Create the environment:**

```bash
micromamba env create -f environment.yml
micromamba activate polopt-carbon
```

## Step 2: Installation

### Option A: Install from a Release (Fastest)

Go to the [Releases](https://github.com/renatovargas/polopt_carbon/releases) page, download the latest `.whl` file, and run:

```bash
pip install polopt_carbon-0.1.0-py3-none-any.whl

```

### Option B: Install for Development

If you want to modify the code, run this from the project root:

```bash
pip install -e .

```

## Step 3: Running an Analysis

The tool uses a `config.yaml` file to locate your rasters and shapefiles. 

1. **Configure:** Open `config.yaml` and set the paths to your LULC, Carbon Zones, and Boundary files. 


2. **Execute:**

```bash
polopt-carbon run --config config.yaml

```

**Note:** All calculations are performed in **EPSG:6933** for accurate equal-area carbon density results.

