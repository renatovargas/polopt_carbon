from pathlib import Path
import os
import json
import logging
import typer
import yaml

from polopt_carbon.core import compute
from polopt_carbon.validate import run_validation
from polopt_carbon.rules import apply_fallback_rules


app = typer.Typer(help="Compute carbon coefficients from LULC and carbon-zone layers.")

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def setup_logging(verbose: bool):
    """Basic console logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Path expansion helper
# ---------------------------------------------------------------------------


def expand_path(path: Path | None) -> Path | None:
    """Expand ~ and environment variables in paths."""
    if path is None:
        return None
    path_str = str(path)
    if not path_str.strip():
        return None
    return Path(os.path.expanduser(os.path.expandvars(path_str)))


# ---------------------------------------------------------------------------
# Main 'run' command
# ---------------------------------------------------------------------------


@app.command()
def run(
    country: str = typer.Option(None, help="ISO3 code, overrides config (e.g., UGA)"),
    lulc: Path = typer.Option(
        None, exists=False, help="LULC raster (overrides config)"
    ),
    carbon_zones: Path = typer.Option(
        None, exists=False, help="Carbon zones vector file (overrides config)"
    ),
    boundary: Path = typer.Option(
        None, exists=False, help="Country boundary vector file (overrides config)"
    ),
    out: Path = typer.Option(None, help="Output table (CSV or Parquet)"),
    out_gpkg: Path = typer.Option(None, help="Optional GeoPackage for map outputs"),
    overwrite: bool = typer.Option(False, help="Overwrite existing outputs"),
    config: Path = typer.Option(
        None, exists=True, help="Path to YAML configuration file"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logs"),
):
    """
    Run the full carbon-coefficient pipeline using CLI arguments or a YAML configuration.
    """
    setup_logging(verbose)

    # -----------------------------------------------------------------------
    # Load configuration file (if provided)
    # -----------------------------------------------------------------------
    cfg = {}
    if config:
        logging.info(f"Loading configuration from {config}")
        with open(config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    # -----------------------------------------------------------------------
    # Resolve parameters: CLI options override YAML
    # -----------------------------------------------------------------------
    country = country or cfg.get("project", {}).get("country")
    lulc = lulc or Path(cfg.get("inputs", {}).get("lulc", ""))
    carbon_zones = carbon_zones or Path(cfg.get("inputs", {}).get("carbon_zones", ""))
    boundary = boundary or Path(cfg.get("inputs", {}).get("boundary", ""))
    out = out or Path(cfg.get("outputs", {}).get("table", "out/coefficients.csv"))
    out_gpkg = out_gpkg or Path(
        cfg.get("outputs", {}).get("geopackage", "out/outputs.gpkg")
    )
    overwrite = overwrite or cfg.get("project", {}).get("overwrite", False)
    rule_config = cfg.get("rules", {})

    # New parameters for coefficient lookup and InVEST table output
    coeff_lookup = Path(cfg.get("inputs", {}).get("coeff_lookup", "")) if cfg else None
    invest_table_out = (
        Path(cfg.get("outputs", {}).get("invest_table", "")) if cfg else None
    )

    # -----------------------------------------------------------------------
    # Expand ~ and environment variables in all paths
    # -----------------------------------------------------------------------
    lulc = expand_path(lulc)
    carbon_zones = expand_path(carbon_zones)
    boundary = expand_path(boundary)
    out = expand_path(out)
    out_gpkg = expand_path(out_gpkg)
    coeff_lookup = expand_path(coeff_lookup)
    invest_table_out = expand_path(invest_table_out)

    logging.info(f"Starting POLoPT Carbon run for {country}")
    logging.debug(f"LULC raster: {lulc}")
    logging.debug(f"Carbon zones: {carbon_zones}")
    logging.debug(f"Boundary: {boundary}")
    logging.debug(f"Coefficient lookup: {coeff_lookup}")
    logging.debug(f"InVEST table output: {invest_table_out}")

    # -----------------------------------------------------------------------
    # Validate inputs first
    # -----------------------------------------------------------------------
    validation = run_validation(lulc, carbon_zones, boundary)
    typer.echo(json.dumps(validation, indent=2))

    # Abort if any critical validation failed
    if any("not found" in str(v) for v in validation.values()):
        logging.error("Validation failed — aborting.")
        raise typer.Exit(code=1)

    # -----------------------------------------------------------------------
    # Execute processing pipeline
    # -----------------------------------------------------------------------
    result = compute(
        country=country,
        lulc=lulc,
        zones=carbon_zones,
        boundary=boundary,
        out=out,
        out_gpkg=out_gpkg,
        overwrite=overwrite,
        coeff_lookup=coeff_lookup,
        invest_table_out=invest_table_out,
    )

    # -----------------------------------------------------------------------
    # Final logging and summary
    # -----------------------------------------------------------------------
    typer.echo(json.dumps(result, indent=2))
    logging.info("Run complete.")


# ---------------------------------------------------------------------------
# Validation-only command
# ---------------------------------------------------------------------------


@app.command()
def validate_inputs(
    lulc: Path = typer.Option(..., exists=True, help="Path to LULC raster"),
    carbon_zones: Path = typer.Option(
        ..., exists=True, help="Path to carbon zones vector"
    ),
    boundary: Path = typer.Option(..., exists=True, help="Path to country boundary"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    """
    Validate input files (CRS, projection, required fields).
    """
    setup_logging(verbose)
    logging.info("Validating inputs...")

    results = run_validation(lulc, carbon_zones, boundary)
    typer.echo(json.dumps(results, indent=2))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    """Entry point for setuptools (via pyproject.toml)."""
    app()


if __name__ == "__main__":
    main()
