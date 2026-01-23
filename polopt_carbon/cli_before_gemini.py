from pathlib import Path
import os
import json
import logging
import typer
import yaml

from polopt_carbon.core import compute
from polopt_carbon.validate import run_validation
from polopt_carbon.rules import apply_fallback_rules

# ---------------------------------------------------------------------------
# Typer app (concise banner)
# ---------------------------------------------------------------------------

app = typer.Typer(
    help="Compute carbon coefficients from LULC and carbon-zone layers for FAO's Policy Optimization Tool (PolOpT)"
)

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


@app.command(
    help=(
        "Run the full carbon-coefficient pipeline using CLI arguments or a YAML configuration."
    )
)
def run(
    country: str = typer.Option(None, help="ISO3 code, overrides config (e.g., UGA)"),
    lulc: Path = typer.Option(None, help="LULC raster (overrides config)"),
    carbon_zones: Path = typer.Option(None, help="Carbon zones vector file"),
    boundary: Path = typer.Option(None, help="Country boundary vector file"),
    out: Path = typer.Option(None, help="Output table (CSV or Parquet)"),
    out_gpkg: Path = typer.Option(None, help="Optional GeoPackage for map outputs"),
    overwrite: bool = typer.Option(False, help="Overwrite existing outputs"),
    config: Path = typer.Option(
        None, exists=True, help="Path to YAML configuration file"
    ),
    method: str = typer.Option(
        "dominant",
        help="Final coefficient method: 'dominant' (most pixels) or 'weighted' (average by count).",
        case_sensitive=False,
    ),
    expert_rules: Path = typer.Option(
        None,
        help="Optional CSV with [lucode, c_above_override] to override final coefficients.",
    ),
    force_wetland_overrides: bool = typer.Option(
        False,
        "--force-wetland-overrides",
        help="Force wetland overrides even when R&G values exist (Onil-style GEZ logic).",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logs"),
):
    """Run the full carbon-coefficient pipeline."""
    setup_logging(verbose)

    # Load config
    cfg = {}
    if config:
        logging.info(f"Loading configuration from {config}")
        with open(config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    # Resolve parameters
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
    rule_config["force_wetland_overrides"] = force_wetland_overrides

    coeff_lookup = Path(cfg.get("inputs", {}).get("coeff_lookup", "")) if cfg else None
    invest_table_out = (
        Path(cfg.get("outputs", {}).get("invest_table", "")) if cfg else None
    )

    # Expand paths
    lulc = expand_path(lulc)
    carbon_zones = expand_path(carbon_zones)
    boundary = expand_path(boundary)
    out = expand_path(out)
    out_gpkg = expand_path(out_gpkg)
    coeff_lookup = expand_path(coeff_lookup)
    invest_table_out = expand_path(invest_table_out)
    expert_rules = expand_path(expert_rules)

    logging.info(f"Starting POLoPT Carbon run for {country}")
    logging.debug(f"Method: {method}")
    logging.debug(f"Expert rules: {expert_rules}")
    logging.debug(f"Force wetland overrides: {force_wetland_overrides}")

    # Validate
    validation = run_validation(lulc, carbon_zones, boundary)
    typer.echo(json.dumps(validation, indent=2))
    if any("not found" in str(v) for v in validation.values()):
        logging.error("Validation failed — aborting.")
        raise typer.Exit(code=1)

    # Run compute
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
        method=method,
        expert_rules=expert_rules,
        force_wetland_overrides=force_wetland_overrides,  # ✅ Now passed correctly
    )

    typer.echo(json.dumps(result, indent=2))
    logging.info("Run complete.")


# ---------------------------------------------------------------------------
# Validation-only command
# ---------------------------------------------------------------------------


@app.command(help="Validate input files (CRS, projection, required fields).")
def validate_inputs(
    lulc: Path = typer.Option(..., exists=True, help="Path to LULC raster"),
    carbon_zones: Path = typer.Option(
        ..., exists=True, help="Path to carbon zones vector"
    ),
    boundary: Path = typer.Option(..., exists=True, help="Path to country boundary"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
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
