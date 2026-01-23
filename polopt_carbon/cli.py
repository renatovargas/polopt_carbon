import logging
import json
from pathlib import Path
import typer
import yaml

from polopt_carbon.core import compute
from polopt_carbon.validate import run_validation

app = typer.Typer(help="POLoPT Carbon CLI: Compute carbon coefficients and maps.")


def setup_logging(verbose: bool):
    """Basic console logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def expand(path: any) -> Path | None:
    """Expand user home (~) and resolve paths."""
    if path is None or str(path).strip() == "":
        return None
    return Path(path).expanduser().resolve()


@app.command(help="Run the full carbon-coefficient and mapping pipeline.")
def run(
    config: Path = typer.Option(
        None, "--config", "-c", exists=True, help="Path to YAML configuration file"
    ),
    country: str = typer.Option(None, help="ISO3 code (overrides config)"),
    method: str = typer.Option("dominant", help="Method: 'dominant' or 'weighted'"),
    force_wetland_overrides: bool = typer.Option(False, help="Force wetland GEZ logic"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logs"),
):
    setup_logging(verbose)

    # 1. Load configuration from YAML
    cfg = {}
    if config:
        logging.info(f"Loading config from {config}")
        with open(config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    # 2. Extract and resolve parameters (CLI overrides Config)
    proj = cfg.get("project", {})
    inp = cfg.get("inputs", {})
    out_cfg = cfg.get("outputs", {})
    rules = cfg.get("rules", {})

    country = country or proj.get("country", "ISO")

    # Inputs
    lulc_path = expand(inp.get("lulc"))
    zones_path = expand(inp.get("carbon_zones"))
    boundary_path = expand(inp.get("boundary"))

    # Optional Data Overrides
    coeff_lookup = expand(inp.get("coeff_lookup"))
    crosswalk = expand(inp.get("crosswalk"))
    expert = expand(inp.get("expert_rules"))

    # Outputs - Updated to use hardcoded logic: only needs the folder path
    output_dir = expand(out_cfg.get("folder", "out"))

    # 3. Validation
    logging.info(f"Starting POLoPT Carbon run for {country}")
    v_results = run_validation(lulc_path, zones_path, boundary_path)
    if any("not found" in str(v).lower() for v in v_results.values()):
        logging.error(f"Validation failed: {json.dumps(v_results, indent=2)}")
        raise typer.Exit(code=1)

    # 4. Execute Core Logic
    # Updated to pass output_dir instead of specific file paths to match core.py
    result = compute(
        country=country,
        lulc=lulc_path,
        zones=zones_path,
        boundary=boundary_path,
        output_dir=output_dir,
        overwrite=proj.get("overwrite", False),
        coeff_lookup=coeff_lookup,
        crosswalk_path=crosswalk,
        method=method,
        expert_rules=expert,
        force_wetland_overrides=force_wetland_overrides
        or rules.get("force_wetland_overrides", False),
    )

    # 5. Output Summary
    typer.echo(json.dumps(result, indent=2))
    logging.info("Run complete.")


@app.command()
def validate(lulc: Path, zones: Path, boundary: Path):
    """Validate input data quality and paths."""
    typer.echo(
        json.dumps(
            run_validation(expand(lulc), expand(zones), expand(boundary)), indent=2
        )
    )


def main():
    app()


if __name__ == "__main__":
    main()
