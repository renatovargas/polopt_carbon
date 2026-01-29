import logging
import json
from pathlib import Path
import typer
import yaml

from polopt_carbon.core import compute
from polopt_carbon.validate import run_validation

app = typer.Typer(help="POLoPT Carbon CLI: Compute carbon coefficients and maps.")


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def expand(path: any) -> Path | None:
    if path is None or str(path).strip() == "":
        return None
    return Path(path).expanduser().resolve()


@app.command(help="Run the full carbon-coefficient and mapping pipeline.")
def run(
    config: Path = typer.Option(
        None, "--config", "-c", exists=True, help="Path to YAML configuration file"
    ),
    country: str = typer.Option(None, help="ISO3 code (overrides config)"),
    year: int = typer.Option(None, help="Year of analysis (overrides config)"),
    method: str = typer.Option("dominant", help="Method: 'dominant' or 'weighted'"),
    force_wetland_overrides: bool = typer.Option(False, help="Force wetland GEZ logic"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logs"),
):
    setup_logging(verbose)
    cfg = {}
    if config:
        with open(config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    proj = cfg.get("project", {})
    inp = cfg.get("inputs", {})
    out_cfg = cfg.get("outputs", {})
    rules = cfg.get("rules", {})

    country = country or proj.get("country", "ISO")
    # Priority: CLI arg > Config 'year' > None (Core defaults to current year)
    year_val = year or proj.get("year")

    lulc_path = expand(inp.get("lulc"))
    boundary_path = expand(inp.get("boundary"))
    output_dir = expand(out_cfg.get("folder", "out"))

    # Optional Overrides
    coeff_lookup = expand(inp.get("coeff_lookup"))
    crosswalk = expand(inp.get("crosswalk"))
    expert = expand(inp.get("expert_rules"))

    logging.info(f"Starting POLoPT Carbon run for {country} (Year: {year_val})")

    # Internalized GEZ: pass None to validation for the zones parameter
    v_results = run_validation(lulc_path, None, boundary_path)
    if any("not found" in str(v).lower() for k, v in v_results.items() if k != "zones"):
        logging.error(f"Validation failed: {json.dumps(v_results, indent=2)}")
        raise typer.Exit(code=1)

    result = compute(
        country=country,
        lulc=lulc_path,
        boundary=boundary_path,
        output_dir=output_dir,
        overwrite=proj.get("overwrite", False),
        coeff_lookup=coeff_lookup,
        crosswalk_path=crosswalk,
        method=method,
        expert_rules=expert,
        force_wetland_overrides=force_wetland_overrides
        or rules.get("force_wetland_overrides", False),
        year=year_val,
    )

    typer.echo(json.dumps(result, indent=2))


@app.command(help="Validate that the LULC and Boundary layers exist and are readable.")
def validate(
    lulc: Path = typer.Argument(..., help="Path to LULC raster"),
    boundary: Path = typer.Argument(..., help="Path to Boundary shapefile/geopackage"),
):
    """Explicitly validate user-supplied layers."""
    results = run_validation(expand(lulc), None, expand(boundary))
    # We remove the 'zones' key from the output to avoid confusing the user
    results.pop("zones", None)
    typer.echo(json.dumps(results, indent=2))


def main():
    app()


if __name__ == "__main__":
    main()
