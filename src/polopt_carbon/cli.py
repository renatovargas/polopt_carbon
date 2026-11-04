"""The polopt-carbon command-line interface."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Annotated, Any

import typer
import yaml
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table

from polopt_carbon import __version__
from polopt_carbon.config import PoloptError, build_config

app = typer.Typer(
    name="polopt-carbon",
    help="Prepare carbon density maps and InVEST carbon tables from land cover data.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
    rich_markup_mode="rich",
)
# Progress and messages go to stderr, leaving stdout for --json results.
console = Console(stderr=True)
output = Console()

LulcArg = Annotated[Path | None, typer.Argument(help="Land cover raster.")]
BoundaryArg = Annotated[Path | None, typer.Argument(help="Boundary vector file.")]
ConfigOpt = Annotated[
    Path | None,
    typer.Option(
        "--config", "-c", help="YAML settings file. Options here override it."
    ),
]
IsoOpt = Annotated[
    str | None, typer.Option("--iso", "--country", help="ISO3 country code.")
]
YearOpt = Annotated[int | None, typer.Option(help="Year of the land cover data.")]
GlobalOpt = Annotated[
    bool,
    typer.Option(
        "--use-global-classes",
        help="Apply the lookup's Global rows and fix its label typo. Changes results.",
    ),
]
JsonOpt = Annotated[
    bool, typer.Option("--json", help="Print results as JSON on stdout.")
]
VerboseOpt = Annotated[
    bool, typer.Option("--verbose", "-v", help="Show detailed logs and tracebacks.")
]

_verbose = False


def _setup_logging(verbose: bool) -> None:
    global _verbose
    _verbose = verbose
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(message)s",
        handlers=[RichHandler(console=console, show_path=False)],
        force=True,
    )


def _fail(exc: Exception, as_json: bool = False) -> typer.Exit:
    code = exc.exit_code if isinstance(exc, PoloptError) else 1
    if as_json:
        output.print_json(json.dumps({"status": "error", "error": str(exc)}))
    else:
        console.print(f"[bold red]Error:[/] {exc}", highlight=False)
    if _verbose or not isinstance(exc, PoloptError):
        console.print_exception(show_locals=False)
    return typer.Exit(code=code)


def _stage_runner(progress: Progress, task: Any):
    started = [False]

    def on_stage(name: str) -> None:
        if started[0]:
            progress.advance(task)
        started[0] = True
        progress.update(task, description=name)

    return on_stage


def _progress(enabled: bool) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=True,
        disable=not enabled,
    )


def _print_warnings(warnings: list[str]) -> None:
    for message in warnings:
        console.print(f"[yellow]Warning:[/] {message}", highlight=False)


def _summary_table(table: Any) -> Table:
    from polopt_carbon.pipeline import class_summary

    view = Table(title="Carbon by land cover class", header_style="bold")
    view.add_column("Code", justify="right")
    view.add_column("Class")
    view.add_column("Area (ha)", justify="right")
    view.add_column("Carbon (t C)", justify="right")
    view.add_column("Mean t C/ha", justify="right")
    view.add_column("Value source")
    summary = class_summary(table)
    for row in summary.itertuples():
        view.add_row(
            str(row.LULC),
            row.LULC_CLASS,
            f"{row.area_ha:,.0f}",
            f"{row.carbon:,.0f}",
            f"{row.density:,.1f}",
            row.sources,
        )
    view.add_section()
    view.add_row(
        "",
        "Total",
        f"{summary['area_ha'].sum():,.0f}",
        f"{summary['carbon'].sum():,.0f}",
        f"{summary['carbon'].sum() / summary['area_ha'].sum():,.1f}",
        "",
        style="bold",
    )
    return view


def _source_pixels(table: Any) -> dict[str, int]:
    pixels = table.groupby("VALUE_SOURCE")["Count"].sum()
    return {source: int(count) for source, count in pixels.items()}


@app.callback(invoke_without_command=True)
def _root(
    version: Annotated[
        bool, typer.Option("--version", help="Show the version and exit.")
    ] = False,
) -> None:
    if version:
        typer.echo(f"polopt-carbon {__version__}")
        raise typer.Exit()


@app.command()
def run(
    lulc: LulcArg = None,
    boundary: BoundaryArg = None,
    config: ConfigOpt = None,
    iso: IsoOpt = None,
    year: YearOpt = None,
    out: Annotated[
        Path | None, typer.Option("--out", "-o", help="Output folder.")
    ] = None,
    overwrite: Annotated[
        bool, typer.Option("--overwrite", help="Replace existing output files.")
    ] = False,
    use_global_classes: GlobalOpt = False,
    invest_all_pools: Annotated[
        bool,
        typer.Option(
            "--invest-all-pools",
            help="Add zero-filled c_below, c_soil and c_dead columns to InVEST tables.",
        ),
    ] = False,
    as_json: JsonOpt = False,
    verbose: VerboseOpt = False,
) -> None:
    """Create the carbon map, carbon table and InVEST tables."""
    _setup_logging(verbose)
    from polopt_carbon.pipeline import ANALYSIS_STAGES, WRITE_STAGES
    from polopt_carbon.pipeline import run as run_pipeline

    try:
        cfg = build_config(
            config_path=config,
            lulc=lulc,
            boundary=boundary,
            iso=iso,
            year=year,
            output_dir=out,
            overwrite=overwrite,
            use_global_classes=use_global_classes,
            invest_all_pools=invest_all_pools,
        )
        stages = len(ANALYSIS_STAGES) + len(WRITE_STAGES)
        with _progress(not as_json) as progress:
            task = progress.add_task("Starting", total=stages)
            result = run_pipeline(cfg, _stage_runner(progress, task))
    except Exception as exc:
        raise _fail(exc, as_json) from exc

    analysis = result.analysis
    if as_json:
        output.print_json(
            json.dumps(
                {
                    "status": "success",
                    "iso": cfg.iso,
                    "year": cfg.year,
                    "total_carbon_mg": analysis.total_carbon,
                    "value_source_pixels": _source_pixels(analysis.table),
                    "warnings": analysis.warnings,
                    "outputs": {k: str(v) for k, v in result.outputs.items()},
                    "manifest": str(result.manifest),
                }
            )
        )
        return

    console.print(_summary_table(analysis.table))
    _print_warnings(analysis.warnings)
    console.print(
        Panel(
            f"[bold]{analysis.total_carbon / 1e6:,.2f} Mt C[/] for {cfg.iso} {cfg.year}\n"
            f"Files written to [cyan]{cfg.output_dir}[/]",
            title="Done",
            border_style="green",
            expand=False,
        ),
        highlight=False,
    )


@app.command()
def check(
    lulc: LulcArg = None,
    boundary: BoundaryArg = None,
    config: ConfigOpt = None,
    iso: IsoOpt = None,
    year: YearOpt = None,
    use_global_classes: GlobalOpt = False,
    as_json: JsonOpt = False,
    verbose: VerboseOpt = False,
) -> None:
    """Check inputs and preview results without writing any files."""
    _setup_logging(verbose)
    from polopt_carbon.pipeline import ANALYSIS_STAGES, analyze, check_inputs

    try:
        cfg = build_config(
            config_path=config,
            lulc=lulc,
            boundary=boundary,
            iso=iso,
            year=year,
            use_global_classes=use_global_classes,
            require_identity=False,
        )
        checks = check_inputs(cfg)
        failed = [c for c in checks if not c.passed]
        if failed:
            if as_json:
                output.print_json(
                    json.dumps(
                        {
                            "status": "failed",
                            "checks": [c.__dict__ for c in checks],
                        }
                    )
                )
            else:
                _print_checks(checks)
                console.print("[bold red]Inputs failed the check.[/]")
            raise typer.Exit(code=3)
        with _progress(not as_json) as progress:
            task = progress.add_task("Starting", total=len(ANALYSIS_STAGES))
            analysis = analyze(cfg, _stage_runner(progress, task))
    except typer.Exit:
        raise
    except Exception as exc:
        raise _fail(exc, as_json) from exc

    if as_json:
        output.print_json(
            json.dumps(
                {
                    "status": "ok",
                    "checks": [c.__dict__ for c in checks],
                    "total_carbon_mg": analysis.total_carbon,
                    "value_source_pixels": _source_pixels(analysis.table),
                    "warnings": analysis.warnings,
                }
            )
        )
        return

    _print_checks(checks)
    console.print(_summary_table(analysis.table))
    _print_warnings(analysis.warnings)
    console.print(
        f"[bold green]Ready to run.[/] Preview total: "
        f"{analysis.total_carbon / 1e6:,.2f} Mt C. No files were written.",
        highlight=False,
    )


def _print_checks(checks: list[Any]) -> None:
    for item in checks:
        mark = "[green]OK[/]" if item.passed else "[red]FAIL[/]"
        console.print(f"{mark} {item.name}: {item.detail}", highlight=False)


@app.command()
def init(
    path: Annotated[Path, typer.Argument(help="Settings file to create.")] = Path(
        "polopt.yaml"
    ),
    force: Annotated[
        bool, typer.Option("--force", help="Replace the file if it exists.")
    ] = False,
) -> None:
    """Create a settings file by answering a few questions."""

    def existing_file(label: str) -> str:
        while True:
            value = Prompt.ask(label, console=console).strip()
            if Path(value).expanduser().is_file():
                return value
            console.print(f"[red]File not found:[/] {value}", highlight=False)

    if path.exists() and not force:
        console.print(
            f"[bold red]Error:[/] {path} already exists. Use --force to replace it.",
            highlight=False,
        )
        raise typer.Exit(code=4)

    iso = ""
    while len(iso) != 3 or not iso.isalpha():
        iso = Prompt.ask("ISO3 country code", console=console).strip().upper()
    year = IntPrompt.ask("Year of the land cover data", console=console)
    lulc = existing_file("Land cover raster")
    boundary = existing_file("Boundary file")
    folder = Prompt.ask("Output folder", default=f"out/{iso.lower()}", console=console)
    use_global = Confirm.ask(
        "Apply the lookup's Global rows? This changes results from the validated "
        "default method",
        default=False,
        console=console,
    )
    settings = {
        "project": {"country": iso, "year": year, "overwrite": False},
        "inputs": {"lulc": lulc, "boundary": boundary},
        "outputs": {"folder": folder},
        "options": {"use_global_classes": use_global, "invest_all_pools": False},
    }
    path.write_text(yaml.safe_dump(settings, sort_keys=False), encoding="utf-8")
    console.print(
        f"[green]Created {path}.[/] Next, run: polopt-carbon check --config {path}",
        highlight=False,
    )


@app.command()
def coefficients(
    region: Annotated[
        str | None, typer.Option(help="Region, for example Africa.")
    ] = None,
    gez: Annotated[
        str | None,
        typer.Option(help="GEZ term, for example 'Tropical rainforest'."),
    ] = None,
    frontier: Annotated[
        int, typer.Option(min=0, max=1, help="Frontier forest: 0 or 1.")
    ] = 0,
    use_global_classes: GlobalOpt = False,
    as_json: JsonOpt = False,
) -> None:
    """Show the value each land cover class receives in a carbon zone."""
    from polopt_carbon.coefficients import (
        coefficients_for_zone,
        load_lookup,
        zone_options,
    )

    if not region or not gez:
        regions, terms = zone_options(load_lookup())
        if as_json:
            output.print_json(json.dumps({"regions": regions, "gez_terms": terms}))
            return
        console.print("[bold]Regions:[/] " + ", ".join(regions), highlight=False)
        console.print("[bold]GEZ terms:[/] " + ", ".join(terms), highlight=False)
        console.print("\nChoose one of each with --region and --gez.")
        return

    try:
        rows = coefficients_for_zone(region, gez, frontier, use_global_classes)
    except Exception as exc:
        raise _fail(exc, as_json) from exc

    if as_json:
        output.print_json(rows.to_json(orient="records"))
        return
    view = Table(title=f"{gez}, {region}, frontier {frontier}", header_style="bold")
    view.add_column("Code", justify="right")
    view.add_column("Class")
    view.add_column("t C/ha", justify="right")
    view.add_column("Source")
    for row in rows.itertuples():
        value = "varies" if row.VALUE != row.VALUE else f"{row.VALUE:g}"
        view.add_row(str(row.LULC), row.LULC_CLASS, value, row.NOTE)
    console.print(view)


@app.command()
def compare(
    old: Annotated[Path, typer.Argument(help="Folder with the earlier outputs.")],
    new: Annotated[Path, typer.Argument(help="Folder with the newer outputs.")],
    as_json: JsonOpt = False,
) -> None:
    """Compare two output folders. Exits with code 1 if they differ."""
    from polopt_carbon.compare import compare_folders

    try:
        results = compare_folders(old, new)
    except Exception as exc:
        raise _fail(exc, as_json) from exc
    same = all(r.same for r in results)
    if as_json:
        output.print_json(
            json.dumps({"same": same, "results": [r.__dict__ for r in results]})
        )
    else:
        view = Table(title="Comparison", header_style="bold")
        view.add_column("Item")
        view.add_column("Result")
        view.add_column("Detail")
        for r in results:
            view.add_row(
                r.item, "[green]Same[/]" if r.same else "[red]Different[/]", r.detail
            )
        console.print(view)
    if not same:
        raise typer.Exit(code=1)


def main() -> None:
    app(prog_name="polopt-carbon")


if __name__ == "__main__":
    sys.exit(main())
