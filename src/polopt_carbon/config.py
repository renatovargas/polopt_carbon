"""Run settings, merged from a YAML file and command-line options."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml


class PoloptError(Exception):
    """An expected failure, reported to the user without a traceback."""

    exit_code = 1


class InputError(PoloptError):
    """The inputs or settings cannot be used."""

    exit_code = 3


class OutputExistsError(PoloptError):
    """Outputs already exist and overwriting was not requested."""

    exit_code = 4


KNOWN_KEYS = {
    "project": {"country", "year", "overwrite"},
    "inputs": {"lulc", "boundary", "coeff_lookup", "crosswalk"},
    "outputs": {"folder"},
    "rules": {
        "savanna_percent_of_forest",
        "wetland_woody_equals_forest",
        "marsh_equals_shrub",
    },
    "options": {"use_global_classes", "invest_all_pools"},
}


@dataclass(frozen=True)
class Rules:
    """Fallback rule settings. The defaults are the validated method."""

    savanna_percent_of_forest: float = 0.4
    wetland_woody_equals_forest: bool = True
    marsh_equals_shrub: bool = True


@dataclass(frozen=True)
class RunConfig:
    lulc: Path
    boundary: Path
    iso: str = ""
    year: int | None = None
    output_dir: Path = Path("out")
    overwrite: bool = False
    coeff_lookup: Path | None = None
    crosswalk: Path | None = None
    rules: Rules = field(default_factory=Rules)
    use_global_classes: bool = False
    invest_all_pools: bool = False
    notices: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("notices")
        return {k: str(v) if isinstance(v, Path) else v for k, v in data.items()}


def expand(path: Any) -> Path | None:
    if path is None or str(path).strip() == "":
        return None
    return Path(path).expanduser().resolve()


def read_yaml(path: Path) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except OSError as exc:
        raise InputError(f"Cannot read config file {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise InputError(f"Config file {path} is not valid YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise InputError(f"Config file {path} must contain YAML sections.")
    for section, values in data.items():
        if section in KNOWN_KEYS and not isinstance(values, dict | type(None)):
            raise InputError(f"Config section '{section}' must contain settings.")
    return data


def ignored_keys(data: dict[str, Any]) -> list[str]:
    """List config entries this version does not use."""
    ignored = []
    for section, values in data.items():
        if section not in KNOWN_KEYS:
            ignored.append(section)
        elif values:
            ignored += [
                f"{section}.{k}" for k in values if k not in KNOWN_KEYS[section]
            ]
    return ignored


def build_config(
    *,
    config_path: Path | None = None,
    lulc: Path | None = None,
    boundary: Path | None = None,
    iso: str | None = None,
    year: int | None = None,
    output_dir: Path | None = None,
    overwrite: bool = False,
    use_global_classes: bool = False,
    invest_all_pools: bool = False,
    require_identity: bool = True,
) -> RunConfig:
    """Merge settings with precedence: command line, then config file, then defaults."""
    data = read_yaml(config_path) if config_path else {}
    project = data.get("project") or {}
    inputs = data.get("inputs") or {}
    outputs = data.get("outputs") or {}
    options = data.get("options") or {}

    lulc_path = expand(lulc) or expand(inputs.get("lulc"))
    boundary_path = expand(boundary) or expand(inputs.get("boundary"))
    missing = [
        name
        for name, value in (("LULC raster", lulc_path), ("boundary", boundary_path))
        if value is None
    ]
    if missing:
        raise InputError(
            f"Missing {' and '.join(missing)}. "
            "Pass them as arguments or set them with --config."
        )

    code = str(iso or project.get("country") or "").strip().upper()
    if code and not re.fullmatch(r"[A-Z]{3}", code):
        raise InputError(f"'{code}' is not a three-letter ISO3 country code.")
    if require_identity and not code:
        raise InputError("Missing country code. Pass --iso, for example --iso UGA.")

    notices = []
    keys = ignored_keys(data)
    if keys:
        notices.append(
            f"Ignored config entries not used by this version: {', '.join(keys)}"
        )

    year_value = year if year is not None else project.get("year")
    if year_value is None and require_identity:
        year_value = datetime.now().year
        notices.append(
            f"No year given, so output files are labeled {year_value}. "
            "Pass --year with the year of your land cover data."
        )
    if year_value is not None:
        try:
            year_value = int(year_value)
        except (TypeError, ValueError) as exc:
            raise InputError(f"'{year_value}' is not a valid year.") from exc
        if not 1900 <= year_value <= 2200:
            raise InputError(f"'{year_value}' is not a valid year.")

    rule_settings = dict(data.get("rules") or {})
    known_rules = KNOWN_KEYS["rules"]
    rules = Rules(**{k: v for k, v in rule_settings.items() if k in known_rules})
    fraction = rules.savanna_percent_of_forest
    if not isinstance(fraction, int | float) or not 0 <= fraction <= 1:
        raise InputError(
            "rules.savanna_percent_of_forest must be a fraction from 0 to 1, "
            "for example 0.4."
        )
    if rules != Rules():
        notices.append("Using non-default fallback rule settings from the config file.")

    return RunConfig(
        lulc=lulc_path,
        boundary=boundary_path,
        iso=code,
        year=year_value,
        output_dir=expand(output_dir) or expand(outputs.get("folder", "out")),
        overwrite=overwrite or bool(project.get("overwrite", False)),
        coeff_lookup=expand(inputs.get("coeff_lookup")),
        crosswalk=expand(inputs.get("crosswalk")),
        rules=rules,
        use_global_classes=use_global_classes
        or bool(options.get("use_global_classes", False)),
        invest_all_pools=invest_all_pools
        or bool(options.get("invest_all_pools", False)),
        notices=tuple(notices),
    )
