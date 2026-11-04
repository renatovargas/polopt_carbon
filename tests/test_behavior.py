import pandas as pd
import pytest

from polopt_carbon.coefficients import (
    SOURCE_GLOBAL,
    SOURCE_UNMATCHED,
    attach_coefficients,
    coefficients_for_zone,
    load_crosswalk,
    load_lookup,
)
from polopt_carbon.config import (
    InputError,
    OutputExistsError,
    Rules,
    build_config,
)
from polopt_carbon.invest import pool_table
from polopt_carbon.pipeline import analyze, check_inputs, run


def overlay(code, gez="Tropical rainforest", region="Africa", frontier=0):
    return pd.DataFrame(
        {
            "LULC": [code],
            "CZ_ID": [1],
            "Count": [10],
            "REGION": [region],
            "GEZ_TERM": [gez],
            "FRONTIER": [frontier],
        }
    )


def attach(frame, **kwargs):
    return attach_coefficients(frame, load_crosswalk(), load_lookup(), **kwargs)


@pytest.mark.parametrize("code,value", [(12, 5.0), (16, 1.0), (13, 0.0), (17, 0.0)])
def test_global_classes_are_opt_in(code, value):
    default = attach(overlay(code)).iloc[0]
    assert default["CARBON_VALUE_ADJ"] == 0.0
    assert default["VALUE_SOURCE"] == SOURCE_UNMATCHED

    fixed = attach(overlay(code), use_global_classes=True).iloc[0]
    assert fixed["CARBON_VALUE_ADJ"] == value
    assert fixed["VALUE_SOURCE"] == SOURCE_GLOBAL


def test_global_flag_fixes_typo_rows():
    frame = overlay(10, gez="Temperate oceanic forest", region="Europe")
    assert attach(frame).iloc[0]["CARBON_VALUE_ADJ"] == 0.0
    assert attach(frame, use_global_classes=True).iloc[0]["CARBON_VALUE_ADJ"] > 0


def test_savanna_rule_uses_configured_fraction():
    frame = pd.concat([overlay(2), overlay(9)], ignore_index=True)
    assert attach(frame).iloc[1]["CARBON_VALUE_ADJ"] == pytest.approx(80.0)
    table = attach(frame, rules=Rules(savanna_percent_of_forest=0.5))
    assert table.iloc[1]["CARBON_VALUE_ADJ"] == pytest.approx(100.0)
    assert table.iloc[1]["rule_applied"] == "savanna=50%_forest"


def test_pool_table_extra_columns_are_opt_in():
    values = pd.DataFrame({"LULC": [1], "CARBON_VALUE_ADJ": [5.0]})
    assert list(pool_table(values, [1, 2]).columns) == ["lucode", "c_above"]
    full = pool_table(values, [1, 2], all_pools=True)
    assert list(full.columns) == ["lucode", "c_above", "c_below", "c_soil", "c_dead"]
    assert full["c_above"].tolist() == [5.0, 0.0]
    assert full[["c_below", "c_soil", "c_dead"]].sum().sum() == 0


def test_analysis_warns_about_zeroed_classes(inputs):
    lulc, boundary = inputs
    analysis = analyze(build_config(lulc=lulc, boundary=boundary, iso="TST", year=2023))
    text = " ".join(analysis.warnings)
    assert "Croplands" in text and "Barren" in text and "--use-global-classes" in text


def test_run_refuses_to_overwrite_unless_asked(inputs, tmp_path):
    lulc, boundary = inputs
    settings = {
        "lulc": lulc,
        "boundary": boundary,
        "iso": "TST",
        "year": 2023,
        "output_dir": tmp_path,
    }
    run(build_config(**settings))
    with pytest.raises(OutputExistsError):
        run(build_config(**settings))
    run(build_config(**settings, overwrite=True))


def test_missing_input_fails_the_check(inputs, tmp_path):
    _, boundary = inputs
    cfg = build_config(
        lulc=tmp_path / "missing.tif", boundary=boundary, require_identity=False
    )
    checks = check_inputs(cfg)
    assert not checks[-1].passed
    assert "not found" in checks[-1].detail


def test_non_overlapping_inputs_fail_the_check(inputs, tmp_path):
    import geopandas as gpd
    from shapely.geometry import box

    lulc, _ = inputs
    far = tmp_path / "far.gpkg"
    gpd.GeoDataFrame(geometry=[box(-100, 30, -99, 31)], crs="EPSG:4326").to_file(far)
    checks = check_inputs(build_config(lulc=lulc, boundary=far, require_identity=False))
    assert checks[-1].name == "Overlap" and not checks[-1].passed


def test_legacy_config_format_is_accepted(inputs, tmp_path):
    lulc, boundary = (path.as_posix() for path in inputs)
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
project: {{country: uga, year: 2020, overwrite: true}}
inputs: {{lulc: {lulc}, boundary: {boundary}, carbon_zones: old.shp}}
outputs: {{folder: {tmp_path.as_posix()}/o, table: old.csv}}
schema: {{cz_id: CODE}}
rules: {{savanna_percent_of_forest: 0.4, force_wetland_overrides: false}}
"""
    )
    cfg = build_config(config_path=path)
    assert (cfg.iso, cfg.year, cfg.overwrite) == ("UGA", 2020, True)
    assert "inputs.carbon_zones" in cfg.notices[0] and "schema" in cfg.notices[0]


def test_command_line_overrides_config(inputs, tmp_path):
    lulc, boundary = (path.as_posix() for path in inputs)
    path = tmp_path / "config.yaml"
    path.write_text(
        f"project: {{country: UGA, year: 2020}}\n"
        f"inputs: {{lulc: {lulc}, boundary: {boundary}}}\n"
    )
    cfg = build_config(config_path=path, iso="KEN", year=2021)
    assert (cfg.iso, cfg.year) == ("KEN", 2021)


@pytest.mark.parametrize(
    "settings",
    [{"iso": "UGANDA"}, {"iso": "UGA", "year": 3}, {}],
)
def test_invalid_settings_are_rejected(inputs, settings):
    lulc, boundary = inputs
    with pytest.raises(InputError):
        build_config(lulc=lulc, boundary=boundary, **settings)


def test_coefficients_for_zone():
    rows = coefficients_for_zone("africa", "tropical rainforest").set_index("LULC")
    assert rows.loc[2, "VALUE"] == 200
    assert rows.loc[12, "VALUE"] == 0
    assert "Global row has 5" in rows.loc[12, "NOTE"]
    assert pd.isna(rows.loc[9, "VALUE"]) and "40%" in rows.loc[9, "NOTE"]
    fixed = coefficients_for_zone(
        "Africa", "Tropical rainforest", use_global_classes=True
    )
    assert fixed.set_index("LULC").loc[12, "VALUE"] == 5


def test_unknown_zone_suggests_alternatives():
    with pytest.raises(InputError, match="Did you mean: Tropical rainforest"):
        coefficients_for_zone("Africa", "Tropical rainforrest")
