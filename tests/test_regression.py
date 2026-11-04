"""Results must match output captured from the validated v0.1.0 code."""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import rasterio

from polopt_carbon.config import build_config
from polopt_carbon.pipeline import run

UGANDA = Path(__file__).parent.parent / "testdata" / "uga"


@pytest.fixture(scope="module")
def result(inputs, tmp_path_factory):
    lulc, boundary = inputs
    out = tmp_path_factory.mktemp("out")
    cfg = build_config(
        lulc=lulc, boundary=boundary, iso="TST", year=2023, output_dir=out
    )
    return run(cfg)


def test_carbon_table_matches_golden(result, golden):
    new = pd.read_excel(result.outputs["carbon_table"])
    old = pd.read_csv(golden / "carbon_table.csv")
    assert list(new.columns[: len(old.columns)]) == list(old.columns)
    pd.testing.assert_frame_equal(new[old.columns], old, check_dtype=False)


@pytest.mark.parametrize("method", ["dominant", "weighted"])
def test_invest_tables_match_golden(result, golden, method):
    new = pd.read_csv(result.outputs[f"invest_{method}"])
    old = pd.read_csv(golden / f"invest_{method}.csv")
    assert list(new.columns) == ["lucode", "c_above"]
    pd.testing.assert_frame_equal(new, old)


def test_density_raster_matches_golden(result, golden):
    saved = np.load(golden / "carbon_density.npz")
    with rasterio.open(result.outputs["carbon_density"]) as src:
        values = src.read(1)
        assert np.isnan(src.nodata)
        assert src.crs.to_epsg() == 6933
        assert np.allclose(
            tuple(src.transform)[:6], saved["transform"], rtol=0, atol=1e-6
        )
    np.testing.assert_array_equal(values, saved["values"])


def test_lulc_raster_aligns_with_density(result):
    with (
        rasterio.open(result.outputs["lulc"]) as lulc,
        rasterio.open(result.outputs["carbon_density"]) as density,
    ):
        assert lulc.transform == density.transform
        assert lulc.shape == density.shape
        assert lulc.crs == density.crs


def test_manifest_records_settings_and_hashes(result):
    manifest = json.loads(result.manifest.read_text())
    assert manifest["settings"]["iso"] == "TST"
    assert set(manifest["outputs"]) == set(result.outputs)
    assert len(manifest["inputs"]["lulc"]["sha256"]) == 64


@pytest.mark.uganda
@pytest.mark.skipif(not UGANDA.is_dir(), reason="testdata/uga is not available")
def test_uganda_matches_delivered_outputs(tmp_path):
    cfg = build_config(
        lulc=UGANDA / "lulc/MCD12Q1.061_LC_Type1_doy2023001_aid0001.tif",
        boundary=UGANDA / "boundaries/uga_admbnda_adm0_ubos_20200824.shp",
        iso="UGA",
        year=2023,
        output_dir=tmp_path,
    )
    result = run(cfg)
    with (
        rasterio.open(UGANDA / "outputs/uga_carbon_density.tif") as old,
        rasterio.open(result.outputs["carbon_density"]) as new,
    ):
        np.testing.assert_array_equal(new.read(1), old.read(1))
    delivered = pd.read_csv(UGANDA / "outputs/uga_carbon_table.csv")
    assert result.analysis.total_carbon == pytest.approx(
        delivered["TOTAL_CARBON"].sum(), abs=1e-3
    )
