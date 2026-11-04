import json

import pytest
from typer.testing import CliRunner

from polopt_carbon import __version__
from polopt_carbon.cli import app

runner = CliRunner()


@pytest.fixture(scope="module")
def run_output(inputs, tmp_path_factory):
    out = tmp_path_factory.mktemp("cli")
    lulc, boundary = inputs
    args = ["run", str(lulc), str(boundary), "--iso", "TST", "--year", "2023"]
    result = runner.invoke(app, [*args, "-o", str(out), "--json"])
    return out, args, result


def test_version():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0 and __version__ in result.stdout


def test_run_prints_parseable_json(run_output):
    _, _, result = run_output
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "success"
    assert payload["total_carbon_mg"] > 0
    assert payload["value_source_pixels"]["unmatched"] > 0


def test_existing_outputs_exit_with_code_4(run_output):
    out, args, _ = run_output
    assert runner.invoke(app, [*args, "-o", str(out)]).exit_code == 4
    assert runner.invoke(app, [*args, "-o", str(out), "--overwrite"]).exit_code == 0


def test_missing_iso_exits_with_code_3(inputs):
    lulc, boundary = inputs
    assert runner.invoke(app, ["run", str(lulc), str(boundary)]).exit_code == 3


def test_check_writes_nothing_and_needs_no_iso(inputs, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    lulc, boundary = inputs
    result = runner.invoke(app, ["check", str(lulc), str(boundary), "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["status"] == "ok"
    assert list(tmp_path.iterdir()) == []


def test_check_missing_file_exits_with_code_3(inputs, tmp_path):
    _, boundary = inputs
    result = runner.invoke(app, ["check", str(tmp_path / "no.tif"), str(boundary)])
    assert result.exit_code == 3


def test_init_writes_a_usable_config(inputs, tmp_path):
    lulc, boundary = inputs
    path = tmp_path / "c.yaml"
    answers = f"uga\n2023\n{lulc}\n{boundary}\n{tmp_path}/o\nn\n"
    result = runner.invoke(app, ["init", str(path)], input=answers)
    assert result.exit_code == 0, result.output
    checked = runner.invoke(app, ["check", "--config", str(path), "--json"])
    assert checked.exit_code == 0, checked.output
    assert runner.invoke(app, ["init", str(path)]).exit_code == 4


def test_coefficients_command():
    result = runner.invoke(
        app,
        [
            "coefficients",
            "--region",
            "Africa",
            "--gez",
            "Tropical rainforest",
            "--json",
        ],
    )
    assert result.exit_code == 0
    rows = {row["LULC"]: row for row in json.loads(result.stdout)}
    assert rows[2]["VALUE"] == 200


def test_compare_same_and_different(run_output, inputs, tmp_path):
    out, args, _ = run_output
    same = runner.invoke(app, ["compare", str(out), str(out)])
    assert same.exit_code == 0, same.output
    other = tmp_path / "other"
    runner.invoke(app, [*args, "-o", str(other), "--use-global-classes"])
    assert runner.invoke(app, ["compare", str(out), str(other)]).exit_code == 1
