from qgis.core import (
    QgsProcessingAlgorithm,
    QgsProcessingParameterRasterLayer,
    QgsProcessingParameterVectorLayer,
    QgsProcessingParameterString,
    QgsProcessingParameterNumber,
    QgsProcessingParameterBoolean,
    QgsProcessingParameterFolderDestination,
    QgsProcessingOutputRasterLayer,
    QgsProcessingException,
)
import subprocess
import os
import tempfile
import yaml
from datetime import datetime

import platform
import urllib.request
import tarfile
import zipfile
import shutil
import uuid

import processing  # QGIS Processing framework


# ----------------------------
# Runtime settings
# ----------------------------
WHEEL_URL = "https://github.com/renatovargas/polopt_carbon/releases/download/v0.1.0/polopt_carbon-0.1.0-py3-none-any.whl"


def _runtime_dir():
    return os.path.join(os.path.expanduser("~"), ".polopt_carbon_runtime")


def _micromamba_exe(rt_dir):
    exe = "micromamba.exe" if os.name == "nt" else "micromamba"
    return os.path.join(rt_dir, "micromamba", exe)


def _env_prefix(rt_dir):
    return os.path.join(rt_dir, "env")


def _env_python(env_prefix):
    return (
        os.path.join(env_prefix, "python.exe")
        if os.name == "nt"
        else os.path.join(env_prefix, "bin", "python")
    )


def _lockfile_path():
    return os.path.join(os.path.dirname(__file__), "conda-lock.yml")


def _download_micromamba(mamba_path, feedback):
    os.makedirs(os.path.dirname(mamba_path), exist_ok=True)

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "windows":
        url = "https://micro.mamba.pm/api/micromamba/win-64/latest"
        archive = mamba_path + ".zip"
        urllib.request.urlretrieve(url, archive)

        with zipfile.ZipFile(archive, "r") as z:
            member = next(n for n in z.namelist() if n.endswith("micromamba.exe"))
            z.extract(member, os.path.dirname(mamba_path))
            shutil.move(os.path.join(os.path.dirname(mamba_path), member), mamba_path)

        os.remove(archive)

    else:
        if system == "darwin":
            plat = "osx-arm64" if machine in ("arm64", "aarch64") else "osx-64"
        else:
            plat = "linux-64"

        url = f"https://micro.mamba.pm/api/micromamba/{plat}/latest"
        archive = mamba_path + ".tar.bz2"
        urllib.request.urlretrieve(url, archive)

        with tarfile.open(archive, "r:bz2") as tar:
            member = next(
                m for m in tar.getmembers() if m.name.endswith("bin/micromamba")
            )
            tar.extract(member, os.path.dirname(mamba_path))
            shutil.move(
                os.path.join(os.path.dirname(mamba_path), member.name), mamba_path
            )

        os.remove(archive)
        os.chmod(mamba_path, 0o755)


def ensure_runtime(feedback):
    rt_dir = _runtime_dir()
    os.makedirs(rt_dir, exist_ok=True)

    mamba = _micromamba_exe(rt_dir)
    env_prefix = _env_prefix(rt_dir)
    py = _env_python(env_prefix)
    lockfile = _lockfile_path()

    if not os.path.exists(lockfile):
        raise QgsProcessingException("Missing conda-lock.yml next to script")

    if not os.path.exists(mamba):
        feedback.pushInfo("Installing micromamba...")
        _download_micromamba(mamba, feedback)

    if not os.path.exists(py):
        feedback.pushInfo("Creating runtime environment...")
        subprocess.run(
            [mamba, "create", "-y", "-p", env_prefix, "-f", lockfile], check=True
        )

        feedback.pushInfo("Installing polopt_carbon...")
        subprocess.run([py, "-m", "pip", "install", "--upgrade", "pip"], check=True)
        subprocess.run([py, "-m", "pip", "install", WHEEL_URL], check=True)

    return py


def export_boundary(boundary_layer, context, feedback):
    out_path = os.path.join(tempfile.gettempdir(), f"boundary_{uuid.uuid4().hex}.gpkg")

    processing.run(
        "native:savefeatures",
        {"INPUT": boundary_layer, "OUTPUT": out_path},
        context=context,
        feedback=feedback,
        is_child_algorithm=True,
    )

    if not os.path.exists(out_path):
        raise QgsProcessingException("Failed to export boundary")

    return out_path


class PoloptCarbonAlgorithm(QgsProcessingAlgorithm):
    LULC = "LULC"
    BOUNDARY = "BOUNDARY"
    COUNTRY = "COUNTRY"
    YEAR = "YEAR"
    OUTPUT_FOLDER = "OUTPUT_FOLDER"
    LOAD_RESULT = "LOAD_RESULT"
    OUTPUT_RASTER = "OUTPUT_RASTER"

    def name(self):
        return "polopt_carbon_mapping"

    def displayName(self):
        return "POLoPT: Run Carbon Mapping"

    def group(self):
        return "POLoPT Tools"

    def groupId(self):
        return "polopt"

    def createInstance(self):
        return PoloptCarbonAlgorithm()

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterRasterLayer(self.LULC, "LULC Raster"))
        self.addParameter(QgsProcessingParameterVectorLayer(self.BOUNDARY, "Boundary"))
        self.addParameter(
            QgsProcessingParameterString(self.COUNTRY, "ISO3", defaultValue="GTM")
        )

        self.addParameter(
            QgsProcessingParameterNumber(
                self.YEAR,
                "Year",
                type=QgsProcessingParameterNumber.Integer,
                defaultValue=datetime.now().year,
            )
        )
        self.addParameter(
            QgsProcessingParameterFolderDestination(self.OUTPUT_FOLDER, "Output Folder")
        )
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.LOAD_RESULT, "Load Result", defaultValue=True
            )
        )

        self.addOutput(
            QgsProcessingOutputRasterLayer(self.OUTPUT_RASTER, "Output Raster")
        )

    def processAlgorithm(self, parameters, context, feedback):
        lulc = self.parameterAsRasterLayer(parameters, self.LULC, context)
        boundary = self.parameterAsVectorLayer(parameters, self.BOUNDARY, context)

        if lulc is None or boundary is None:
            raise QgsProcessingException("Invalid inputs")

        lulc_path = lulc.source()
        boundary_path = export_boundary(boundary, context, feedback)

        country = self.parameterAsString(parameters, self.COUNTRY, context)
        year = self.parameterAsInt(parameters, self.YEAR, context)
        output_dir = self.parameterAsString(parameters, self.OUTPUT_FOLDER, context)

        config_path = os.path.join(tempfile.gettempdir(), "qgis_config.yaml")

        with open(config_path, "w") as f:
            yaml.safe_dump(
                {
                    "project": {"country": country, "year": year, "overwrite": True},
                    "inputs": {"lulc": lulc_path, "boundary": boundary_path},
                    "outputs": {"folder": output_dir},
                },
                f,
            )

        py = ensure_runtime(feedback)

        cmd = [
            py,
            "-m",
            "polopt_carbon.cli",
            "run",
            "--config",
            config_path,
            "--year",
            str(year),
        ]
        feedback.pushInfo("Running: " + " ".join(cmd))

        subprocess.run(cmd, check=True)

        out = os.path.join(output_dir, f"{country.lower()}_{year}_carbon_density.tif")

        return {self.OUTPUT_RASTER: out}
