from qgis.core import (QgsProcessing,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterRasterLayer,
                       QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterString,
                       QgsProcessingParameterNumber,
                       QgsProcessingParameterBoolean,
                       QgsProcessingParameterFolderDestination,
                       QgsProcessingOutputRasterLayer)
import subprocess
import os
import tempfile
import yaml
from datetime import datetime

class PoloptCarbonAlgorithm(QgsProcessingAlgorithm):
    LULC = 'LULC'
    BOUNDARY = 'BOUNDARY'
    COUNTRY = 'COUNTRY'
    YEAR = 'YEAR'
    OUTPUT_FOLDER = 'OUTPUT_FOLDER'
    LOAD_RESULT = 'LOAD_RESULT'
    
    # Output constant for the layer produced
    OUTPUT_RASTER = 'OUTPUT_RASTER'

    def name(self):
        return 'polopt_carbon_mapping'

    def displayName(self):
        return 'POLoPT: Run Carbon Mapping'

    def group(self):
        return 'POLoPT Tools'

    def groupId(self):
        return 'polopt'

    def createInstance(self):
        return PoloptCarbonAlgorithm()

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterRasterLayer(self.LULC, 'LULC Raster (.tif)'))
        # Internalized: GEZ layer is no longer a required input parameter
        self.addParameter(QgsProcessingParameterFeatureSource(self.BOUNDARY, 'National Boundary Layer'))
        self.addParameter(QgsProcessingParameterString(self.COUNTRY, 'Country ISO3 Code', defaultValue='GTM'))
        
        # Add Year Parameter, default to current year
        current_year = datetime.now().year
        self.addParameter(QgsProcessingParameterNumber(self.YEAR, 'Analysis Year', type=QgsProcessingParameterNumber.Integer, defaultValue=current_year))
        
        self.addParameter(QgsProcessingParameterFolderDestination(self.OUTPUT_FOLDER, 'Output Folder'))
        self.addParameter(QgsProcessingParameterBoolean(self.LOAD_RESULT, 'Load Resulting Raster', defaultValue=True))
        
        self.addOutput(QgsProcessingOutputRasterLayer(self.OUTPUT_RASTER, 'Carbon Density Map'))

    def processAlgorithm(self, parameters, context, feedback):
        lulc_source = self.parameterAsRasterLayer(parameters, self.LULC, context)
        boundary_source = self.parameterAsSource(parameters, self.BOUNDARY, context)
        
        if lulc_source is None or boundary_source is None:
            raise QgsProcessingException("Invalid inputs")

        lulc_path = lulc_source.source()
        boundary_path = boundary_source.source().split('|')[0]  # Handle possible layer string extras
        
        country = self.parameterAsString(parameters, self.COUNTRY, context)
        year = self.parameterAsInt(parameters, self.YEAR, context)
        output_dir = self.parameterAsString(parameters, self.OUTPUT_FOLDER, context)
        load_map = self.parameterAsBoolean(parameters, self.LOAD_RESULT, context)
        
        # 2. Setup temporary config
        temp_dir = tempfile.gettempdir()
        config_path = os.path.join(temp_dir, 'qgis_config.yaml')
        
        # Config structure
        config_data = {
            'project': {'country': country, 'overwrite': True, 'year': year},
            'inputs': {'lulc': lulc_path, 'boundary': boundary_path},
            'outputs': {'folder': output_dir}
        }
        
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        # 3. Call the CLI command
        cmd = ["polopt-carbon", "run", "--config", config_path, "--year", str(year)]
        
        feedback.pushInfo(f"Executing CLI command: {' '.join(cmd)}")
        
        try:
            process = subprocess.run(cmd, capture_output=True, text=True, check=True)
            feedback.pushInfo(process.stdout)
        except subprocess.CalledProcessError as e:
            feedback.reportError(f"CLI Error: {e.stderr}", fatal=True)
            return {}

        # 4. Handle results
        expected_tif = os.path.join(output_dir, f"{country.lower()}_{year}_carbon_density.tif")
        results = {self.OUTPUT_FOLDER: output_dir}
        
        if load_map:
            if os.path.exists(expected_tif):
                results[self.OUTPUT_RASTER] = expected_tif
            else:
                feedback.reportWarning(f"Output file not found at {expected_tif}")
        
        return results
