import xml.etree.ElementTree as ET
import boto3
import zipfile
import io
import re
import datetime
from pyspark.sql.types import StringType, StructField, StructType, DateType, IntegerType, FloatType

class XMLtoParquet:
    def __init__(self, spark, bucket, prefix):
        self.s3 = boto3.client('s3')
        self.bucket = bucket
        self.prefix = prefix
        self.spark = spark

    def getRBSDimSchema(self):
        return StructType([
            StructField("Date", DateType(), True),
            StructField("CountryCode", StringType(), True),
            StructField("AnalyticsRBSKey", StringType(), True),
            StructField("RBSCode", StringType(), True),
            StructField("RBSName", StringType(), True),
            StructField("ConstructionDate", DateType(), True),
            StructField("ServiceEffectiveDate", DateType(), True),
            StructField("ServiceEndDate", DateType(), True),
            StructField("RetirementDate", DateType(), True),
            StructField("FunctionalStatus", StringType(), True),
            StructField("VendorName", StringType(), True),
            StructField("TechnologyCode", StringType(), True),
            StructField("SiteID", StringType(), True),
            StructField("SiteName", StringType(), True),
            StructField("AnalyticsControllerKey", StringType(), True),
            StructField("ControllerName", StringType(), True),
            StructField("CVECode", StringType(), True),
            StructField("CVEName", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("StructureType", StringType(), True),
            StructField("Role", StringType(), True),
            StructField("Territory", StringType(), True),
            StructField("Department", StringType(), True),
            StructField("Municipality", StringType(), True),
            StructField("Latitude", FloatType(), True),
            StructField("Longitude", FloatType(), True),
            StructField("MetersAboveSeaLevel", FloatType(), True),
            StructField("ElectricalAccountNumber", StringType(), True),
            StructField("ElectricalVendor", StringType(), True),
            StructField("ElectricalEnergySource", StringType(), True),
            StructField("SiteOwner", StringType(), True),
            StructField("SharedSite", StringType(), True),
            StructField("OwnershipStatus", StringType(), True),
            StructField("Operator", StringType(), True),
            StructField("SymbolPowerSaving", StringType(), True)
        ])

    def getCellDimSchema(self):
        return StructType([
            StructField("Date", DateType(), True),
            StructField("CountryCode", StringType(), True),
            StructField("AnalyticsCellKey", StringType(), True),
            StructField("CellCode", StringType(), True),
            StructField("CellName", StringType(), True),
            StructField("ParentCellName", StringType(), True),
            StructField("EffectiveStartDate", DateType(), True),
            StructField("EffectiveEndDate", DateType(), True),
            StructField("MCC", StringType(), True),
            StructField("MNC", StringType(), True),
            StructField("LAC", StringType(), True),
            StructField("RAC", StringType(), True),
            StructField("TAC", StringType(), True),
            StructField("UTRANRegistrationArea", StringType(), True),
            StructField("SectorCode", StringType(), True),
            StructField("SectorName", StringType(), True),
            StructField("CellId", StringType(), True),
            StructField("LocalCellId", StringType(), True),
            StructField("CellGlobalId", StringType(), True),
            StructField("AnalyticsRBSKey", StringType(), True),
            StructField("RBSName", StringType(), True),
            StructField("SiteID", StringType(), True),
            StructField("AnalyticsControllerKey", StringType(), True),
            StructField("ControllerName", StringType(), True),
            StructField("VendorName", StringType(), True),
            StructField("TechnologyCode", StringType(), True),
            StructField("CarrierCode", StringType(), True),
            StructField("Bandwidth", FloatType(), True),
            StructField("FrequencyBand", StringType(), True),
            StructField("MIMOMode", StringType(), True),
            StructField("AntennaAzimuth", StringType(), True),
            StructField("AntennaHeight", StringType(), True),
            StructField("AntennaTilt", StringType(), True),
            StructField("HorizontalBeamwidth", StringType(), True),
            StructField("VerticalBeamwidth", StringType(), True),
            StructField("PowerCarrierOff", StringType(), True)
        ])

    def showBucketFiles(self, pattern):
        # Lista los objetos en el bucket especificado
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)

        # Lista para almacenar los archivos encontrados
        file_keys = []

        # Verifica si hay archivos en el bucket
        if 'Contents' in response:
            # Itera sobre los archivos y los imprime
            for obj in response['Contents']:
                file_key = obj['Key']
                # Verifica si el archivo tiene extensión ZIP
                if file_key.endswith(pattern):
                    file_keys.append(file_key)

        # Retorna la lista de archivos
        return file_keys
    
    def getXmlFromZip(self, zip_keys, pattern):
        for zip_key in zip_keys:
            # Descarga el archivo ZIP desde el bucket
            response = self.s3.get_object(Bucket=self.bucket, Key=zip_key)
            zip_data = response['Body'].read()

            # Crea un objeto ZipFile a partir de los datos del archivo ZIP
            zip_file = zipfile.ZipFile(io.BytesIO(zip_data))

            # Extrae el archivo XML del archivo ZIP
            for file_name in zip_file.namelist():
                if file_name.endswith(pattern):
                    # Lee el contenido del archivo XML en una cadena
                    xml_data = zip_file.read(file_name).decode('utf-8')
                    
                    # Obtiene la ruta de destino para guardar el archivo XML
                    dest_folder = f"{self.prefix}/XML_{zip_key.split('/')[1]}/"
                    dest_key = f"{dest_folder}{file_name}"
                    
                    # Guarda el archivo XML en S3
                    self.s3.put_object(Body=xml_data.encode('utf-8'), Bucket=self.bucket, Key=dest_key)
                    
                    #print(f"Archivo XML guardado en: s3://{bucket_name}/{dest_key}")

    def readXmlFromS3andParseSchema(self, file_path, schema):
        
        namespace = {'xmlns': 'http://www.millicom.com'}

        # Crea una lista para almacenar los registros del XML
        rows = []

        # Descarga el archivo XML desde S3
        response = self.s3.get_object(Bucket=self.bucket, Key=file_path)
        xml_content = response['Body'].read()

        # Analiza el contenido XML
        root = ET.fromstring(xml_content)

        # Extrae los registros del XML
        for r_element  in root.findall('xmlns:r', namespace):
            # Obtener todos los campos en una fila
            fields = [element.text for element in r_element]
            fields = self.convertDateFieldsToDate(fields)
            fields = self.convertFloatFieldsToFloat(fields)
            rows.append(fields)
            
        # Crea un DataFrame de Spark a partir de los registros
        df = self.spark.createDataFrame(rows, schema)

        return df
    
    def convertDateFieldsToDate(self,fields):
        for i in range(len(fields)):
            field_value = fields[i]
            if self.isDateWithoutTime(field_value):
                date_obj = datetime.datetime.strptime(field_value, "%Y-%m-%d").date()
                # Reemplazar el valor de la cadena con el objeto de fecha válido sin hora
                fields[i] = date_obj
        return fields
    
    def convertFloatFieldsToFloat(self, fields):
        for i in range(len(fields)):
            field_value = fields[i]
            if isinstance(field_value, str) and self.isFloat(field_value):
                float_value = float(field_value)
                # Reemplazar el valor de la cadena con el valor float convertido
                fields[i] = float_value
        return fields

    def isDateWithoutTime(self, value):
        # Expresión regular para verificar si el valor tiene apariencia de fecha sin hora
        date_pattern = r'^\d{4}-\d{2}-\d{2}$'
        return re.match(date_pattern, value) is not None

    def isFloat(self, value):
        # Expresión regular para verificar si el valor tiene apariencia de float
        float_pattern = r'^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
        return re.match(float_pattern, value) is not None        

    def saveDataframeToS3(self, dataframe, s3_path):
        # Verificar si la ruta de S3 ya existe
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=s3_path)

        if 'Contents' in response:
            # La ruta de S3 ya existe, hacer un append al directorio existente
            dataframe.write.mode("append").parquet(f"s3a://{self.bucket}/{s3_path}")
        else:
            # La ruta de S3 no existe, crear el directorio y guardar los datos
            dataframe.write.mode("overwrite").parquet(f"s3a://{self.bucket}/{s3_path}")
                
    def convertXMLtoParquet(self, xml_list, schema):
        for xml in xml_list:
            # Extraer la fecha del nombre del archivo XML
            date_str = xml.split("/")[1][4:12]
            base_s3_path = self.prefix
            # Construir la ruta en S3 para guardar el DataFrame
            s3_path = f"{base_s3_path}/DatePartKey={date_str}/"

            df = self.readXmlFromS3andParseSchema(xml, schema)
            self.saveDataframeToS3(df, s3_path)
 
    def moveXMLFoldersToBackup(self, destination_prefix = None, destination_bucket = None):
        if destination_bucket is None:
            destination_bucket = self.bucket
        if destination_prefix is None:
            destination_prefix = self.prefix        
            
        # Obtener la lista de objetos en el bucket y prefijo de origen
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
        
        for obj in response.get('Contents', []):
            # Obtener el nombre del objeto/directorio
            key = obj['Key']
            # Verificar si el objeto es un directorio y no tiene el prefijo "Date="
            if 'DatePartKey=' not in key:
                # Construir la ruta de destino
                destination_key = key.replace(self.prefix, destination_prefix)
                
                # Copiar el objeto al bucket de destino
                self.s3.copy_object(
                    CopySource={'Bucket': self.bucket, 'Key': key},
                    Bucket=destination_bucket,
                    Key=destination_key
                )
                # Borrar archivos no necesarios en bucket principal
                self.s3.delete_object(Bucket=self.bucket, Key=key)
            
