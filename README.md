# Configurar Script de Instalación (create_job.sh)

Antes de ejecutar el script, asegúrate de configurar los siguientes parámetros en el archivo `script.sh`:

- `BUCKET`: El nombre del bucket de S3 donde se encuentran los archivos.
- `DEST`: El nombre del directorio de destino en el bucket de S3.
- `BACKUPFOLDER`: El nombre del directorio de respaldo de archivos XML.
- `PREFIX1`: El prefijo del directorio para el primer tipo de archivo.
- `PREFIX2`: El prefijo del directorio para el segundo tipo de archivo.
- `PATTERNZIP1`: El patrón para los archivos ZIP del primer tipo.
- `PATTERNXML1`: El patrón para los archivos XML del primer tipo.
- `PATTERNZIP2`: El patrón para los archivos ZIP del segundo tipo.
- `PATTERNXML2`: El patrón para los archivos XML del segundo tipo.
- `SCRIPT1`: El nombre del script de Python para el primer tipo de archivo.
- `SCRIPT2`: El nombre del script de Python para el segundo tipo de archivo.
- `CLASS`: El nombre del archivo ZIP que contiene la clase `GlueUtilities`.
- `REGION`: La región de AWS donde se encuentra el bucket de S3.
- `PROFILE`: El nombre del perfil de AWS con los permisos necesarios.
- `NAME1`: El nombre del trabajo de Glue para el primer tipo de archivo.
- `NAME2`: El nombre del trabajo de Glue para el segundo tipo de archivo.
- `ROLE`: El ARN del rol de AWS Glue utilizado por los trabajos.
- `GLUEV`: La versión de AWS Glue utilizada.
- `WORKER_TYPE`: El tipo de instancia de AWS Glue utilizada para los trabajos.
- `WORKER_NUM`: El número de instancias de AWS Glue utilizadas para los trabajos.

Recuerda que estos parámetros deben ajustarse según tu configuración y los requisitos del entorno.

# Configurar archivo de copia de archivos a bucket (copy_files_to_s3.sh) 

Antes de ejecutar el script bash que utiliza los comandos de AWS CLI para copiar archivos al bucket de S3, asegúrate de seguir estos pasos de configuración: 

1. Asegúrate de tener instalada la AWS CLI en tu sistema. Si no la tienes instalada, puedes seguir la guía oficial de instalación de AWS CLI para tu sistema operativo. https://docs.aws.amazon.com/es_es/cli/latest/userguide/getting-started-install.html

2. Abre el archivo del script bash en tu editor de texto preferido.

3. Reemplaza los valores de las variables `BUCKET`, `DEST` y `PROFILE` con tus propios valores:

```
   BUCKET="<YOUR_BUCKET_NAME>"
   DEST="<YOUR_DESTINATION_FOLDER>"
   PROFILE="<YOUR_AWS_PROFILE>"
```

# Configuración de las credenciales de AWS

Antes de utilizar los servicios de AWS, asegúrate de configurar correctamente las credenciales de acceso en el archivo `.aws/credentials`. Sigue los siguientes pasos para realizar la configuración:

1. Abre el archivo `.aws/credentials` en tu editor de texto preferido.

2. Agrega las siguientes líneas al archivo, reemplazando `<YOUR_ACCESS_KEY>` y `<YOUR_SECRET_KEY>` con tus propias credenciales de AWS:

   ```
   [default]
   aws_access_key_id = <YOUR_ACCESS_KEY>
   aws_secret_access_key = <YOUR_SECRET_KEY>
   ```

3. Algunos casos necesitarán configurar `aws_session_token` del mismo modo

# Ejecutar copy_files_tos3.sh

NOTA: primero debe comprimirse la clase GlueUtilities.py a GlueUtilities.zip

usar como referencia los archivos:
-   compress_files.sh
-   compress_files.bat 

```
cd Utilities
./copy_files_to_s3.sh
```

# Ejecutar create_jobs.sh
```
cd Utilities
./create_jobs.sh
```

