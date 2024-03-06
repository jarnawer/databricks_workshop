# COMMAND ----------
# MAGIC %md
# MAGIC ## Ingesta de datos desde los archivos de origen
# MAGIC
# MAGIC En este notebook se realiza la carga de datos desde los archivos de origen,y se almacenan en un formato que permita su posterior análisis.
# MAGIC Cargaremos los datos desde un fichero de metadatos que nos hemos creado para simular el origen de los datos.
# MAGIC

# COMMAND ----------
# MAGIC %md
# MAGIC ## Hemos creado un fichero de metadatos para cargar cada una de las tablas:
# MAGIC
# MAGIC ### Por cada tabla se especifica:
# MAGIC - Nombre de la tabla
# MAGIC - Nombre del fichero
# MAGIC - Primary key del fichero
# MAGIC
# MAGIC ```yaml
# MAGIC tables:
# MAGIC   - table: "circuits"
# MAGIC     file: "circuits.csv"
# MAGIC     primary_key: "circuitId"
# MAGIC   - table: "constructor_results"
# MAGIC     file: "constructor_results.csv"
# MAGIC     pk: "constructorResultsId"
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ### Definimos a continuacion una serie de funciones que nos permitiran leer el fichero de metadatos y leer y escribir los datos en el formato deseado.
# MAGIC

# COMMAND ----------
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable


spark_session = SparkSession.builder.appName("bronze_pipeline").getOrCreate()
SCHEMA="bronze"
SOURCE_PATH = "dbfs:/FileStore/tables/datasets"
BRONZE_PATH = f"dbfs:/FileStore/tables/f1/{SCHEMA}"
METADATA_PATH = f"{SOURCE_PATH}/table_metadata.yaml"


def read_yml_file(file_path:str):
    file_content=dbutils.fs.head(file_path)
    return yaml.safe_load(file_content)
    
def read_table(spark_session:SparkSession, table:str, file_path:str):
    # read a csv file
    return (spark_session.read.format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .option("quote", "\"")
            .option("escape","\"")
            .load(f"{file_path}"))
    
def write_table(spark_session:SparkSession, df:DataFrame, table:str, table_path:str, schema_name:str, mode:str="append"):
    # write a dataframe to a delta table
    spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    
    deltaTable = (
            DeltaTable.createIfNotExists(spark_session)
            .location(table_path)
            .tableName(f"{schema_name}.{table}")
            .addColumns(df.schema)
            .execute()
    )
    
    deltaTable.alias("existing").merge(df.alias("incoming"), 
                                       f"existing.ingestion_timestamp = incoming.ingestion_timestamp") \
                                .whenNotMatchedInsertAll() \
                                .execute()
                                
                                
def transform_data(df:DataFrame):
    return df.withColumn("ingestion_timestamp", F.current_timestamp())

# COMMAND ----------
# MAGIC %md
# MAGIC ### Leemos los datos en CSV desde la ruta de origen usando el fichero de metadatos
# MAGIC

# COMMAND ----------
metadata = read_yml_file(METADATA_PATH)
print("Metadata file loaded!")
print(metadata)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Iteramos sobre cada tabla y aplicamos el siguiente pipeline:
# MAGIC - 1. Leer el archivo de origen
# MAGIC - 2. Aplicamos la logica de transformacion sobre el archivo de origen
# MAGIC - 3. Guardamos el archivo transformado en formato Delta
# MAGIC

# COMMAND ----------
for table in metadata["tables"]:
    table_name = table["table_name"]
    file_name = table["file_name"]
    primary_key = table["primary_key"]
    
    print(f"Reading {file_name} into {table_name}...")
    df = read_table(spark_session, table_name, f"{SOURCE_PATH}/{file_name}")
    
    print(f"Applying transformation logic to {table_name}...")
    transformed_df = transform_data(df)
    
    row_count = transformed_df.count()
    
    print(f"Writing {row_count} rows to Delta Table {SCHEMA}.{table_name}...")
    write_table(spark_session, transformed_df, table_name, f"{BRONZE_PATH}/{table_name}", SCHEMA)
    
# COMMAND ----------
# MAGIC %md
# MAGIC ### Ahora que hemos cargado los datos en formato Delta, podemos realizar consultas y analisis sobre ellos.
# MAGIC
# MAGIC Hemos creado una base de datos (schema), **bronze**, en la que se almacenan los datos en bruto.
# MAGIC
# MAGIC Podemos usar consultas sobre ellos para realizar analisis exploratorio, limpieza de datos, etc.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM bronze.circuits

# COMMAND ----------
drivers = spark_session.read.format("delta").table("bronze.drivers")
display(drivers)