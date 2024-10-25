# Databricks Workshop

Contiene slides y codigo de laboratorios

# Contenido:
- **code_labs**: Contiene los notebooks de los laboratorios
- **datasets**: Contiene los datasets necesarios para los laboratorios
- **slide_deck**: Contiene las slides de la presentacion en formato PPT
- **slide deck(pdf)**: Contiene las slides de la presentacion en formato PDF

# Cómo usar los laboratorios

- Crear una cuenta en community edition de Databricks: https://community.cloud.databricks.com/
- Una vez regidstrados, crear un cluster para la ejecucion de los notebooks
- Subir los notebooks al workspace, en nuestra carpeta personal
- Subir los datasets de la carpeta **datasets** a la ruta dbfs:/FileStore/tables/ (Utilizad el boton de DBFS->Upload en la UI de Databricks)
- Subir el fichero table_metadata.yml a la ruta dbfs:/FileStore/tables/ (Utilizad el boton de DBFS->Upload en la UI de Databricks)
- Ejecutad los notebooks siguiendo esta secuencia:
   - 1. data_engineering_nb_bronze.py 
   - 2. data_engineering_nb_silver.py
    - 3. data_engineering_nb_gold.py
- Para comprobar como funciona el historico de versiones de Delta Lake:
    - Ejecutad el notebook **data_engineering_delta_history.py**