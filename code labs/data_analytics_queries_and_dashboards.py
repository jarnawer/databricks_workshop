# COMMAND ----------
# MAGIC %md
# MAGIC ## SQL Analytics
# MAGIC Ahora realizaremos consultas sobre las tablas que hemos creado en la capa Gold, para ello utilizaremos SQL.
# MAGIC Además, exploraremos las posibilidades de visualización que nos ofrece Databricks.
# MAGIC 
# MAGIC

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pilotos con mas victorias
# MAGIC
# MAGIC Vamos a realizar una consulta sobre la tabla **most_awarded_drivers** que hemos creado en la capa Gold.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM gold.most_awarded_drivers order by total_wins desc


# COMMAND ----------
# MAGIC %md
# MAGIC ## Pilotos con mas pole positions
# MAGIC
# MAGIC Vamos a realizar una consulta sobre la tabla **most_pole_position_drivers** que hemos creado en la capa Gold.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM gold.most_pole_position_drivers order by total_pole_positions desc