# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

print(dbutils.secrets.listScopes())
print(dbutils.secrets.list("formula1-scope"))

# COMMAND ----------

dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")

# COMMAND ----------

# for x in dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id"):
#     print(x)

# COMMAND ----------

storage_account_name = "formula1dllearning"
client_id = dbutils.secrets.get(scope='formula1-scope', key='databricks-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='databricks-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='databricks-app-client-secret')



# dbutils.fs.unmount("/mnt/formula1dllearning/raw")
# dbutils.fs.unmount("/mnt/formula1dllearning/processed")
# dbutils.fs.unmount("/mnt/formula1dllearning/presentation")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container):
    dbutils.fs.mount(
    source = f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("raw")
mount_adls("processed")
mount_adls("presentation")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dllearning/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dllearning/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dllearning/presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dllearning/demo")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

