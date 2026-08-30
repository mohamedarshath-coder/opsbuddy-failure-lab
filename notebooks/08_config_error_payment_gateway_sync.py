# Databricks notebook source
# MAGIC %md
# MAGIC # Payment Gateway Reconciliation Sync
# MAGIC Pulls the latest settled-transactions export from the payment processor's API
# MAGIC and reconciles it against internal order records.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve the API credential
# MAGIC The processor's API key lives in a Databricks secret scope, never hardcoded.

# COMMAND ----------

# BUG: "payments_prod" was this scope's name in the OLD workspace, before a workspace
# migration a few months ago -- the new workspace's scope was never actually created
# under that name (or was created with a different name), so this fails with
# "Secret scope 'payments_prod' does not exist" rather than a bad API call.
api_key = dbutils.secrets.get(scope="payments_prod", key="reconciliation_api_key")

import requests

response = requests.get(
    "https://payments.example.com/api/v1/settled-transactions",
    headers={"Authorization": f"Bearer {api_key}"},
)
print(response.status_code)
