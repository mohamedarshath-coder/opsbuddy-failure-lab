# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Geo Enrichment
# MAGIC Resolves customer shipping addresses to precise lat/long coordinates for the
# MAGIC logistics team's regional delivery-time modeling.

# COMMAND ----------

# FIX (SCRUM-83): geopy is not part of this cluster's base image and isn't declared in the
# job/cluster library configuration. Install it explicitly for this notebook run and restart
# the Python process so the import below picks it up.
# MAGIC %pip install geopy

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="insightops_logistics")

addresses = ["1 Infinite Loop, Cupertino, CA", "350 5th Ave, New York, NY"]
for addr in addresses:
    location = geolocator.geocode(addr)
    print(addr, "->", location.latitude, location.longitude)
