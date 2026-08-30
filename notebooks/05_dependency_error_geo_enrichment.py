# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Geo Enrichment
# MAGIC Resolves customer shipping addresses to precise lat/long coordinates for the
# MAGIC logistics team's regional delivery-time modeling.

# COMMAND ----------

# BUG: geopy is a real, commonly-used geocoding library, but it isn't part of this
# cluster's base image and was never added to the cluster's library configuration --
# a very common real mistake when a notebook is copied from a different cluster/
# workspace that happened to have it pre-installed.
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="insightops_logistics")

addresses = ["1 Infinite Loop, Cupertino, CA", "350 5th Ave, New York, NY"]
for addr in addresses:
    location = geolocator.geocode(addr)
    print(addr, "->", location.latitude, location.longitude)
