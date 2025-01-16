import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import geohash
from config import OPEN_CAGE_API_KEY


# Function to fetch coordinates from OpenCage API
def fetch_coordinates(country, city):
    if not country or not city:
        return None, None
    location_query = f"{city}, {country}"
    url = f"https://api.opencagedata.com/geocode/v1/json?q={location_query}&key={OPEN_CAGE_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data["results"]:
            location = data["results"][0]["geometry"]
            return location["lat"], location["lng"]
    return None, None


# Spark UDF to use the API
@udf(returnType=StringType())
def fetch_coordinates_udf(country, city):
    lat, lng = fetch_coordinates(country, city)
    return f"{lat},{lng}" if lat and lng else None


# UDF for GeoHash generation
@udf(returnType=StringType())
def generate_geohash(lat, lng):
    if lat is not None and lng is not None:
        return geohash.encode(lat, lng, precision=4)
    return None
