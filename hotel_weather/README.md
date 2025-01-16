# **Restaurant and Weather ETL Pipeline**

## **Overview**
This project is an ETL (Extract, Transform, Load) pipeline that processes restaurant and weather data:
1. **Extract**:
   - Reads restaurant data (CSV) and weather data (Parquet).
2. **Transform**:
   - Fills missing latitude (`lat`) and longitude (`lng`) values in the restaurant dataset using the [OpenCage Geocoding API](https://opencagedata.com/).
   - Generates GeoHash for both datasets for spatial joining.
   - Enriches the restaurant dataset with weather data.
3. **Load**:
   - Saves the enriched data in Parquet format, partitioned by `year`, `month`, and `day`.

## **Project Structure**
```plaintext
restaurant_weather_etl/
├── main.py                   # Main script to run the ETL pipeline
├── config.py                 # Configuration with .env integration
├── data_processing.py        # Functions for data cleaning and transformation
├── utils.py                  # Utility functions for geohashing and geocoding
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (API key, file paths)
└── README.md                 # Documentation (this file)
```

### **Setting Up the `.env` File**

Create a `.env` file in the project root directory and add the following configuration:

```env
# OpenCage API key for geocoding
OPEN_CAGE_API_KEY=your_opencage_api_key

# Path to the restaurant dataset (CSV format)
RESTAURANT_FILE_PATH=path/to/restaurants.csv

# Path to the weather dataset directory (Parquet format)
WEATHER_FILE_PATH=path/to/weather

# Path to save the enriched data (Parquet format)
OUTPUT_PATH=output/enriched_data



