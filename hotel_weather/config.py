from dotenv import load_dotenv
import os

load_dotenv()

OPEN_CAGE_API_KEY = os.getenv("OPEN_CAGE_API_KEY")
RESTAURANT_FILE_PATH = os.getenv("RESTAURANT_FILE_PATH")
WEATHER_FILE_PATH = os.getenv("WEATHER_FILE_PATH")
OUTPUT_PATH = os.getenv("OUTPUT_PATH")

if not OPEN_CAGE_API_KEY:
    raise ValueError("Missing OPEN_CAGE_API_KEY in .env file")
