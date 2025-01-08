import pandas as pd
import numpy as np

def load_csv(file_path):
    return pd.read_csv(file_path)

def preprocess_data(data):
    # Clean the Volume and Market Cap columns by handling invalid values and converting to numeric.
    data['Volume'] = data['Volume'].replace('-', np.nan)
    data['Market Cap'] = data['Market Cap'].replace('-', np.nan)

    data['Volume'] = data['Volume'].str.replace(',', '').astype(float)
    data['Market Cap'] = data['Market Cap'].str.replace(',', '').astype(float)

    return data
