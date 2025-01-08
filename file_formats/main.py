from csv_handler import load_csv, preprocess_data
from avro_converter import define_avro_schema, convert_to_avro,validate_avro_file

if __name__ == "__main__":
    data = load_csv('bitcoin_price_Training - Training.csv')
    data = preprocess_data(data)

    schema = define_avro_schema()

    avro_file_path = 'bitcoin_price_data.avro'
    convert_to_avro(data, schema, avro_file_path)

    validate_avro_file(avro_file_path)
    print("AVRO conversion complete.")
