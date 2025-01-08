from avro.schema import parse
import avro.datafile
import avro.io

def define_avro_schema():
    # Define the Avro schema for the BitcoinPrice record
    return parse('''
    {
        "type": "record",
        "name": "BitcoinPrice",
        "fields": [
            {"name": "Date", "type": "string"},
            {"name": "Open", "type": "float"},
            {"name": "High", "type": "float"},
            {"name": "Low", "type": "float"},
            {"name": "Close", "type": "float"},
            {"name": "Volume", "type": "float"},
            {"name": "Market Cap", "type": "float"}
        ]
    }
    ''')

def convert_to_avro(data, schema, output_file_path):
    # Write the DataFrame to an Avro file
    with open(output_file_path, 'wb') as avro_file:
        writer = avro.datafile.DataFileWriter(avro_file, avro.io.DatumWriter(), schema)
        for _, row in data.iterrows():
            writer.append(row.to_dict())
        writer.close()

def validate_avro_file(file_path):
    # Read the Avro file and print the schema and sample records
    with open(file_path, 'rb') as avro_file:
        reader = avro.datafile.DataFileReader(avro_file, avro.io.DatumReader())
        print("Schema:", reader.meta['avro.schema'])
        print("Sample Records:")
        for i, record in enumerate(reader):
            print(record)
            if i == 4:
                break
        reader.close()
