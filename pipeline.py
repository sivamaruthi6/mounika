import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import csv
import json

class ParseCSV(beam.DoFn):
    def process(self, element):
        """
        element = line from CSV
        return dict with column names
        """
        row = list(csv.reader([element]))[0]
        return [{
            "id": row[0],
            "name": row[1],
            "department": row[2],
            "salary": row[3]
        }]

def run():
    pipeline_options = PipelineOptions(
        save_main_session=True,
        streaming=False
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True

    input_file = pipeline_options.view_as(PipelineOptions).lookup("input")
    output_table = pipeline_options.view_as(PipelineOptions).lookup("output")

    with beam.Pipeline(options=pipeline_options) as p:

        (p
         | "Read CSV" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
         | "Parse CSV" >> beam.ParDo(ParseCSV())
         | "Write to BQ" >> beam.io.WriteToBigQuery(
                output_table,
                schema={
                    "fields": [
                        {"name": "id", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "department", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "salary", "type": "STRING", "mode": "NULLABLE"}
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
         )
        )

if __name__ == "__main__":
    run()



monika
