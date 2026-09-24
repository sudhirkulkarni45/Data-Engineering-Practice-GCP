import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
beam_option=PipelineOptions(
    runner="DataflowRunner",
    project="project-b5a1bfde-12d9-4a81-8bc",
    region="us-central1",
    job_name="my-first-beam-2",
    temp_location="gs://sudhir-aug26/temp/",
    staging_location="gs://sudhir-aug26/staging/"
)
file_path="gs://sudhir-aug26/passesnger_details.csv"
with beam.Pipeline(options=beam_option) as pipeline:
    pc1= pipeline | "Take data from GCS" >> beam.io.ReadFromText(file_path)
    pc1 | "print Data">> beam.Map(print)