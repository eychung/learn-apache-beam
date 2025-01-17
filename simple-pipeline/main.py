import apache_beam as beam

def run_pipeline():
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'CreateData' >> beam.Create([1, 2, 3, 4, 5])
            | 'MultiplyByTwo' >> beam.Map(lambda x: x * 2)
            | 'Print' >> beam.Map(print)
        )


# Run the pipeline
if __name__ == '__main__':
    run_pipeline()
