import io
import pickle
import s3


class PythonPredictor:
    def __init__(self, config):
        print(f"Init with config {config}")

    def predict(self, payload):
        processed_batch = dict(yes=True, payload=payload)

        s3_path = f'test/batch-yes/{sum(payload)}.json'
        with io.BytesIO() as fp:
            pickle.dump(processed_batch, fp)
            fp.seek(0)
            s3.upload_fileobj(fp, s3_path)
        print(f"Uploaded processed batch to S3: {s3_path}")

    def on_job_complete(self):
        print("Processed all the uploaded batches.")
