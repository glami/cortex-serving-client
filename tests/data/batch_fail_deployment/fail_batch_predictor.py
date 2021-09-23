class PythonPredictor:
    def __init__(self, config):
        raise RuntimeError(f"Intentional testing error!")

    def predict(self, payload):
        return dict(yes=True, payload=payload)

    def on_job_complete(self):
        print("Processed all the uploaded batches.")
