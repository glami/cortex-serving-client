class PythonPredictor:
    def __init__(self, config):
        print(f"Initted predictor with config: {config}")
        pass

    def predict(self, payload):
        return dict(yes=True)
