class PythonPredictor:
    def __init__(self, config):
        self.config = config
        print(f'Initialized the model with config: {config}')

    def predict(self, payload):
        print('Predicting!')
        return f"Predictor config: {self.config} Received payload type: {type(payload)}, payload: {payload}"

