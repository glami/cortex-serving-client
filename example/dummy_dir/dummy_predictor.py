class PythonPredictor:
    def __init__(self, config):
        print('Initialized the model!')

    def predict(self, payload):
        print('Predicting!')
        return dict(yes=True, **payload)

