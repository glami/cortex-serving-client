class PythonPredictor:
    def __init__(self, config):
        print('Inited!')

    def predict(self, payload):
        print('Predicting')
        return dict(yes=True, **payload)
