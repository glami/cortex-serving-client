class PythonPredictor:
    def __init__(self, config):
        raise RuntimeError('Intentional testing exception')

    def predict(self, payload):
        return dict(yes=True)
