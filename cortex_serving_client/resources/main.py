import importlib
import json
import os

from fastapi import FastAPI, Body
from starlette import status
from starlette.responses import PlainTextResponse


def get_class(module_path: str, class_name: str):
    """
    @param module_path: cls.__module__ = 'predictors.my_predictor'
    @param class_name: cls.__name__ = 'MyPythonPredictor'
    @return: The imported class ready to be instantiated.
    """
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


app = FastAPI()
app.ready = False
app.predictor = None


@app.on_event("startup")
def startup():
    with open("predictor_config.json", "r") as f:
        config = json.load(f)

    predictor_file_path = os.environ['CSC_PREDICTOR_PATH']
    predictor_cls_name = os.environ['CSC_PREDICTOR_CLASS_NAME']
    predictor_cls = get_class(predictor_file_path, predictor_cls_name)
    app.predictor = predictor_cls(config)
    app.ready = True


@app.get("/healthz")
def healthz():
    if app.ready:
        return PlainTextResponse("ok")
    return PlainTextResponse("service unavailable", status_code=status.HTTP_503_SERVICE_UNAVAILABLE)


@app.post("/")
def handle_post_or_batch(payload=Body(...)):
    response = app.predictor.predict(payload)
    if response is not None:
        return response


@app.post("/on-job-complete")
def on_job_complete():
    app.predictor.on_job_complete()