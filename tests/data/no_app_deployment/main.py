import json

from fastapi import FastAPI, status, Body
from fastapi.responses import PlainTextResponse
from yes_predictor import PythonPredictor

my_app = FastAPI()
my_app.ready = False
my_app.predictor = None


@my_app.on_event("startup")
def startup():
    with open("predictor_config.json", "r") as f:
        config = json.load(f)
    my_app.predictor = PythonPredictor(config)
    my_app.ready = True


@my_app.get("/healthz")
def healthz():
    if my_app.ready:
        return PlainTextResponse("ok")
    return PlainTextResponse("service unavailable", status_code=status.HTTP_503_SERVICE_UNAVAILABLE)


@my_app.post("/")
def post_handler(payload: dict = Body(...)):
    return my_app.predictor.predict(payload)
