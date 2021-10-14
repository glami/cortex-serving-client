import importlib
import json
import os
import re

from fastapi import FastAPI
from starlette import status
from starlette.requests import Request
from starlette.responses import PlainTextResponse, JSONResponse


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
def handle_post_or_batch(request: Request):
    response = app.predictor.predict(request.state.payload)
    if response is not None:
        return response


@app.post("/on-job-complete")
def on_job_complete():
    app.predictor.on_job_complete()


@app.middleware("http")
async def parse_payload(request: Request, call_next):
    content_type = request.headers.get("content-type", "").lower()

    if content_type.startswith("text/plain"):
        try:
            charset = "utf-8"
            matches = re.findall(r"charset=(\S+)", content_type)
            if len(matches) > 0:
                charset = matches[-1].rstrip(";")
            body = await request.body()
            request.state.payload = body.decode(charset)
        except Exception as e:
            return PlainTextResponse(content=str(e), status_code=400)
    elif content_type.startswith("multipart/form") or content_type.startswith(
        "application/x-www-form-urlencoded"
    ):
        try:
            request.state.payload = await request.form()
        except Exception as e:
            return PlainTextResponse(content=str(e), status_code=400)
    elif content_type.startswith("application/json"):
        try:
            request.state.payload = await request.json()
        except json.JSONDecodeError as e:
            return JSONResponse(content={"error": str(e)}, status_code=400)
    else:
        request.state.payload = await request.body()

    return await call_next(request)