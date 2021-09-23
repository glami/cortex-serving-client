FROM python:3.6

ENV PYTHONUNBUFFERED TRUE

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY download_source_code.py /tmp/

CMD python /tmp/download_source_code.py