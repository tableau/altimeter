FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

COPY ./requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt
RUN rm /tmp/requirements.txt
COPY ./services/qj/requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt
RUN rm /tmp/requirements.txt

COPY ./services/qj/gunicorn_conf.py /app
COPY ./services/qj/main.py /app
COPY ./altimeter /app/altimeter
