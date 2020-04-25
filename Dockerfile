from python:3.8-slim

COPY . /tmp/src

RUN pip install -r /tmp/src/requirements.txt
RUN cd /tmp/src && python setup.py install

STOPSIGNAL SIGTERM
