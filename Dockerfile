from python:3.8-slim

COPY . /tmp/src

RUN groupadd -r altimeter && useradd -r -s /bin/false -g altimeter altimeter
RUN pip install -r /tmp/src/requirements.txt
RUN cd /tmp/src && python setup.py install && rm -rf /tmp/src

STOPSIGNAL SIGTERM

USER altimeter
