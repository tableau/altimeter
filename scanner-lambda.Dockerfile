FROM public.ecr.aws/lambda/python:3.8

COPY . /tmp/src
COPY bin/aws2n.py "${LAMBDA_TASK_ROOT}"

RUN pip install -r /tmp/src/requirements.txt
RUN cd /tmp/src && python setup.py install && rm -rf /tmp/src

STOPSIGNAL SIGTERM

CMD ["aws2n.lambda_handler"]
