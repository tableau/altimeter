FROM public.ecr.aws/lambda/python:3.8

COPY . /tmp/src
COPY bin/altimeter_lambda.py "${LAMBDA_TASK_ROOT}"

RUN pip install -r /tmp/src/requirements.txt -t "${LAMBDA_TASK_ROOT}"
RUN cd /tmp/src && python setup.py install && rm -rf /tmp/src

STOPSIGNAL SIGTERM

CMD ["altimeter_lambda.lambda_handler"]
