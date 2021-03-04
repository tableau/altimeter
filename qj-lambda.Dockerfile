FROM public.ecr.aws/lambda/python:3.8

COPY . /tmp/src
COPY bin/queryjob_lambda.py "${LAMBDA_TASK_ROOT}"

RUN pip install -r /tmp/src/requirements.txt
RUN cd /tmp/src && pip install .[qj] && rm -rf /tmp/src

STOPSIGNAL SIGTERM

CMD ["queryjob_lambda.lambda_handler"]
