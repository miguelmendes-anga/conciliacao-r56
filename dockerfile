FROM public.ecr.aws/lambda/python:3.10
RUN pip install awswrangler==3.7.3
COPY lambda_function.py ./
COPY src/ ./src/
CMD [ "lambda_function.lambda_handler" ]