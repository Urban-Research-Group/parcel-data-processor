FROM python:3.13-rc-alpine

RUN apk add --no-cache --update \
    python3 python3-dev gcc \
    gfortran musl-dev \
    libffi-dev openssl-dev \
    build-base

RUN pip install --upgrade pip

WORKDIR /src

ADD . /src

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

CMD ["python", "main.py"]