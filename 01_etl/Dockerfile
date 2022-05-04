FROM python:3.9.6-alpine

RUN pip install --upgrade pip
COPY requirments.txt .
RUN pip install -r requirments.txt
COPY . .