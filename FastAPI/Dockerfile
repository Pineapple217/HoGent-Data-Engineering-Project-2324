# Dockerfile

# pull the official docker image
FROM python:3.11.1-slim

RUN apt-get update && apt-get install -y --no-install-recommends build-essential unixodbc curl gnupg2 
RUN apt-get clean
# set work directory
WORKDIR /app

# set env variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies
COPY requirements.txt .

RUN pip install -r requirements.txt

RUN apt-get install apt-transport-https
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
# copy project
COPY . .



