FROM python:3.11-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install GDAL and other geospatial libraries
RUN apt-get update && apt-get install -y build-essential binutils libproj-dev gdal-bin libgdal-dev libsqlite3-mod-spatialite 

# Copy project files
COPY . .

RUN python -m pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install

