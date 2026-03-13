# Pre-install Python deps so Jenkins can run scripts without pip (avoids network in container)
FROM python:3.12-slim

RUN pip install --no-cache-dir \
    psycopg2-binary \
    requests \
    beautifulsoup4 \
    google-api-python-client \
    google-auth-httplib2 \
    google-auth-oauthlib

WORKDIR /scripts
