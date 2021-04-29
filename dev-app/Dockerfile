# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.8-slim-buster

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1


# Install  msodbc header
RUN apt-get update && \
    apt-get -y install curl gnupg2
RUN su && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get -y install msodbcsql17

# Install python build tools
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install git gcc mono-mcs python3-dev python3-pip libxml2-dev libxslt1-dev zlib1g-dev g++ unixodbc-dev libgssapi-krb5-2 && \
    rm -rf /var/lib/apt/lists/*

# Install interface classes
## Pull latest class source
WORKDIR /ims-lib
# RUN git clone https://github.com/DarrylBrysonDev0/ms-interface-lib.git && \
#     python -m pip install -r ./ms-interface-lib/requirements.txt && \
#     python -m pip install ./ms-interface-lib
COPY ./ms-interface-lib/requirements.txt /ims-lib/ms-interface-lib/requirements.txt
RUN python -m pip install -r ./ms-interface-lib/requirements.txt

COPY ./ms-interface-lib /ims-lib/ms-interface-lib
RUN python -m pip install ./ms-interface-lib

# Install application requirements
COPY app/requirements.txt .
RUN python -m pip install -r requirements.txt

# Create data directory
WORKDIR /data

WORKDIR /app
COPY ./app /app

# Switching to a non-root user, please refer to https://aka.ms/vscode-docker-python-user-rights
RUN useradd appuser && \
    chown -R appuser /app && \
    chown -R appuser /data
USER appuser

# USER 0

# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "version"]

