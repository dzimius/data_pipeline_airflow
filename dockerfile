FROM apache/airflow:2.5.1-python3.7

USER root

# System dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    gcc \
    g++ \
    libpq-dev \
    python3-dev

# Microsoft SQL Server repo
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list \
    -o /etc/apt/sources.list.d/mssql-release.list

# Install MS SQL ODBC driver
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17

# Install pyodbc as airflow user
USER airflow
RUN pip install --no-cache-dir pyodbc