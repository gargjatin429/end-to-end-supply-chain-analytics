FROM apache/airflow:2.9.3-python3.12

USER root

# Install MS SQL Server ODBC Drivers
RUN apt-get update && \
    apt-get install -y curl apt-transport-https gnupg2 unixodbc-dev && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

USER airflow

# Install our pipeline requirements
RUN pip install --no-cache-dir \
    polars \
    s3fs==2024.6.1 \
    pyodbc==5.1.0 \
    sqlalchemy==2.0.31 \
    pytest==8.3.2
