FROM mcr.microsoft.com/mssql/server:2019-latest

# To be able to make directories
USER root 

RUN mkdir -p /sqlserver

WORKDIR /sqlserver

COPY sqlserver-setup.sh /sqlserver/
COPY scripts/sql/init.sql /sqlserver/scripts/sql/

RUN chmod +x /sqlserver/sqlserver-setup.sh

CMD ["./sqlserver-setup.sh"]

# Build: docker build -t sqlserver-image .
# Logs: docker logs sqlserver

# Sources:
# https://hub.docker.com/_/microsoft-mssql-server