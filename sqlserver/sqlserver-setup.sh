#!/bin/bash

echo "Starting SQL Server"
# Run SQL Server process in the background and wait for it to be ready for connections
/opt/mssql/bin/sqlservr & sleep 10
echo "SQL Server started"

echo "Creating database"
# Create database by running the init.sql script
if [ -f /sqlserver/scripts/sql/init.sql ]; then
    /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P Password123* -d master -i /sqlserver/scripts/sql/init.sql
else
    exit 1
fi
echo "Database created"

echo "Start logging to keep container running..."
touch /dev/logs && tail -f /dev/logs

# Sources:
# https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-troubleshooting-guide?view=sql-server-ver16
# https://stackoverflow.com/questions/51039052/how-to-run-a-setup-script-on-a-docker-sql-server-image