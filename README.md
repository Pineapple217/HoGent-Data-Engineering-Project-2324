# DEP2-GROEP-1

## SQL Server Management Studio

### Server starten

```sh
pwd
# ./DEP2-GROEP-1
cd sqlserver
docker-compose up -d
```

Als er problemen zijn check of de _end of line sequence_ op `LF` staat.

Connecteren met SQL server:

![SSMS](img/SSMS.png)

# Python setup

```console
python -m venv venv
.\venv\Scripts\activate
pip install -r .\requirements.txt
```

## Python scripts usage

```
python .\src\main.py
```

De helpt tekst die ge krijgt spreekt voor zich.

# .ENV

Voorbleed

```env
DB_URL=mssql+pyodbc://I-400/voka?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server
DATA_PATH=../Data
```

# Data folder

alles csv's in de de root van de DATA_PATH folder
