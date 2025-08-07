import os
import io
import requests
import xarray as xr
import pandas as pd
import sqlite3
from datetime import datetime


START_YEAR = 2015
END_YEAR = 2025
DB_PATH = './api_mock/api_data/chirps_data.db'
AGRO_EXPORT_HUBS = [
    {"hub": "Sorriso", "state": "MT", "commodity": "Soja", "lat": -12.54, "lon": -55.71},
    {"hub": "Rio Verde", "state": "GO", "commodity": "Soja", "lat": -17.79, "lon": -50.92},
    {"hub": "Formoza do Rio Preto", "state": "BA", "commodity": "Soja", "lat": -11.05, "lon": -45.19},
    {"hub": "Sao Desiderio", "state": "BA", "commodity": "Soja", "lat": -12.36, "lon": -44.97},
    {"hub": "Nova Mutum", "state": "MT", "commodity": "Soja", "lat": -13.83, "lon": -56.09},
    {"hub": "Sapezal", "state": "MT", "commodity": "Soja", "lat": -13.55, "lon": -58.82},
    {"hub": "Diamantino", "state": "MT", "commodity": "Soja", "lat": -14.41, "lon": -56.45},
    {"hub": "Campo Novo do Perecis", "state": "MT", "commodity": "Soja", "lat": -13.67, "lon": -57.89},
    {"hub": "Nova Ubirata", "state": "MT", "commodity": "Soja", "lat": -12.99, "lon": -55.25},
    {"hub": "Querencia", "state": "MT", "commodity": "Soja", "lat": -12.6, "lon": -52.21},
]
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def setup_database():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS precipitation (
            date TEXT,
            hub TEXT,
            state TEXT,
            latitude REAL,
            longitude REAL,
            precipitation_mm REAL,
            PRIMARY KEY (date, hub)
        )
    ''')
    conn.commit()
    conn.close()

def fetch_and_store_chirps_data():
    print(f"INICIANDO INGESTÃO DE DADOS (CHIRPS) PARA O BANCO DE DADOS...")
    setup_database()
    conn = sqlite3.connect(DB_PATH)

    for year in range(START_YEAR, END_YEAR + 1):
        print(f"\n{'='*20} PROCESSANDO ANO: {year} {'='*20}")
        url_chirps = f"https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/chirps-v2.0.{year}.days_p05.nc"

        try:
            print(f"Baixando: {url_chirps}")
            response = requests.get(url_chirps, headers=HEADERS)
            response.raise_for_status()
            
            file_obj = io.BytesIO(response.content)
            ds = xr.open_dataset(file_obj, engine='h5netcdf')

            for location in AGRO_EXPORT_HUBS:
                print(f"  -> Extraindo dados para {location['hub']}")
                precip_series = ds['precip'].sel(latitude=location['lat'], longitude=location['lon'], method='nearest')
                precip_df = precip_series.to_dataframe().reset_index()
                precip_df.rename(columns={'time': 'date', 'precip': 'precipitation_mm'}, inplace=True)
                
                precip_df['hub'] = location['hub']
                precip_df['state'] = location['state']
                precip_df['latitude'] = location['lat']
                precip_df['longitude'] = location['lon']
                precip_df['date'] = pd.to_datetime(precip_df['date']).dt.strftime('%Y-%m-%d')
                
                precip_df[['date', 'hub', 'state', 'latitude', 'longitude', 'precipitation_mm']].to_sql('precipitation', conn, if_exists='append', index=False)
        
        except Exception as e:
<<<<<<< HEAD
            print(f" ERRO AO PROCESSAR O ANO {year}: {e}")
=======
            print(f"ERRO AO PROCESSAR O ANO {year}: {e}")
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2

    conn.close()
    print(f"\n{'='*50}\nINGESTÃO PARA O BANCO DE DADOS CONCLUÍDA.\n{'='*50}")

if __name__ == "__main__":
    fetch_and_store_chirps_data()