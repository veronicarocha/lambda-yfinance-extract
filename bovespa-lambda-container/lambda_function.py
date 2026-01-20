import yfinance as yf
import pandas as pd
import boto3
import requests
from datetime import datetime
import io
import time
import pytz
import os
import re

BRAPI_KEY = os.environ.get("BRAPI_KEY")

def clean_column_name(col_name):
    """Limpar nomes de colunas para serem compatíveis com Spark"""
    if isinstance(col_name, tuple):
        # Se for uma tupla, pegar o primeiro elemento
        col_name = str(col_name[0])
    
    # Converter para string
    col_name = str(col_name)
    
    col_name = re.sub(r'[\(\)\'\"\s]', '_', col_name)
    
    col_name = col_name.replace(',', '_').replace('.', '_').replace(' ', '_')
    
    col_name = re.sub(r'_+', '_', col_name).strip('_').lower()
    
    return col_name

def lambda_handler(event, context):
    tickers = ["PETR4", "VALE3", "ITUB4", "ABEV3"]
    bucket = "fiap-bovespa-id_conta"
    br_tz = pytz.timezone("America/Sao_Paulo")
    data_hoje = datetime.now(br_tz).strftime('%Y-%m-%d')
    s3 = boto3.client("s3")

    for ticker in tickers:
        df = None
        print(f"\n{'='*50}")
        print(f" Processando {ticker}")
        print(f"{'='*50}")

        try:
            print(f"1- Baixando {ticker}.SA...")
            # Método 1: Usar yfinance de forma direta
            stock_data = yf.download(
                f"{ticker}.SA",
                period="7d",
                interval="1d",
                progress=False,
                auto_adjust=False
            )
            
            if stock_data is not None and not stock_data.empty:
                print(f"OK- Dados obtidos para {ticker}")
                
                # Converter para DataFrame regular
                df = stock_data.copy()
                
                df = df.reset_index()
                
                print(f"   Colunas originais: {df.columns.tolist()}")
                print(f"   Tipo das colunas: {type(df.columns)}")
                
                # LIMPAR NOMES DE COLUNAS
                cleaned_columns = []
                for col in df.columns:
                    cleaned_col = clean_column_name(col)
                    # Mapear para nomes padrão
                    if cleaned_col == 'adj_close' or cleaned_col == 'adjclose':
                        cleaned_col = 'adj_close'
                    elif cleaned_col == 'date':
                        cleaned_col = 'date'
                    cleaned_columns.append(cleaned_col)
                
                df.columns = cleaned_columns
                print(f"   Colunas limpas: {df.columns.tolist()}")

        except Exception as e:
            print(f"! Erro com yfinance para {ticker}: {e}")
            import traceback
            traceback.print_exc()

        if df is None or df.empty:
            print(f"Tentando fallback com brapi.dev para {ticker}")
            try:
                url = f"https://brapi.dev/api/quote/{ticker}?range=7d&interval=1d&token={BRAPI_KEY}"
                response = requests.get(url)
                data = response.json()

                candles = data.get("results", [{}])[0].get("historicalDataPrice", [])
                if candles:
                    df = pd.DataFrame([{
                        "date": pd.to_datetime(c["date"], unit="s"),
                        "open": c.get("open"),
                        "high": c.get("high"),
                        "low": c.get("low"),
                        "close": c.get("close"),
                        "volume": c.get("volume", None)
                    } for c in candles])
                    df = df.sort_values("date")
                    print(f"Sucesso: Dados obtidos com brapi.dev para {ticker}")
                else:
                    print(f"Erro: brapi.dev retornou dados vazios para {ticker}")
                    continue
            except Exception as e:
                print(f"Erro com brapi.dev para {ticker}: {e}")
                continue

        try:
            # GARANTIR COLUNAS PADRÃO
            df["ticker"] = ticker
            df["data_extraida"] = data_hoje
            
            # Formatar data
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            else:
                df['date'] = data_hoje
            
            # CRIAR COLUNAS PADRÃO SE NÃO EXISTIREM
            required_cols = {
                'open': 0.0,
                'high': 0.0,
                'low': 0.0,
                'close': 0.0,
                'volume': 0,
                'adj_close': 0.0
            }
            
            for col, default_val in required_cols.items():
                if col not in df.columns:
                    print(f" Coluna '{col}' não encontrada, criando com valor default")
                    df[col] = default_val
                else:
                    # Garantir tipo numérico
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(default_val)
            
            # ORDENAR COLUNAS
            base_cols = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'ticker', 'data_extraida']
            existing_cols = [col for col in base_cols if col in df.columns]
            df = df[existing_cols]
            
            print(f" ESTRUTURA FINAL:")
            print(f"   Colunas: {df.columns.tolist()}")
            print(f"   Shape: {df.shape}")
            print(f"   Tipos: {df.dtypes.to_dict()}")
            
            # Salvar no S3
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)

            s3_prefix = f"raw/ticker={ticker}/data={data_hoje}/dados.parquet"
            s3.upload_fileobj(buffer, bucket, s3_prefix)
            print(f" Upload concluído para {ticker}")
            
        except Exception as e:
            print(f" Erro ao processar/enviar {ticker}: {e}")
            import traceback
            traceback.print_exc()

        time.sleep(2)

    return {"status": "concluído"}