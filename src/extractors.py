import os
import pandas as pd
import requests
from datetime import datetime

def extract_goes(save_path: str, total_day: int = 3) -> None:
    """
    Extrai e atualiza os dados do satélite GOES e salva em formato Excel, evitando duplicatas.

    Args:
        save_path (str): Caminho onde o arquivo será salvo.
        total_day (int): Quantos dias de dados baixar (máximo suportado: 3 dias).
    """
    url = f"https://services.swpc.noaa.gov/json/goes/primary/integral-protons-{total_day}-day.json"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Erro ao baixar dados GOES. Código: {response.status_code}")
        return

    df_new = pd.DataFrame(response.json())
    df_new["time_tag"] = pd.to_datetime(df_new["time_tag"], errors="coerce").dt.tz_localize(None)
    df_new = df_new[["time_tag", "satellite", "energy", "flux"]]

    if os.path.exists(save_path):
        df_existing = pd.read_excel(save_path)
        df_existing["time_tag"] = pd.to_datetime(df_existing["time_tag"], errors="coerce")
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined.drop_duplicates(subset=["time_tag", "satellite", "energy"], inplace=True)
    else:
        df_combined = df_new

    df_combined.sort_values("time_tag", inplace=True)
    df_combined.to_excel(save_path, index=False)
    print(f"Dados GOES salvos/atualizados em: {save_path}")

def extract_ace_all(output_dir: str) -> None:
    """
    Extrai e atualiza os dados dos quatro endpoints do satélite ACE
    (EPAM, MAG, SIS, SWEPAM) e salva em arquivos Excel separados.

    Args:
        output_dir (str): Caminho da pasta onde os arquivos serão salvos.
    """
    ACE_URLS = {
        "ace_epam_5m.xlsx": "https://services.swpc.noaa.gov/json/ace/epam/ace_epam_5m.json",
        "ace_mag_1h.xlsx": "https://services.swpc.noaa.gov/json/ace/mag/ace_mag_1h.json",
        "ace_sis_5m.xlsx": "https://services.swpc.noaa.gov/json/ace/sis/ace_sis_5m.json",
        "ace_swepam_1h.xlsx": "https://services.swpc.noaa.gov/json/ace/swepam/ace_swepam_1h.json"
    }

    def detectar_coluna_tempo(df):
        for col in df.columns:
            if col.lower() in ['time_tag', 'timestamp', 'time', 'date']:
                return col
        return None

    os.makedirs(output_dir, exist_ok=True)

    for filename, url in ACE_URLS.items():
        save_path = os.path.join(output_dir, filename)
        print(f"\nBaixando dados de: {filename}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            df_new = pd.DataFrame(response.json())
        except Exception as e:
            print(f"[ERRO] Falha ao acessar {url}: {e}")
            continue

        if df_new.empty:
            print(f"[-] Dados vazios para: {filename}")
            continue

        col_tempo = detectar_coluna_tempo(df_new)
        if not col_tempo:
            print(f"[-] Coluna de tempo não encontrada em {filename}, pulando.")
            continue

        df_new[col_tempo] = pd.to_datetime(df_new[col_tempo], errors='coerce')

        if os.path.exists(save_path):
            df_existing = pd.read_excel(save_path)
            df_existing[col_tempo] = pd.to_datetime(df_existing[col_tempo], errors='coerce')
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=[col_tempo], inplace=True)
        else:
            df_combined = df_new

        df_combined.sort_values(by=col_tempo, inplace=True)
        df_combined.to_excel(save_path, index=False)
        print(f" {filename} atualizado com {len(df_new)} novos registros")


def extract_kp(save_path: str) -> None:
    """
    Extrai e atualiza o índice Kp (1 minuto) e salva em formato Excel.

    Args:
        save_path (str): Caminho onde o arquivo será salvo.
    """
    url = "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Erro ao baixar índice Kp. Código: {response.status_code}")
        return

    df_new = pd.DataFrame(response.json())
    df_new["time_tag"] = pd.to_datetime(df_new["time_tag"], errors="coerce", utc=True).dt.tz_localize(None)

    if os.path.exists(save_path):
        df_existing = pd.read_excel(save_path)
        df_existing["time_tag"] = pd.to_datetime(df_existing["time_tag"], errors="coerce")
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined.drop_duplicates(subset=["time_tag"], inplace=True)
    else:
        df_combined = df_new

    df_combined.sort_values("time_tag", inplace=True)
    df_combined.to_excel(save_path, index=False)
    print(f" Índice Kp salvo/atualizado em: {save_path}")

def extract_kp_gfz_xlsx(start: str, end: str, save_path: str, index: str = "Kp", status: str = "def"):
    """
    Extrai e atualiza o índice Kp do GFZ Potsdam e salva em formato Excel (.xlsx).
    """
    url = "https://kp.gfz.de/app/json/"
    params = {
        "start": start,
        "end": end,
        "index": index,
        "status": status
    }
    try:
        # response = requests.get(url)
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Monta o DataFrame novo (com timezone)
        df_new = pd.DataFrame({
            "datetime": pd.to_datetime(data["datetime"], utc=True),
            "Kp": data["Kp"],
            "status": data["status"]
        })
        # Remove timezone do df_new ANTES de qualquer concat
        df_new["datetime"] = df_new["datetime"].dt.tz_localize(None)

        # Atualização incremental, se arquivo já existir
        if os.path.exists(save_path):
            df_existing = pd.read_excel(save_path)
            df_existing["datetime"] = pd.to_datetime(df_existing["datetime"], errors="coerce")
            # Remove timezone do df_existing também
            df_existing["datetime"] = df_existing["datetime"].dt.tz_localize(None)
            # Trata DataFrames vazios para evitar FutureWarning do pandas
            frames_to_concat = [df_existing, df_new]
            frames_to_concat = [df for df in frames_to_concat if not df.empty]
            df_combined = pd.concat(frames_to_concat, ignore_index=True)
            df_combined.drop_duplicates(subset=["datetime"], inplace=True)
        else:
            df_combined = df_new

        df_combined.sort_values("datetime", inplace=True)

        # Aqui já está sem timezone!
        df_combined.to_excel(save_path, index=False)
        print(f"Índice Kp (GFZ) salvo/atualizado em: {save_path}")

    except Exception as e:
        print(f"Erro ao extrair ou salvar índice Kp (GFZ): {e}")


