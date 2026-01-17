import requests
import pandas as pd
import os

def update_kp_from_json_url(url, save_path):
    """
    Atualiza incrementalmente o arquivo Kp usando um JSON do GFZ Potsdam.
    Se já existir, só adiciona o que for novo, sem duplicar.
    """
    try:
        # Baixa os dados
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Monta DataFrame do retorno
        df_new = pd.DataFrame({
            "datetime": pd.to_datetime(data["datetime"], utc=True),
            "Kp": data["Kp"],
            "status": data["status"]
        })
        df_new["datetime"] = df_new["datetime"].dt.tz_localize(None)  # Remove timezone para merge
        
        # Se já existe arquivo, concatena incrementalmente (sem duplicatas)
        if os.path.exists(save_path):
            df_existing = pd.read_excel(save_path)
            df_existing["datetime"] = pd.to_datetime(df_existing["datetime"], errors="coerce")
            df_existing["datetime"] = df_existing["datetime"].dt.tz_localize(None)
            frames_to_concat = [df_existing, df_new]
            frames_to_concat = [df for df in frames_to_concat if not df.empty]
            df_final = pd.concat(frames_to_concat, ignore_index=True)
            df_final.drop_duplicates(subset=["datetime"], inplace=True)
        else:
            df_final = df_new

        df_final.sort_values("datetime", inplace=True)
        df_final.to_excel(save_path, index=False)
        print(f"Arquivo atualizado: {save_path} ({len(df_new)} novos registros baixados)")
    except Exception as e:
        print(f"Erro na atualização do arquivo Kp: {e}")

# --------- USO EXEMPLO --------------
if __name__ == "__main__":
    url = "https://kp.gfz.de/app/json/?start=2025-05-31T00:00:00Z&end=2025-06-18T23:59:59Z&index=Kp"
    save_path = "data/data_kp/dados_kp_gfz.xlsx"
    update_kp_from_json_url(url, save_path)
