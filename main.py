# run_pipeline.py
from src.download import download_station_data
from src.processing import load_station_data
from src.plot import plot_stations_comparison_by_type
from src.extractors import extract_goes, extract_kp, extract_ace_all, extract_kp_gfz_xlsx

import os
import pandas as pd
from datetime import datetime
import shutil
import stat

def run_pipeline():
    # ========== Input do usuário ==========
    print("\n=== CONFIGURAÇÃO DA EXTRAÇÃO DE DADOS ===")
    start_str = input("Data inicial (DDMMYYYY): ") or "01042025"
    end_str = input("Data final (DDMMYYYY): ") or datetime.today().strftime("%d%m%Y")

    try:
        START_DATE = datetime.strptime(start_str, "%d%m%Y")
        END_DATE = datetime.strptime(end_str, "%d%m%Y")
    except ValueError:
        print("Formato inválido. Usando datas padrão.")
        START_DATE = datetime(2024, 1, 1)
        END_DATE = datetime.today()

    EXTRACT_STATION = input("Extrair dados das estações? (s/n): ").lower().startswith("s")
    EXTRACT_GOES = input("Extrair dados do satélite GOES? (s/n): ").lower().startswith("s")
    EXTRACT_KP = input("Extrair índice Kp? (s/n): ").lower().startswith("s")
    EXTRACT_KP_GFZ = input("Extrair índice Kp GFZ? (s/n): ").lower().startswith("s")
    EXTRACT_ACE = input("Extrair dados do satélite ACE? (s/n): ").lower().startswith("s")

    DELETE_DATA = input("Deseja apagar os dados e gráficos anteriores? (s/n): ").lower().startswith("s")

    stations_input = input("Digite os códigos das estações separados por vírgula (ou ENTER para padrão): ")
    DEFAULT_STATIONS = [
        "OULU", "APT", "CALG", "CALM", "DRBS",
        "INVK", "IRK2", "JUNG", "JUNG1", "KERG",
        "KIEL2", "LMKS", "PTFM", "PWNK", "ROME",
        "TERA", "THUL"
    ]
    stations = [s.strip().upper() for s in stations_input.split(",") if s.strip()] if stations_input.strip() else DEFAULT_STATIONS

    # ========== Diretórios ==========
    station_data_dir = os.path.join("data", "data_station")
    goes_data_dir = os.path.join("data", "data_goes")
    kp_data_dir = os.path.join("data", "data_kp")
    ace_data_dir = os.path.join("data", "data_ace")
    plot_dir = "plots"

    def handle_remove_readonly(func, path, _):
        os.chmod(path, stat.S_IWRITE)
        func(path)

    if DELETE_DATA:
        for d in [station_data_dir, goes_data_dir, kp_data_dir, ace_data_dir, plot_dir]:
            if os.path.exists(d):
                print(f"\nDiretório a ser apagado: {d}")
                confirm = input("Deseja realmente apagar este diretório? (s/n): ").lower()
                if confirm == "s":
                    shutil.rmtree(d, onerror=handle_remove_readonly)
                    print(f"Diretório apagado: {d}")
                else:
                    print(f"Diretório mantido: {d}")

    for d in [station_data_dir, goes_data_dir, kp_data_dir, ace_data_dir, plot_dir]:
        os.makedirs(d, exist_ok=True)

    # ========== Estações ==========
    if EXTRACT_STATION:
        dataframes = {}
        for station in stations:
            filename = f"{station}_{START_DATE.year}.txt"
            file_path = os.path.join(station_data_dir, filename)
            download_station_data(station, file_path, start_date=START_DATE, end_date=END_DATE)

            if os.path.exists(file_path):
                try:
                    df = load_station_data(file_path)
                    dataframes[station] = df
                except Exception as e:
                    print(f"Erro ao processar {station}: {e}")

        if dataframes:
            combined_df = pd.concat(dataframes.values(), axis=1, keys=dataframes.keys())
            combined_df.columns.names = ["station", "type"]

            plot_stations_comparison_by_type(
                df=combined_df,
                stations=list(dataframes.keys()),
                tipo=["RCORR_E", "RUNCORR", "RCORR_P"],
                save_path=plot_dir
            )
        else:
            print("Nenhuma estação válida carregada.")

    # ========== GOES ==========
    if EXTRACT_GOES:
        extract_goes(save_path=os.path.join(goes_data_dir, "goes_protons.xlsx"))

    # ========== ACE ==========
    if EXTRACT_ACE:
        # extract_ace(save_path=os.path.join(ace_data_dir, "ace_solar_wind.xlsx"))
        extract_ace_all(os.path.join(ace_data_dir))


    # ========== Kp ==========
    if EXTRACT_KP:
        extract_kp(save_path=os.path.join(kp_data_dir, "kp_index_1min.xlsx"))

    if EXTRACT_KP_GFZ:
        start_iso = START_DATE.strftime("%Y-%m-%dT00:00:00Z")
        end_iso = END_DATE.strftime("%Y-%m-%dT23:59:59Z")
        # extract_kp_gfz_xlsx(
        #     start=start_iso,
        #     end=end_iso,
        #     save_path=os.path.join(kp_data_dir, "dados_kp_gfz.xlsx")
        # )
        extract_kp_gfz_xlsx(
            start="2022-01-30T00:00:00Z",
            end="2022-02-02T23:59:59Z",
            save_path=os.path.join(kp_data_dir, "dados_kp_gfz.xlsx")
        )

if __name__ == "__main__":
    run_pipeline()

