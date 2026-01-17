import matplotlib.pyplot as plt
import os
from typing import List, Optional
import pandas as pd

def plot_stations_comparison_by_type(
    df: pd.DataFrame,
    stations: List[str],
    tipo: Optional[List[str]] = None,
    save_path: Optional[str] = None
) -> None:
    """
    Plota os dados de cada estação separadamente para cada tipo de dado (RCORR_E, RUNCORR, RCORR_P).

    Args:
        df (pd.DataFrame): DataFrame com colunas MultiIndex (station, tipo).
        stations (List[str]): Lista de estações.
        tipo (List[str], optional): Lista de tipos de correção. Defaults para ["RCORR_E", "RUNCORR", "RCORR_P"].
        save_path (str, optional): Diretório onde os gráficos serão salvos. Se None, os gráficos são exibidos na tela.
    """
    tipo = tipo or ["RCORR_E", "RUNCORR", "RCORR_P"]

    for station in stations:
        for t in tipo:
            if (station, t) not in df.columns:
                continue

            plt.figure(figsize=(14, 6))
            plt.plot(df.index, df[(station, t)], label=f"{station} - {t}", linewidth=0.7)
            plt.title(f"Estatísticas da Estação {station} - {t}")
            plt.xlabel("Data")
            plt.ylabel("Contagens corrigidas (c/s)")
            plt.grid(True)
            plt.legend()
            plt.tight_layout()

            if save_path:
                os.makedirs(save_path, exist_ok=True)
                filename = os.path.join(save_path, f"{station}_{t}.png")
                plt.savefig(filename)
                print(f" Gráfico salvo: {filename}")
                plt.close()
            else:
                plt.show()