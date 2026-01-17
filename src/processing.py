import pandas as pd
from typing import Optional

def load_station_data(filepath: str) -> pd.DataFrame:
    """
    Carrega os dados ASCII de uma estação NMDB, removendo conteúdo HTML e validando o formato.

    Args:
        filepath (str): Caminho do arquivo de texto da estação

    Returns:
        pd.DataFrame: DataFrame com colunas ['RCORR_E', 'RUNCORR', 'RCORR_P'] indexado por data/hora
    """
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Localiza índice do início dos dados
    data_start_index: Optional[int] = next((i for i, line in enumerate(lines) if "start_date_time" in line), None)

    if data_start_index is None:
        raise ValueError("Formato inesperado: cabeçalho 'start_date_time' não encontrado.")

    # Extração dos dados com as 4 primeiras colunas (datetime + 3 tipos de correção)
    data_lines = [
        line.strip().split(";")[:4]
        for line in lines[data_start_index + 1:]
        if ";" in line
    ]

    df = pd.DataFrame(data_lines, columns=["datetime", "RCORR_E", "RUNCORR", "RCORR_P"])

    # Conversão de colunas para os tipos corretos
    df["datetime"] = pd.to_datetime(df["datetime"].str.strip(), format="%Y-%m-%d %H:%M:%S", errors="coerce")
    for col in ["RCORR_E", "RUNCORR", "RCORR_P"]:
        df[col] = pd.to_numeric(df[col].str.strip(), errors="coerce")

    # Limpa entradas inválidas e define índice temporal
    df.dropna(inplace=True)
    df.set_index("datetime", inplace=True)

    return df