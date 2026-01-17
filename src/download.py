import requests
import os
from datetime import datetime
from typing import List, Optional

def download_station_data(
    station_code: str,
    file_path: str,
    year: Optional[int] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    include_types: Optional[List[str]] = None
) -> None:
    """
    Faz o download dos dados da estação NMDB no formato ASCII.

    Args:
        station_code (str): Código da estação (ex: "OULU").
        file_path (str): Caminho completo para salvar o arquivo.
        year (int, optional): Ano de referência (não utilizado diretamente).
        start_date (datetime, optional): Data de início do intervalo.
        end_date (datetime, optional): Data de fim do intervalo.
        include_types (List[str], optional): Lista com os tipos de dados desejados. Defaults para as três opções principais.
    """
    if not start_date or not end_date:
        raise ValueError("É necessário fornecer start_date e end_date.")

    include_types = include_types or [
        "uncorrected",
        "corr_for_pressure",
        "corr_for_efficiency"
    ]

    base_url = "http://nest.nmdb.eu/draw_graph.php"
    odtype_params = "".join(f"&odtype[]={dtype}" for dtype in include_types)

    url = (
        f"{base_url}?formchk=1"
        f"&stations[]={station_code}"
        f"&output=ascii"
        f"&tabchoice=revori"
        f"{odtype_params}"
        f"&date_choice=bydate"
        f"&start_year={start_date.year}&start_month={start_date.month}&start_day={start_date.day}"
        f"&start_hour={start_date.hour}&start_min={start_date.minute}"
        f"&end_year={end_date.year}&end_month={end_date.month}&end_day={end_date.day}"
        f"&end_hour={end_date.hour}&end_min={end_date.minute}"
        f"&yunits=0"
    )

    print(f"🔗 Requisitando dados de {station_code}...")
    response = requests.get(url)

    if "DOCTYPE html" in response.text or "no data available" in response.text.lower():
        print(f"Nenhum dado disponível para {station_code}. Status {response.status_code}")
        print("Prévia da resposta:", response.text[:300])
        return

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(response.text)

    print(f"Dados salvos: {file_path}")
