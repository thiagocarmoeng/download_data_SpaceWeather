# NMDB-NOAA Data Pipeline

Este projeto realiza a extração e visualização de dados relacionados ao clima espacial provenientes de:

- Estações do NMDB (dados corrigidos de nêutrons)
- Satélite GOES (fluxo de prótons)
- Satélite ACE/DSCOVR (vento solar)
- Índice Kp (atividade geomagnética)

## Funcionalidades
- Extração de dados por intervalo de datas
- Atualização incremental de arquivos
- Plotagem de gráficos por estação e tipo de correção
- Organização automática de diretórios

## Como executar

1. Crie o ambiente virtual:

```bash
python -m venv venv
