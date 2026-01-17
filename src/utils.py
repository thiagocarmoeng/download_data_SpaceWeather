# src/utils.py
import os
import stat
from datetime import datetime

def parse_date_input(date_str: str, default: datetime) -> datetime:
    try:
        return datetime.strptime(date_str.strip(), "%d%m%Y")
    except ValueError:
        print(f"Data '{date_str}' inválida. Usando padrão: {default.strftime('%d/%m/%Y')}")
        return default

def get_station_list(user_input: str, default_list: list) -> list:
    if not user_input.strip():
        return default_list
    return [s.strip().upper() for s in user_input.split(",") if s.strip()]

def handle_remove_readonly(func, path, _):
    os.chmod(path, stat.S_IWRITE)
    func(path)

def confirm_and_delete_directories(directories: list):
    import shutil
    for d in directories:
        if os.path.exists(d):
            print(f"\nDiretório a ser apagado: {d}")
            confirm = input("Deseja realmente apagar este diretório? (s/n): ").strip().lower()
            if confirm == "s":
                try:
                    shutil.rmtree(d, onerror=handle_remove_readonly)
                    print(f" Diretório apagado: {d}")
                except Exception as e:
                    print(f" Falha ao apagar {d}: {e}")
            else:
                print(f"Diretório mantido: {d}")

def ensure_directories(dirs: list):
    for d in dirs:
        os.makedirs(d, exist_ok=True)
