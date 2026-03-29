import logging
from pathlib import Path
import shutil
from src.extract import run_extraction
from src.transform import save_transform_data
from src.load import load_silver_data, save_to_duckdb_and_parquet

# ------------------- Config Logging -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ------------------- Paths -------------------
BRONZE_PATH = Path(__file__).parent / "data" / "bronze"
BRONZE_PATH.mkdir(parents=True, exist_ok=True)  # garante que a pasta exista

# ------------------- Limpa Bronze -------------------
if any(BRONZE_PATH.iterdir()):  # se houver arquivos
    logging.info("🧹 Limpando pasta Bronze...")
    for file in BRONZE_PATH.iterdir():
        file.unlink()  # remove cada arquivo
    logging.info("Pasta Bronze limpa (apenas arquivos)")

# ------------------- Main -------------------
if __name__ == "__main__":
    logging.info("🚀 Pipeline iniciado")

    # 1️⃣ Extract
    logging.info("🔹 Etapa 1: Extract")
    run_extraction()

    # 2️⃣ Transform
    logging.info("🔹 Etapa 2: Transform")
    save_transform_data()

    # 3️⃣ Load
    logging.info("🔹 Etapa 3: Load")
    df_silver = load_silver_data()
    if not df_silver.empty:
        save_to_duckdb_and_parquet(df_silver)
    else:
        logging.warning("Nenhum dado encontrado no Silver. Load ignorado.")

    logging.info("✅ Pipeline concluído com sucesso")
