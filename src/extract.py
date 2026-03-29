from playwright.sync_api import sync_playwright
import json
import time
from pathlib import Path
from datetime import datetime
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ------------------- Configs -------------------
URL = "https://www.seminovosmovida.com.br/busca"

# Caminho correto relativo ao projeto
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "bronze"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 100  # salva a cada 100 carros

def extract_car(car):
    """Extrai informações de um card de carro"""
    if car.locator("a").count() == 0:
        return None
    link = car.locator("a").get_attribute("href")
    title = car.locator(".info__title").inner_text().strip()
    version = car.locator(".info__subtitle").inner_text().strip()
    details = car.locator(".add__info").inner_text().strip()
    parcela = car.locator(".fin").inner_text().strip()

    old_price = None
    new_price = None
    if car.locator(".price-24.tachado").count() > 0:
        old_price = car.locator(".price-24.tachado").inner_text().strip()
    if car.locator(".price-30").count() > 0:
        new_price = car.locator(".price-30").inner_text().strip()
    if new_price is None:
        new_price = car.locator(".price label").first.inner_text().strip()

    return {
        "link": "https://www.seminovosmovida.com.br" + link,
        "title": title,
        "version": version,
        "details": details,
        "old_price": old_price,
        "new_price": new_price,
        "parcela": parcela
    }

def save_batch(batch, idx):
    """Salva batch em JSON"""
    if not batch:
        return
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = OUTPUT_PATH / f"cars_batch_{idx}_{now}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(batch, f, ensure_ascii=False, indent=4)
    logging.info(f"Batch {idx} salvo: {len(batch)} carros → {file_path}")


def run_extraction():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(URL)
        page.wait_for_selector("div.car")

        batch = []
        batch_idx = 1
        loaded = 0

        while True:
            cars = page.locator("div.car")
            total = cars.count()

            # extrai apenas os novos carros que ainda não foram processados
            for i in range(loaded, total):
                car_data = extract_car(cars.nth(i))
                if car_data:
                    batch.append(car_data)

                # salva o batch quando atingir BATCH_SIZE
                if len(batch) >= BATCH_SIZE:
                    save_batch(batch, batch_idx)
                    batch_idx += 1
                    batch = []

            loaded = total

            # tenta rolar mais, se não houver mudança termina
            old_height = page.evaluate("document.body.scrollHeight")
            page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            time.sleep(15)
            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == old_height:
                break

        # salva o batch restante
        if batch:
            save_batch(batch, batch_idx)

        browser.close()
