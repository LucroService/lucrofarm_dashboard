import yaml, os, sys, signal, argparse
from datetime import datetime, UTC
from tqdm import tqdm

from utils import (
    ensure_dirs, setup_logger, smart_sleep,
    load_lines, append_line, save_checkpoint, load_checkpoint, today_slug
)
from modules.maps import get_places
from modules.crawl import extract_emails_from_url
from modules.validate import is_valid_email
from modules.exporter import export_rows_in_chunks, export_city_partial

# ✅ Novo: permite passar o arquivo de cidades como argumento
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lote", type=str, help="Arquivo de cidades YAML a rodar (ex: cidades_zona_norte.yaml)")
    return parser.parse_args()

def load_cities(active_cities_file: str):
    with open(active_cities_file, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)
    return y.get("cities", [])

def graceful_exit(logger, pending_rows, cfg):
    logger.info("Encerrando com salvamento final...")
    if pending_rows:
        export_rows_in_chunks(
            pending_rows,
            cfg["chunk_size"],
            cfg["output_base"],
            prefix=f"prospects_{today_slug()}"
        )
    logger.info("✅ Salvo. Fim.")
    sys.exit(0)

def main():
    args = parse_args()  # ✅ Novo: carrega o argumento --lote

    # Carrega config.yaml
    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # ✅ Se o usuário passar --lote, substitui o arquivo de cidades
    if args.lote:
        cfg["active_cities_file"] = args.lote

    ensure_dirs(cfg["output_base"], cfg["log_dir"], "data")
    logger = setup_logger(cfg["log_dir"])

    suppression = load_lines(cfg["suppression_file"])
    seen_emails = load_lines(cfg["seen_emails_file"])
    seen_urls   = load_lines(cfg["seen_urls_file"])

    cities = load_cities(cfg["active_cities_file"])
    logger.info(f"Cidades carregadas: {len(cities)} | Lote: {cfg['active_cities_file']}")

    daily_limit = int(cfg["daily_limit"])
    chunk_size  = int(cfg["chunk_size"])
    smtp_verify = bool(cfg["smtp_verify"])

    state = load_checkpoint(cfg["state_file"]) if cfg.get("resume", True) else None
    start_city_idx = state.get("city_idx", 0) if state else 0
    total_captured = state.get("captured", 0) if state else 0

    all_rows = []
    part_idx = 1

    stop_flag = {"stop": False}
    def on_sigint(sig, frame):
        stop_flag["stop"] = True
        logger.warning("CTRL+C detectado. Vou salvar e encerrar no próximo ponto seguro.")
    signal.signal(signal.SIGINT, on_sigint)

    try:
        for ci in range(start_city_idx, len(cities)):
            if stop_flag["stop"]: break
            city = cities[ci]
            logger.info(f"==> Cidade: {city}")

            links = get_places(
                city=city,
                search_terms=cfg["search_terms"],
                max_cards_per_city=cfg["max_cards_per_city"],
                scroll_rounds=cfg["scroll_rounds"],
                headless=cfg["headless"]
            )

            logger.info(f"Links coletados (brutos): {len(links)}")

            city_rows = []
            details_count = 0
            for item in tqdm(links, desc=f"{city} links"):
                if stop_flag["stop"]: break

                link = item["maps_url"]
                name = item.get("name", "")

                if link in seen_urls:
                    continue
                append_line(cfg["seen_urls_file"], link)
                seen_urls.add(link)

                if details_count >= cfg["max_details_per_city"]:
                    break
                details_count += 1

                emails = extract_emails_from_url(link)
                if not emails:
                    continue

                for email in emails:
                    eml = email.lower().strip()
                    if (eml in suppression) or (eml in seen_emails):
                        continue

                    valid, status = is_valid_email(eml, do_smtp=smtp_verify)
                    if not valid:
                        continue

                    row = {
                        "ts": datetime.now(UTC).isoformat(),
                        "city": city,
                        "name": name,
                        "website": link,
                        "email": eml,
                        "status": status,
                        "source": "maps+heuristic"
                    }
                    city_rows.append(row)
                    all_rows.append(row)

                    append_line(cfg["seen_emails_file"], eml)
                    seen_emails.add(eml)

                    total_captured += 1
                    if total_captured % chunk_size == 0:
                        export_rows_in_chunks(
                            all_rows, chunk_size, cfg["output_base"],
                            prefix=f"prospects_{today_slug()}"
                        )
                        logger.info(f"💾 Checkpoint de chunk salvo ({total_captured} leads).")

                    if total_captured >= daily_limit:
                        logger.info("🎯 Limite diário atingido. Encerrando.")
                        stop_flag["stop"] = True
                        break

                smart_sleep(cfg["pause_seconds"], 1.5)

            if city_rows:
                fname = export_city_partial(city_rows, cfg["output_base"], city, part_idx)
                logger.info(f"💾 Parcial de {city} salvo: {fname}")
                part_idx += 1

            save_checkpoint(cfg["state_file"], {
                "city_idx": ci + 1,
                "captured": total_captured,
                "date": today_slug()
            })

            if stop_flag["stop"]:
                break

        export_rows_in_chunks(
            all_rows, chunk_size, cfg["output_base"],
            prefix=f"prospects_{today_slug()}"
        )
        logger.info(f"✅ Final: {total_captured} e-mails válidos capturados.")

    except Exception as e:
        logger.exception(f"ERRO fatal: {e}")
    finally:
        logger.info("Encerrado.")

if __name__ == "__main__":
    main()
