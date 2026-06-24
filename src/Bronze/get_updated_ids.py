def run_get_updated_ids(mode="incremental_refresh", start_date=None, end_date=None):

    import requests
    import json
    from datetime import datetime, timezone, timedelta, date
    import boto3
    from airflow.models import Variable

    # =============================
    # LOAD ENV
    # =============================

    API_KEY = Variable.get("TMDB_API_KEY")

    if not API_KEY:
        raise ValueError("TMDB_API_KEY não encontrada")

    # =============================
    # CONFIG
    # =============================

    BUCKET_NAME = "movies-raw-gustavo-portfolio"
    BASE_URL = "https://api.themoviedb.org/3/movie/changes"

    MAX_RANGE_DAYS = 10  # limite da API
    MIN_DATE = date(2012, 10, 5)  # 🔥 LIMITE DA API

    s3 = boto3.client("s3")
    now = datetime.now(timezone.utc)

    # =============================
    # FUNÇÃO AUXILIAR
    # =============================

    def parse_date(d):
        if isinstance(d, str):
            return datetime.strptime(d, "%Y-%m-%d").date()
        if isinstance(d, datetime):
            return d.date()
        if isinstance(d, date):
            return d
        raise ValueError(f"Formato de data inválido: {d}")

    # =============================
    # DEFINE DATE RANGE
    # =============================

    if mode == "full_refresh":

        start_date = start_date or date(2020, 1, 1)
        end_date = end_date or datetime.now().date()

        print("RUNNING FULL REFRESH")

    elif mode == "incremental_refresh":

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=1)

        print("RUNNING INCREMENTAL REFRESH")

    else:
        raise ValueError("mode deve ser full_refresh ou incremental_refresh")

    # normaliza
    start_date = parse_date(start_date)
    end_date = parse_date(end_date)

    print(f"Collecting updates from {start_date} to {end_date}")

    # =============================
    # 🔥 PROTEÇÃO CONTRA ERRO DA API
    # =============================

    if start_date < MIN_DATE:

        if end_date < MIN_DATE:
            print("⚠️ Intervalo inteiro antes de 2012-10-05. Nada para coletar. Encerrando.")
            return

        print(f"⚠️ Ajustando start_date de {start_date} para {MIN_DATE}")
        start_date = MIN_DATE

    # =============================
    # LOOP DE DATAS (BATCH)
    # =============================

    movie_ids = []
    seen_ids = set()

    current_start = start_date

    while current_start <= end_date:

        current_end = min(current_start + timedelta(days=MAX_RANGE_DAYS), end_date)

        print(f"\nPROCESSING RANGE: {current_start} -> {current_end}")

        page = 1

        while True:

            params = {
                "api_key": API_KEY,
                "start_date": current_start.strftime("%Y-%m-%d"),
                "end_date": current_end.strftime("%Y-%m-%d"),
                "page": page
            }

            try:
                response = requests.get(BASE_URL, params=params)

                if response.status_code != 200:
                    print(f"❌ Erro na API: {response.text}")
                    response.raise_for_status()

                data = response.json()

            except requests.exceptions.RequestException as e:
                print(f"❌ Erro na requisição: {e}")
                break  # não quebra o job inteiro

            results = data.get("results", [])

            print(f"RANGE {current_start}->{current_end} | PAGE {page} | IDS {len(results)}")

            if not results:
                break

            for movie in results:
                movie_id = movie["id"]

                if movie_id not in seen_ids:
                    seen_ids.add(movie_id)
                    movie_ids.append({
                        "movie_id": movie_id,
                        "ingestion_timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
                    })

            total_pages = data.get("total_pages", 1)

            if page >= total_pages:
                break

            page += 1

        current_start = current_end + timedelta(days=1)

    print(f"\nTotal de IDs únicos coletados: {len(movie_ids)}")

    # =============================
    # SALVAR NO S3
    # =============================

    if not movie_ids:
        print("⚠️ Nenhum ID encontrado. Nada será salvo.")
        return

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    s3_key = f"bronze/updated_movie_ids/{year}/{month}/{day}/ids.json"

    json_lines = "\n".join(json.dumps(movie) for movie in movie_ids)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_lines,
        ContentType="application/json"
    )

    print(f"✅ Arquivo enviado para S3: {s3_key}")


if __name__ == "__main__":
    run_get_updated_ids()