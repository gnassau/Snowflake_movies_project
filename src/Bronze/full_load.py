def run_ingestion(mode="incremental_refresh", start_date=None, end_date=None):

    import requests
    import json
    from datetime import datetime, timezone, timedelta, date
    import boto3
    from airflow.models import Variable
    from concurrent.futures import ThreadPoolExecutor, as_completed

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
    BASE_DISCOVER_URL = "https://api.themoviedb.org/3/discover/movie"
    BASE_MOVIE_URL = "https://api.themoviedb.org/3/movie"

    MAX_PAGES = 500
    MAX_WORKERS = 10

    # 🔥 FLUSH CONFIG
    MAX_RECORDS = 1000
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB

    s3 = boto3.client("s3")
    now = datetime.now(timezone.utc)

    # =============================
    # NORMALIZE DATES
    # =============================

    def parse_to_date(d):
        if d is None:
            return None
        if isinstance(d, date):
            return d
        if isinstance(d, str):
            return datetime.fromisoformat(d).date()
        raise ValueError(f"Formato de data inválido: {d}")

    start_date = parse_to_date(start_date)
    end_date = parse_to_date(end_date)

    # =============================
    # DEFINE MODE
    # =============================

    if mode == "full_refresh":

        if not start_date:
            start_date = date(1990, 1, 1)

        if not end_date:
            end_date = datetime.now().date()

        print("RUNNING FULL REFRESH")

    elif mode == "incremental_refresh":

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=1)

        print("RUNNING INCREMENTAL REFRESH")

    elif mode == "period_refresh":

        if not start_date or not end_date:
            raise ValueError("period_refresh exige start_date e end_date")

        print("RUNNING PERIOD REFRESH")

    else:
        raise ValueError("mode inválido")

    print(f"Collecting from {start_date} to {end_date}")

    # =============================
    # S3 BUFFER CONTROL
    # =============================

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    buffer = []
    buffer_size_bytes = 0
    file_counter = 1

    def flush_to_s3():
        nonlocal buffer, buffer_size_bytes, file_counter

        if not buffer:
            return

        s3_key = f"bronze/movies/year={year}/month={month}/day={day}/part_{file_counter:05d}.json"

        json_lines = "\n".join(buffer)

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json_lines,
            ContentType="application/json"
        )

        print(f"✅ Uploaded: {s3_key} | records={len(buffer)} | size={buffer_size_bytes/1024/1024:.2f} MB")

        buffer = []
        buffer_size_bytes = 0
        file_counter += 1

    def add_to_buffer(movie_dict):
        nonlocal buffer, buffer_size_bytes

        line = json.dumps(movie_dict)
        line_size = len(line.encode("utf-8"))

        buffer.append(line)
        buffer_size_bytes += line_size

        # 🔥 condição de flush
        if len(buffer) >= MAX_RECORDS or buffer_size_bytes >= MAX_FILE_SIZE:
            flush_to_s3()

    # =============================
    # FETCH MOVIE DETAIL
    # =============================

    def fetch_movie_detail(movie_id):

        response = requests.get(
            f"{BASE_MOVIE_URL}/{movie_id}",
            params={"api_key": API_KEY}
        )
        response.raise_for_status()
        return response.json()

    # =============================
    # CORE LOGIC (STREAMING)
    # =============================

    def fetch_batch(batch_start, batch_end):

        page = 1

        while True:

            params = {
                "api_key": API_KEY,
                "primary_release_date.gte": batch_start.strftime("%Y-%m-%d"),
                "primary_release_date.lte": batch_end.strftime("%Y-%m-%d"),
                "sort_by": "primary_release_date.desc",
                "page": page
            }

            response = requests.get(BASE_DISCOVER_URL, params=params)
            response.raise_for_status()
            data = response.json()

            total_pages = data.get("total_pages", 1)

            # 🔥 SPLIT automático
            if total_pages > MAX_PAGES:

                print(f"⚠️ SPLITTING: {batch_start} -> {batch_end} ({total_pages} pages)")

                mid_date = batch_start + (batch_end - batch_start) / 2

                fetch_batch(batch_start, mid_date)
                fetch_batch(mid_date + timedelta(days=1), batch_end)

                return

            results = data.get("results", [])

            print(f"BATCH {batch_start}->{batch_end} | PAGE {page}/{total_pages} | {len(results)} movies")

            if not results:
                break

            movie_ids = [m["id"] for m in results]

            # 🚀 paralelo
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

                futures = [executor.submit(fetch_movie_detail, mid) for mid in movie_ids]

                for future in as_completed(futures):
                    movie_detail = future.result()

                    movie_record = {
                        "movie_id": movie_detail.get("id"),
                        "title": movie_detail.get("title"),
                        "release_date": movie_detail.get("release_date"),
                        "budget": movie_detail.get("budget"),
                        "revenue": movie_detail.get("revenue"),
                        "runtime": movie_detail.get("runtime"),
                        "popularity": movie_detail.get("popularity"),
                        "vote_average": movie_detail.get("vote_average"),
                        "vote_count": movie_detail.get("vote_count"),
                        "language": movie_detail.get("original_language"),
                        "status": movie_detail.get("status"),
                        "ingestion_timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                        "overview": movie_detail.get("overview"),
                        "original_language": movie_detail.get("original_language"),
                        "adult": movie_detail.get("adult"),
                        "homepage": movie_detail.get("homepage"),
                        "genres": [g.get("name") for g in movie_detail.get("genres", []) if g.get("name")]
                    }

                    # 🔥 escreve incremental
                    add_to_buffer(movie_record)

            if page >= total_pages:
                break

            page += 1

    # =============================
    # EXECUTION
    # =============================

    fetch_batch(start_date, end_date)

    # flush final
    flush_to_s3()

    print("🏁 Ingestion finalizada com sucesso")


if __name__ == "__main__":
    run_ingestion()