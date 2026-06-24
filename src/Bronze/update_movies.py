def run_get_movie_details():
    import os
    import json
    import requests
    from datetime import datetime, timezone
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
    PREFIX = "bronze/updated_movie_ids/"
    BASE_MOVIE_URL = "https://api.themoviedb.org/3/movie"

    MAX_MOVIES = 10  # quantidade máxima de filmes a processar (mude para testar)

    s3 = boto3.client("s3")

    # =============================
    # ENCONTRAR ARQUIVO MAIS RECENTE
    # =============================
    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=PREFIX
    )

    if "Contents" not in response:
        raise ValueError("Nenhum arquivo encontrado em updated_movie_ids")

    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])
    key = latest_file["Key"]

    print("Lendo arquivo:", key)

    obj = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=key
    )

    content = obj["Body"].read().decode("utf-8")

    movie_ids = [json.loads(line) for line in content.splitlines()]

    print("Total de IDs encontrados:", len(movie_ids))

    # limitar quantidade de filmes processados
    if MAX_MOVIES:
        movie_ids = movie_ids[:MAX_MOVIES]

    print("Total de IDs que serão processados:", len(movie_ids))

    # =============================
    # BUSCAR DETALHES DOS FILMES
    # =============================
    now = datetime.now(timezone.utc)

    movies_data = []

    for movie in movie_ids:

        movie_id = movie["movie_id"]

        try:
            response = requests.get(
                f"{BASE_MOVIE_URL}/{movie_id}",
                params={"api_key": API_KEY}
            )

            if response.status_code == 404:
                print(f"Filme não encontrado: {movie_id}")
                continue

            response.raise_for_status()

            movie_detail = response.json()

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

            movies_data.append(movie_record)

            print(f"Atualizado: {movie_record['title']}")

        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar filme {movie_id}: {e}")
            continue

    # =============================
    # SALVAR NO S3
    # =============================
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    s3_key = f"bronze/movies_updates/{year}/{month}/{day}/movies_updates.json"

    print(f"Salvando em S3 com partições: ano={year}, mês={month}, dia={day}")

    json_lines = "\n".join(json.dumps(movie) for movie in movies_data)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json_lines,
        ContentType="application/json"
    )

    print(f"Arquivo enviado para S3: {s3_key}")


if __name__ == "__main__":
    run_get_movie_details()