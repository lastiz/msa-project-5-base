import logging
import os
from contextlib import closing
from pathlib import Path

import psycopg2


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    path = Path(os.getenv("EXPORT_PATH", "/exports/shipments.csv"))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    logging.info(f"Exporting shipments to {path}")

    with closing(psycopg2.connect(connect_timeout=10)) as connection:
        with connection.cursor() as cursor:
            with temporary.open("w", encoding="utf-8", newline="") as output:
                cursor.copy_expert(
                    "COPY (SELECT id, origin, destination, weight_kg, status "
                    "FROM shipments ORDER BY id) TO STDOUT WITH CSV HEADER",
                    output,
                )

    temporary.replace(path)
    logging.info(f"Export completed: {path}, size {path.stat().st_size} bytes")


if __name__ == "__main__":
    main()
