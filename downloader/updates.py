import asyncio
import logging
import json
import time
import asyncpg
from aiohttp import ClientTimeout
from aiohttp_sse_client import client as sse_client


DB_INITIALIZATION = """CREATE TABLE IF NOT EXISTS updates (
    id INT PRIMARY KEY,
    updated_datetime BIGINT
);

CREATE INDEX IF NOT EXISTS index_updates_id ON updates(id);
"""

URL = "https://hacker-news.firebaseio.com/v0/updates.json"


async def main():

    conn = await asyncpg.connect(
        "postgresql://postgres@db.zzhhpnrzijdaounaqdjr.supabase.co:5432/postgres",
    )

    # Execute a statement to create a new table.
    await conn.execute(DB_INITIALIZATION)

    headers = {"timeout": ClientTimeout(total=0)}
    async with sse_client.EventSource(URL, **headers) as event_source:
        try:
            async for event in event_source:
                if event.type == "put":
                    json_data = json.loads(event.data)
                    item_ids = json_data["data"].get("items", [])
                    updated_datetime = time.time()
                    print(updated_datetime, item_ids)
                    if item_ids:
                        logging.info(f"Write {len(item_ids)} records to updates table.")
                        async with conn.transaction():
                            await conn.executemany(
                                """INSERT INTO updates (id, updated_datetime) VALUES ($1, $2)
                                   ON CONFLICT(id) DO UPDATE
                                   SET updated_datetime = EXCLUDED.updated_datetime""",
                                ((id, updated_datetime) for id in item_ids),
                            )
        except ConnectionError as e:
            logging.error(e)


if __name__ == "__main__":
    try:
        logging.info("Starting updater")
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Received exit call. Exiting.")
