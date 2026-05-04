import asyncio
import logging

from config import get_settings
from services.background_sync import run_sync_worker_loop


settings = get_settings()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("services.background_sync").setLevel(logging.INFO)
    logging.getLogger("wb_api.client").setLevel(logging.INFO)
    asyncio.run(run_sync_worker_loop())


if __name__ == "__main__":
    main()
