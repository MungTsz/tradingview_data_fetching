import shutil
from pathlib import Path
from loguru import logger
from result import Err, Ok


def delete_local_data(local_data_base_path: Path) -> [Ok[str] | Err[str]]:
    try:
        shutil.rmtree(local_data_base_path)
        return Ok(f"{local_data_base_path} deleted")
    except Exception as e:
        return Err(f"Error deleting {local_data_base_path}: {e}")


def delete_all_local_data(local_data_base_path: Path, local_log_path: Path):
    logger.info("Deleting local data")
    delete_local_data(local_data_base_path)
    # after delete all data amd catch the log msg, remove the log the upload to S3
    logger.remove()
    delete_local_data(local_log_path)
