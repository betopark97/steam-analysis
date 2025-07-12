import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging():
    """
    Configures a centralized logger for the application.

    This function sets up a root logger that writes to both the console (stdout)
    and a rotating file. The log file is stored in a dedicated 'logs' directory
    within the 'data_pipeline' folder.
    """
    # Define the base directory for the data_pipeline
    pipeline_dir = Path(__file__).resolve().parent.parent
    log_dir = pipeline_dir / "logs"
    log_dir.mkdir(exist_ok=True)  # Ensure the log directory exists
    log_path = log_dir / "app.log"

    # Clear existing handlers to avoid duplicate logs
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Set up log file rotation: max 200MB, keep 2 backup files
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=200 * 1024 * 1024,  # 200 MB
        backupCount=2,
        encoding='utf-8'
    )

    # Also log to the console
    stream_handler = logging.StreamHandler(sys.stdout)

    # Configure the logging format and handlers
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[stream_handler, file_handler],
    )

    # Set specific log levels for noisy libraries if needed
    # logging.getLogger("some_library").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured. Log file: {log_path}")

