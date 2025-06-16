import logging
import os
from datetime import datetime

def setup_run_logger():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"pipeline_run_{timestamp}.log")

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)

    # Remove any old handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create file handler
    fh = logging.FileHandler(log_file, mode='w')
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

    
