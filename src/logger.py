import logging
import os
from datetime import datetime

# create log file
LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE)

os.makedirs(LOG_DIR, exist_ok=True)

# logging.basicConfig(
#     filename=LOG_FILE_PATH,
#     format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
#     level=logging.INFO,
# )

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter(
    "[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s"
)

# create file handler
file_handler = logging.FileHandler(LOG_FILE_PATH)
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)
