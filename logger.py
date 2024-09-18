import logging
import os

def custom_logger(module_name, level=logging.DEBUG):
    # Create a logger
    logger = logging.getLogger(module_name)
    logger.setLevel(level)

    # Ensure the log directory exists
    log_dir = "./log_file"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create a file handler that logs debug and higher level messages
    log_file = f"{log_dir}/{module_name}.log"
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(level)

    # Create a console handler with a higher log level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Create a formatter and set it for both handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Check if handlers already exist to avoid duplicate logs
    if not logger.handlers:
        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
