import logging
import sys
import gc
import time
import psutil

class Logger:
    """Simple logger for production use"""

    def __init__(self, name: str = "salary_pipeline"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def info(self, message: str):
        self.logger.info(message)

    def error(self, message: str):
        self.logger.error(message)

    def warning(self, message: str):
        self.logger.warning(message)

class SmartMemoryManager:
    """Memory management utilities"""

    def __init__(self):
        self.memory_info = psutil.virtual_memory()
        self.total_memory_gb = self.memory_info.total / (1024**3)
        self.safe_threshold = 0.75
        self.warning_threshold = 0.85
        self.critical_threshold = 0.95

    def get_memory_usage(self):
        return psutil.virtual_memory().percent / 100

    def get_available_memory_gb(self):
        return psutil.virtual_memory().available / (1024**3)

    def calculate_optimal_chunk_size(self, total_files, avg_file_size_mb=50):
        available_gb = self.get_available_memory_gb()
        usable_memory_mb = (available_gb * 1024) * 0.5
        files_in_memory = max(1, int(usable_memory_mb / avg_file_size_mb))

        if files_in_memory >= total_files:
            chunk_size = total_files
        else:
            chunk_size = files_in_memory

        return max(1, min(chunk_size, 15))

    def force_cleanup(self):
        gc.collect()
        time.sleep(0.5)

    def memory_check(self, operation="operation"):
        usage = self.get_memory_usage()

        if usage > self.critical_threshold:
            self.force_cleanup()
            usage = self.get_memory_usage()
            if usage > self.critical_threshold:
                return False
        elif usage > self.warning_threshold:
            self.force_cleanup()

        return True
