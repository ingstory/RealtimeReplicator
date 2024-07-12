from abc import ABC, abstractmethod
import time
from datetime import datetime, timedelta
import psutil


class DatabaseReplicator(ABC):
    def __init__(self, source_host, source_port, target_host, target_port, realtime=False, clear_target=False):
        self.source_host = source_host
        self.source_port = source_port
        self.target_host = target_host
        self.target_port = target_port
        self.realtime = realtime
        self.clear_target = clear_target
        self.running = True
        self.total_items = 0
        self.total_synced = 0
        self.start_time = None
        self.process = psutil.Process()

    @abstractmethod
    def replicate(self):
        pass

    @abstractmethod
    def start_realtime_sync(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    def display_progress_info(self):
        elapsed_time = time.time() - self.start_time
        if self.total_synced > 0:
            avg_time_per_item = elapsed_time / self.total_synced
            remaining_items = self.total_items - self.total_synced
            remaining_seconds = remaining_items * avg_time_per_item

            estimated_completion_time = datetime.now() + timedelta(seconds=remaining_seconds)

            cpu_percent = self.process.cpu_percent()
            memory_info = self.process.memory_info()

            progress_info = (
                f"\r진행률: {self.total_synced}/{self.total_items} "
                f"| 남은 예상 시간: {time.strftime('%H:%M:%S', time.gmtime(remaining_seconds))} "
                f"| 예상 완료 시간: {estimated_completion_time.strftime('%Y-%m-%d %H:%M:%S')} "
                f"| CPU: {cpu_percent:.1f}% "
                f"| 메모리: {memory_info.rss / 1024 / 1024:.1f} MB"
            )
            print(progress_info, end="", flush=True)
