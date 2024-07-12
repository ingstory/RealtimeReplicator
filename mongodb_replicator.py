import time

from database_replicator import DatabaseReplicator
from pymongo import MongoClient
import queue
import threading
from tqdm import tqdm


class MongoDBReplicator(DatabaseReplicator):
    def __init__(self, source_host, source_port, target_host, target_port, database, realtime=False,
                 clear_target=False):
        super().__init__(source_host, source_port, target_host, target_port, realtime, clear_target)
        self.database = database
        self.source_client = None
        self.target_client = None
        self.realtime_queue = queue.Queue()
        self.realtime_thread = None

    def replicate(self):
        print(f"MongoDB 복제 시작: {self.source_host}:{self.source_port} -> {self.target_host}:{self.target_port}")

        self.source_client = MongoClient(f"mongodb://{self.source_host}:{self.source_port}/")
        self.target_client = MongoClient(f"mongodb://{self.target_host}:{self.target_port}/")

        source_db = self.source_client[self.database]
        target_db = self.target_client[self.database]

        if self.clear_target:
            self.clear_target_data(target_db)

        try:
            self.total_items = self.count_total_documents(source_db)
            if self.realtime:
                self.start_realtime_sync()

            self.sync_all_collections(source_db, target_db)

            print("\n초기 동기화 완료.")
            if self.realtime:
                print("실시간 동기화 모드로 전환. 프로세스를 유지합니다. 종료하려면 Ctrl+C를 누르세요.")
                while self.running:
                    time.sleep(1)
            else:
                print("동기화 완료. 프로그램을 종료합니다.")
        except Exception as e:
            print(f"오류 발생: {e}")
        finally:
            self.stop()

    def clear_target_data(self, target_db):
        print("대상 MongoDB 데이터베이스의 모든 컬렉션을 삭제합니다...")
        for collection_name in target_db.list_collection_names():
            target_db[collection_name].drop()
        print("대상 MongoDB 데이터베이스 컬렉션 삭제 완료")

    def count_total_documents(self, source_db):
        total_documents = 0
        for collection_name in source_db.list_collection_names():
            total_documents += source_db[collection_name].count_documents({})
        return total_documents

    def sync_all_collections(self, source_db, target_db):
        self.start_time = time.time()
        self.total_synced = 0

        collections = source_db.list_collection_names()

        with tqdm(total=self.total_items, desc="전체 진행률", unit="docs") as pbar:
            for collection_name in collections:
                self.sync_collection(source_db[collection_name], target_db[collection_name], pbar)

        print(f"\n총 {self.total_synced}/{self.total_items} 문서 동기화 완료")

    def sync_collection(self, source_collection, target_collection, pbar):
        target_collection.drop()

        batch_size = 1000
        for document in source_collection.find().batch_size(batch_size):
            target_collection.insert_one(document)
            self.total_synced += 1
            pbar.update(1)

    def start_realtime_sync(self):
        self.realtime_thread = threading.Thread(target=self.realtime_sync_worker)
        self.realtime_thread.start()

    def realtime_sync_worker(self):
        source_db = self.source_client[self.database]
        target_db = self.target_client[self.database]

        for collection_name in source_db.list_collection_names():
            self.watch_collection(source_db[collection_name], target_db[collection_name])

    def watch_collection(self, source_collection, target_collection):
        change_stream = source_collection.watch()
        for change in change_stream:
            if not self.running:
                break

            operation_type = change['operationType']
            document_id = change['documentKey']['_id']

            if operation_type == 'insert':
                target_collection.insert_one(change['fullDocument'])
            elif operation_type == 'update':
                target_collection.replace_one({'_id': document_id}, change['fullDocument'])
            elif operation_type == 'delete':
                target_collection.delete_one({'_id': document_id})

            print(f"실시간 변경 적용: {operation_type} on {source_collection.name}")

    def stop(self):
        print("MongoDB 복제 중지 시작...")
        self.running = False
        if self.realtime_thread and self.realtime_thread.is_alive():
            print("실시간 동기화 스레드 종료 중...")
            self.realtime_thread.join(timeout=10)
            if self.realtime_thread.is_alive():
                print("경고: 실시간 동기화 스레드가 10초 내에 종료되지 않았습니다.")
        if self.source_client:
            self.source_client.close()
        if self.target_client:
            self.target_client.close()
        print("MongoDB 복제 중지 완료.")
