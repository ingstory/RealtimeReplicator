from database_replicator import DatabaseReplicator
from mysql.connector import pooling
import queue
import threading
import time
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from tqdm import tqdm


class MySQLReplicator(DatabaseReplicator):
    def __init__(self, source_host, source_port, target_host, target_port, user, password, database, realtime=False,
                 clear_target=False):
        super().__init__(source_host, source_port, target_host, target_port, realtime, clear_target)
        self.user = user
        self.password = password
        self.database = database
        self.source_pool = None
        self.target_pool = None
        self.realtime_queue = queue.Queue()
        self.realtime_thread = None

    def replicate(self):
        print(f"MySQL 복제 시작: {self.source_host}:{self.source_port} -> {self.target_host}:{self.target_port}")

        self.source_pool = self.create_connection_pool(self.source_host, self.source_port, self.user, self.password,
                                                       self.database)
        self.target_pool = self.create_connection_pool(self.target_host, self.target_port, self.user, self.password,
                                                       self.database)

        if self.clear_target:
            self.clear_target_data()

        try:
            self.total_items = self.count_total_rows()
            if self.realtime:
                self.start_realtime_sync()

            self.sync_all_tables()

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

    def create_connection_pool(self, host, port, user, password, database):
        return pooling.MySQLConnectionPool(
            pool_name=f"{host}:{port}",
            pool_size=5,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )

    def clear_target_data(self):
        print("대상 MySQL 데이터베이스의 모든 테이블을 삭제합니다...")
        with self.target_pool.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                for table in tables:
                    cursor.execute(f"TRUNCATE TABLE {table[0]}")
        print("대상 MySQL 데이터베이스 테이블 삭제 완료")

    def count_total_rows(self):
        total_rows = 0
        with self.source_pool.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                    total_rows += cursor.fetchone()[0]
        return total_rows

    def sync_all_tables(self):
        self.start_time = time.time()
        self.total_synced = 0

        with self.source_pool.get_connection() as source_conn, self.target_pool.get_connection() as target_conn:
            with source_conn.cursor() as source_cursor, target_conn.cursor() as target_cursor:
                source_cursor.execute("SHOW TABLES")
                tables = source_cursor.fetchall()

                with tqdm(total=self.total_items, desc="전체 진행률", unit="rows") as pbar:
                    for table in tables:
                        table_name = table[0]
                        self.sync_table(source_cursor, target_cursor, table_name, pbar)

        print(f"\n총 {self.total_synced}/{self.total_items} 행 동기화 완료")

    def sync_table(self, source_cursor, target_cursor, table_name, pbar):
        source_cursor.execute(f"SELECT * FROM {table_name}")
        columns = [column[0] for column in source_cursor.description]

        target_cursor.execute(f"TRUNCATE TABLE {table_name}")

        batch_size = 1000
        while True:
            rows = source_cursor.fetchmany(batch_size)
            if not rows:
                break

            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            target_cursor.executemany(insert_query, rows)

            self.total_synced += len(rows)
            pbar.update(len(rows))

    def start_realtime_sync(self):
        self.realtime_thread = threading.Thread(target=self.realtime_sync_worker)
        self.realtime_thread.start()

    def realtime_sync_worker(self):
        stream = BinLogStreamReader(
            connection_settings={
                "host": self.source_host,
                "port": self.source_port,
                "user": self.user,
                "passwd": self.password
            },
            server_id=100,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            only_schemas=[self.database],
            resume_stream=True,
            blocking=True
        )

        for binlogevent in stream:
            if not self.running:
                break

            if isinstance(binlogevent, DeleteRowsEvent):
                self.handle_delete(binlogevent)
            elif isinstance(binlogevent, UpdateRowsEvent):
                self.handle_update(binlogevent)
            elif isinstance(binlogevent, WriteRowsEvent):
                self.handle_insert(binlogevent)

        stream.close()

    def handle_delete(self, event):
        table = event.table
        for row in event.rows:
            self.execute_target_query(f"DELETE FROM {table} WHERE id = %s", (row["values"]["id"],))

    def handle_update(self, event):
        table = event.table
        for row in event.rows:
            old_values = row["before_values"]
            new_values = row["after_values"]
            set_clause = ", ".join([f"{key} = %s" for key in new_values.keys() if key != "id"])
            values = tuple(value for key, value in new_values.items() if key != "id") + (old_values["id"],)
            self.execute_target_query(f"UPDATE {table} SET {set_clause} WHERE id = %s", values)

    def handle_insert(self, event):
        table = event.table
        for row in event.rows:
            values = row["values"]
            columns = ", ".join(values.keys())
            placeholders = ", ".join(["%s"] * len(values))
            self.execute_target_query(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
                                      tuple(values.values()))

    def execute_target_query(self, query, params):
        with self.target_pool.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                connection.commit()

    def stop(self):
        print("MySQL 복제 중지 시작...")
        self.running = False
        if self.realtime_thread and self.realtime_thread.is_alive():
            print("실시간 동기화 스레드 종료 중...")
            self.realtime_thread.join(timeout=10)
            if self.realtime_thread.is_alive():
                print("경고: 실시간 동기화 스레드가 10초 내에 종료되지 않았습니다.")
        if self.source_pool:
            self.source_pool.close()
        if self.target_pool:
            self.target_pool.close()
        print("MySQL 복제 중지 완료.")
