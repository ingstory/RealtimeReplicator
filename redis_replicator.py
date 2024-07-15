import binascii
import json
import time

from database_replicator import DatabaseReplicator
import redis
import queue
import threading
import platform
from datetime import datetime, timedelta
from tqdm import tqdm


class RedisReplicator(DatabaseReplicator):
    def __init__(self, source_host, source_port, target_host, target_port, num_dbs=16, realtime=False,
                 clear_target=False):
        super().__init__(source_host, source_port, target_host, target_port, realtime, clear_target)
        self.num_dbs = num_dbs
        self.total_keys = 0
        self.realtime_queue = queue.Queue()
        self.realtime_thread = None
        self.is_windows = platform.system().lower() == 'windows'

    def get_source_connection(self):
        return redis.Redis(host=self.source_host, port=self.source_port, decode_responses=False)

    def get_target_connection(self):
        return redis.Redis(host=self.target_host, port=self.target_port, decode_responses=False)

    def replicate(self):
        global realtime_thread
        print(
            f"복제 시작: {self.source_host}:{self.source_port} -> "
            f"{self.target_host}:{self.target_port}")
        print(f"동기화할 데이터베이스: 0부터 {self.num_dbs - 1}까지")

        if self.clear_target:
            print(f"데이터베이스 초기화: 타겟 Redis 서버의 모든 데이터를 삭제합니다.")
            self.clear_target_data()

        try:
            self.total_keys = self.count_total_keys()

            # 실시간 동기화 스레드 시작
            if self.realtime:
                self.start_realtime_sync()
                realtime_thread = threading.Thread(target=self.process_realtime_events)
                realtime_thread.start()

            # 초기 동기화 스레드 시작
            sync_thread = threading.Thread(target=self.sync_all_dbs)
            sync_thread.start()

            # 초기 동기화 완료 대기
            sync_thread.join()
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
            if self.realtime:
                realtime_thread.join()  # 실시간 동기화 스레드 종료 대기

    def clear_target_data(self):
        print("대상 Redis 서버의 모든 데이터를 삭제합니다...")
        target_conn = redis.Redis(host=self.target_host, port=self.target_port)
        for db in range(self.num_dbs):
            target_conn.select(db)
            target_conn.flushdb()
        print("대상 Redis 서버 데이터 삭제 완료")

    def count_total_keys(self):
        total_keys = 0
        source_conn = self.get_source_connection()
        for db in range(self.num_dbs):
            source_conn.select(db)
            db_size = source_conn.dbsize()
            total_keys += db_size
            print(f"데이터베이스 {db}: {db_size} 키")
        return total_keys

    def start_realtime_sync(self):
        self.realtime_thread = threading.Thread(target=self.realtime_sync_worker)
        self.realtime_thread.start()

    def realtime_sync_worker(self):
        realtime_source = self.get_source_connection()
        pubsub = realtime_source.pubsub()

        for db in range(self.num_dbs):
            pubsub.psubscribe(f'__keyspace@{db}__:*')

        print("실시간 동기화 시작. 모든 데이터베이스 감시 중...")
        for message in pubsub.listen():
            if not self.running:
                break
            if message['type'] == 'pmessage':
                channel = message['channel']
                event = message['data']
                try:
                    db, key = self.parse_channel(channel)
                    if db is not None and key is not None:
                        self.realtime_queue.put((db, key, event))
                except Exception as e:
                    print(f"채널 처리 중 오류 발생: {channel}, 오류: {e}")

    def process_realtime_events(self):
        while self.running:
            try:
                db, key, event = self.realtime_queue.get(timeout=1)
                self.sync_key_realtime(db, key, event)
                self.realtime_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"실시간 이벤트 처리 중 오류 발생: {e}")

    def sync_key_realtime(self, db, key, event):
        try:
            source_conn = self.get_source_connection()
            target_conn = self.get_target_connection()
            source_conn.select(db)
            target_conn.select(db)

            if event == 'del' or event == 'expired':
                target_conn.delete(key)
                print(f"키 삭제됨 (실시간, {event}): db{db}:{key}")
            elif event in ['set', 'rename_from', 'rename_to', 'expire', 'persist']:
                self.sync_key(source_conn, target_conn, key, db)
                print(f"키 업데이트됨 (실시간, {event}): db{db}:{key}")
            else:
                self.sync_key(source_conn, target_conn, key, db)
                print(f"기타 이벤트 처리됨 (실시간, {event}): db{db}:{key}")
        except Exception as e:
            print(f"실시간 키 동기화 중 오류 발생 (db{db}:{key}): {e}")

    def sync_all_dbs(self):
        self.start_time = time.time()
        self.total_synced = 0
        total_keys = 0

        for db in range(self.num_dbs):
            if not self.running:
                print("동기화가 중단되었습니다.")
                break
            db_keys = self.count_db_keys(db)
            total_keys += db_keys

        with tqdm(total=total_keys, desc="전체 진행률", unit="keys") as pbar:
            for db in range(self.num_dbs):
                if not self.running:
                    print("동기화가 중단되었습니다.")
                    break
                self.sync_db(db, pbar)

        print(f"\n총 {self.total_synced}/{total_keys} 키 동기화 완료")

    def count_db_keys(self, db):
        source_conn = self.get_source_connection()
        source_conn.select(db)
        return source_conn.dbsize()

    def sync_db(self, db, pbar):
        source_conn = self.get_source_connection()
        target_conn = self.get_target_connection()
        source_conn.select(db)
        target_conn.select(db)

        try:
            all_keys = set()
            max_iterations = 3  # 최대 반복 횟수

            for iteration in range(max_iterations):
                new_keys = set()
                cursor = 0
                while True:
                    cursor, keys = source_conn.scan(cursor, count=1000)
                    new_keys.update(keys)
                    if cursor == 0:
                        break

                if iteration == 0:
                    all_keys = new_keys
                    continue

                missed_keys = new_keys - all_keys
                if not missed_keys:
                    break  # 새로운 키가 없으면 중단

                all_keys.update(missed_keys)
                print(f"데이터베이스 {db}, 반복 {iteration + 1}: {len(missed_keys)}개의 새로운 키 발견")

            total_keys = len(all_keys)
            print(f"데이터베이스 {db}: 총 {total_keys}개의 키 발견")

            for key in all_keys:
                if not self.running:
                    return
                self.sync_key(source_conn, target_conn, key, db)
                self.total_synced += 1
                pbar.update(1)

            print(f"데이터베이스 {db} 동기화 완료. 총 {total_keys}개의 키 처리됨.")

        except redis.exceptions.ResponseError as e:
            print(f"데이터베이스 {db} 동기화 중 Redis 오류 발생: {e}")
        except Exception as e:
            print(f"데이터베이스 {db} 동기화 중 예기치 않은 오류 발생: {e}")
            import traceback
            print(traceback.format_exc())

    def sync_key(self, source_conn, target_conn, key, db):
        max_retries = 3
        retry_delay = 0.3  # 300ms 대기

        for attempt in range(max_retries):
            try:
                pipe = source_conn.pipeline()
                pipe.type(key)
                pipe.ttl(key)
                pipe.object('encoding', key)
                key_type, ttl, encoding = pipe.execute()

                key_type = self.safe_decode(key_type)
                encoding = self.safe_decode(encoding)

                if key_type == '' or key_type == 'none':
                    if attempt < max_retries - 1:
                        print(f"키를 찾을 수 없음. 재시도 중... (시도 {attempt + 1}/{max_retries}): db({db}):{key}")
                        time.sleep(retry_delay)
                        continue
                    else:
                        print(f"키가 소스에 존재하지 않음 (최종 확인): db({db}):{key}")
                        target_conn.delete(key)
                        return

                if key_type == 'string':
                    value = source_conn.get(key)
                    if value is not None:
                        target_conn.set(key, value)
                elif key_type == 'hash':
                    self.sync_hash(source_conn, target_conn, key, encoding)
                elif key_type == 'list':
                    value = source_conn.lrange(key, 0, -1)
                    if value:
                        target_conn.delete(key)
                        target_conn.rpush(key, *value)
                elif key_type == 'set':
                    value = source_conn.smembers(key)
                    if value:
                        target_conn.delete(key)
                        target_conn.sadd(key, *value)
                elif key_type == 'zset':
                    value = source_conn.zrange(key, 0, -1, withscores=True)
                    if value:
                        target_conn.delete(key)
                        target_conn.zadd(key, dict(value))
                elif key_type == 'stream':
                    self.sync_stream(source_conn, target_conn, key)
                else:
                    print(f"지원하지 않는 키 타입: {key_type} for db{db}:{key}")
                    return

                if ttl > 0:
                    target_conn.expire(key, ttl)
                elif ttl == -1:  # 영구적인 키
                    target_conn.persist(key)

                return  # 성공적으로 동기화되면 루프 종료

            except redis.exceptions.ResponseError as e:
                if "no such key" in str(e).lower():
                    if attempt < max_retries - 1:
                        print(f"키를 찾을 수 없음. 재시도 중... (시도 {attempt + 1}/{max_retries}): db({db}):{key}")
                        time.sleep(retry_delay)
                    else:
                        print(f"키가 동기화 중 삭제됨 (최종 확인): db{db}:{key}")
                        self.total_synced -= 1
                        target_conn.delete(key)
                else:
                    raise
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"예기치 않은 오류 발생. 재시도 중... (시도 {attempt + 1}/{max_retries}): db({db}):{key}")
                    print(f"오류 내용: {e}")
                    time.sleep(retry_delay)
                else:
                    print(f"키 'db{db}:{key}' 동기화 중 예기치 않은 오류 발생 (최종): {e}")
                    import traceback
                    print(traceback.format_exc())

    def sync_hash(self, source_conn, target_conn, key, encoding):
        try:
            cursor = 0
            while True:
                cursor, data = source_conn.hscan(key, cursor)
                if data:
                    pipe = target_conn.pipeline()
                    for field, value in data.items():
                        decoded_field = self.safe_decode(field)
                        processed_value = self.process_value(value)
                        pipe.hset(key, self.safe_encode(decoded_field), processed_value)
                    pipe.execute()
                if cursor == 0:
                    break
        except redis.exceptions.ResponseError as e:
            print(f"Hash 동기화 중 오류 발생 (key: {key}): {e}")
            self.sync_hash_fallback(source_conn, target_conn, key)

    def sync_hash_fallback(self, source_conn, target_conn, key):
        try:
            value = source_conn.hgetall(key)
            pipe = target_conn.pipeline()
            for field, val in value.items():
                try:
                    decoded_field = self.safe_decode(field)
                    processed_value = self.process_value(val)
                    pipe.hset(key, self.safe_encode(decoded_field), processed_value)
                except Exception as field_e:
                    print(
                        f"필드 '{self.safe_decode(field)}' 설정 중 오류 발생 (db{source_conn.connection_pool.connection_kwargs['db']}:{key}): {field_e}")
            pipe.execute()
        except Exception as inner_e:
            print(f"개별 필드 설정 중 오류 발생 (db{source_conn.connection_pool.connection_kwargs['db']}:{key}): {inner_e}")

    def process_value(self, value):
        if self.is_hex_data(value):
            return value  # 16진수 데이터는 그대로 반환

        decoded_value = self.safe_decode(value)

        if self.is_json_data(decoded_value):
            return self.safe_encode(decoded_value)  # JSON 데이터는 문자열로 저장

        parsed_value = self.parse_tab_separated_value(decoded_value)
        if isinstance(parsed_value, dict):
            return self.safe_encode(json.dumps(parsed_value))  # 탭 구분 데이터는 JSON으로 변환하여 저장

        return self.safe_encode(decoded_value)  # 일반 텍스트는 그대로 인코딩하여 저장

    def is_hex_data(self, data):
        if isinstance(data, bytes):
            try:
                binascii.unhexlify(data)
                return True
            except binascii.Error:
                return False
        return False

    def is_json_data(self, data):
        if isinstance(data, str):
            try:
                json.loads(data)
                return True
            except json.JSONDecodeError:
                return False
        return False

    def parse_tab_separated_value(self, value):
        if isinstance(value, str) and '\t' in value:
            parts = value.split('\t')
            return {parts[i]: parts[i + 1] for i in range(0, len(parts), 2)}
        return value

    def sync_stream(self, source_conn, target_conn, key):
        try:
            last_id = '0-0'
            while True:
                response = source_conn.xrange(key, min=last_id, count=1000)
                if not response:
                    break

                for item_id, fields in response:
                    target_conn.xadd(key, fields, id=item_id)
                    last_id = item_id

            self.sync_stream_metadata(source_conn, target_conn, key)

            print(f"스트림 동기화 완료: db{source_conn.connection_pool.connection_kwargs['db']}:{key}")

        except redis.exceptions.ResponseError as e:
            print(f"스트림 'db{source_conn.connection_pool.connection_kwargs['db']}:{key}' 동기화 중 Redis 오류 발생: {e}")
        except Exception as e:
            print(f"스트림 'db{source_conn.connection_pool.connection_kwargs['db']}:{key}' 동기화 중 예기치 않은 오류 발생: {e}")
            import traceback
            print(traceback.format_exc())

    def sync_stream_metadata(self, source_conn, target_conn, key):
        try:
            groups = source_conn.xinfo_groups(key)
            for group in groups:
                group_name = group['name']
                try:
                    target_conn.xgroup_create(key, group_name, id='0-0', mkstream=True)
                except redis.exceptions.ResponseError:
                    pass

                consumers = source_conn.xinfo_consumers(key, group_name)
                for consumer in consumers:
                    consumer_name = consumer['name']
                    pending = consumer['pending']
                    if pending > 0:
                        pending_entries = source_conn.xpending_range(key, group_name, min='-', max='+', count=pending,
                                                                     consumer=consumer_name)
                        for entry in pending_entries:
                            message_id = entry['message_id']
                            target_conn.xack(key, group_name, message_id)
                            target_conn.xclaim(key, group_name, consumer_name, 0, [message_id])

        except redis.exceptions.ResponseError as e:
            print(f"스트림 'db{source_conn.connection_pool.connection_kwargs['db']}:{key}' 메타데이터 동기화 중 Redis 오류 발생: {e}")
        except Exception as e:
            print(f"스트림 'db{source_conn.connection_pool.connection_kwargs['db']}:{key}' 메타데이터 동기화 중 예기치 않은 오류 발생: {e}")
            import traceback
            print(traceback.format_exc())

    def parse_channel(self, channel):
        try:
            parts = channel.split(':')
            if len(parts) >= 2:
                db_part = parts[0].split('@')[1].split('__')[0]
                db = int(db_part)
                key = ':'.join(parts[1:])
                return db, key
            else:
                return None, None
        except Exception as e:
            print(f"채널 파싱 오류: {channel}, 오류: {e}")
            return None, None

    def stop(self):
        print("복제 중지 시작...")
        self.running = False
        if self.realtime_thread and self.realtime_thread.is_alive():
            print("실시간 동기화 스레드 종료 중...")
            self.realtime_thread.join(timeout=10)  # 10초 동안 스레드 종료 대기
            if self.realtime_thread.is_alive():
                print("Warning: 실시간 동기화 스레드가 10초 내에 종료되지 않았습니다.")
        print("복제 중지 완료.")

    def display_progress_info(self):
        elapsed_time = time.time() - self.start_time
        if self.total_synced > 0:
            avg_time_per_key = elapsed_time / self.total_synced
            remaining_keys = self.total_keys - self.total_synced
            remaining_seconds = remaining_keys * avg_time_per_key

            estimated_completion_time = datetime.now() + timedelta(seconds=remaining_seconds)

            cpu_percent = self.process.cpu_percent()
            memory_info = self.process.memory_info()

            progress_info = (
                f"\r진행률: {self.total_synced}/{self.total_keys} "
                f"| 남은 예상 시간: {time.strftime('%H:%M:%S', time.gmtime(remaining_seconds))} "
                f"| 예상 완료 시간: {estimated_completion_time.strftime('%Y-%m-%d %H:%M:%S')} "
                f"| CPU: {cpu_percent:.1f}% "
                f"| 메모리: {memory_info.rss / 1024 / 1024:.1f} MB"
            )
            print(progress_info, end="", flush=True)

    def safe_decode(self, value):
        if isinstance(value, bytes):
            encodings = ['utf-8', 'cp949' if self.is_windows else 'latin-1']
            for encoding in encodings:
                try:
                    return value.decode(encoding)
                except UnicodeDecodeError:
                    continue
            return value.decode('latin-1', errors='replace')
        return value

    def safe_encode(self, value):
        if isinstance(value, str):
            return value.encode('utf-8', errors='replace')
        return value

    def realtime_sync_worker(self):
        realtime_source = self.get_source_connection()
        pubsub = realtime_source.pubsub()

        for db in range(self.num_dbs):
            pubsub.psubscribe(f'__keyspace@{db}__:*')

        print("실시간 동기화 시작. 모든 데이터베이스 감시 중...")
        while self.running:
            message = pubsub.get_message(timeout=1)
            if message and message['type'] == 'pmessage':
                channel = message['channel']
                event = message['data']
                try:
                    db, key = self.parse_channel(channel)
                    if db is not None and key is not None:
                        self.realtime_queue.put((db, key, event))
                except Exception as e:
                    print(f"채널 처리 중 오류 발생: {channel}, 오류: {e}")

        print("실시간 동기화 워커 종료.")
        pubsub.close()

    def stop(self):
        print("Redis 복제 중지 시작...")
        self.running = False
        if self.realtime_thread and self.realtime_thread.is_alive():
            print("실시간 동기화 스레드 종료 중...")
            self.realtime_thread.join(timeout=10)
            if self.realtime_thread.is_alive():
                print("경고: 실시간 동기화 스레드가 10초 내에 종료되지 않았습니다.")
        print("Redis 복제 중지 완료.")

    # 기존 RedisReplicator 클래스의 다른 메서드들을 여기에 추가
    def get_source_connection(self):
        return redis.Redis(host=self.source_host, port=self.source_port, decode_responses=False)

    def get_target_connection(self):
        return redis.Redis(host=self.target_host, port=self.target_port, decode_responses=False)
