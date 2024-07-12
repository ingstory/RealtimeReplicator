import argparse
import signal
import sys
from redis_replicator import RedisReplicator
from mysql_replicator import MySQLReplicator
from mongodb_replicator import MongoDBReplicator

# 전역 변수로 replicator 선언
replicator = None


def signal_handler(signum, frame):
    global replicator
    print("\n종료 신호를 받았습니다. 안전하게 종료합니다...")
    if replicator:
        replicator.stop()
    sys.exit(0)


def create_replicator(args):
    if args.db_type == "redis":
        return RedisReplicator(
            args.source_host,
            args.source_port,
            args.target_host,
            args.target_port,
            args.num_dbs,
            args.realtime,
            args.clear_target
        )
    elif args.db_type == "mysql":
        return MySQLReplicator(
            args.source_host,
            args.source_port,
            args.target_host,
            args.target_port,
            args.user,
            args.password,
            args.database,
            args.realtime,
            args.clear_target
        )
    elif args.db_type == "mongodb":
        return MongoDBReplicator(
            args.source_host,
            args.source_port,
            args.target_host,
            args.target_port,
            args.mongodb_database,
            args.realtime,
            args.clear_target
        )
    else:
        raise ValueError(f"지원하지 않는 데이터베이스 유형: {args.db_type}")


def main():
    global replicator

    parser = argparse.ArgumentParser(description="실시간 데이터베이스 복제기")
    parser.add_argument("--db-type", choices=["redis", "mysql", "mongodb"], required=True, help="복제할 데이터베이스 유형")
    parser.add_argument("--source-host", default="localhost", help="원본 데이터베이스 서버 호스트")
    parser.add_argument("--source-port", type=int, help="원본 데이터베이스 서버 포트")
    parser.add_argument("--target-host", default="localhost", help="대상 데이터베이스 서버 호스트")
    parser.add_argument("--target-port", type=int, help="대상 데이터베이스 서버 포트")
    parser.add_argument("--realtime", action="store_true", help="실시간 동기화 모드 활성화")
    parser.add_argument("--clear-target", action="store_true", help="복제 시작 전 대상 데이터베이스의 모든 데이터 삭제")

    # Redis 특정 인자
    parser.add_argument("--num-dbs", type=int, default=16, help="동기화할 Redis 데이터베이스 수 (기본값: 16)")

    # MySQL 특정 인자
    parser.add_argument("--user", help="MySQL 사용자")
    parser.add_argument("--password", help="MySQL 비밀번호")
    parser.add_argument("--database", help="MySQL 데이터베이스 이름")

    # MongoDB 특정 인자
    parser.add_argument("--mongodb-database", help="MongoDB 데이터베이스 이름")

    args = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        replicator = create_replicator(args)
        replicator.replicate()
    except KeyboardInterrupt:
        print("\n사용자에 의해 프로그램이 중단되었습니다.")
    except Exception as e:
        print(f"\n예기치 않은 오류 발생: {e}")
    finally:
        print("프로그램을 종료합니다...")
        if replicator:
            replicator.stop()
        print("프로그램이 안전하게 종료되었습니다.")


if __name__ == "__main__":
    main()
