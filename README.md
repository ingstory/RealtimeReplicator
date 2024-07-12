# RealtimeReplicator

![RealtimeReplicator Logo](https://via.placeholder.com/150)

RealtimeReplicator는 다양한 유형의 데이터베이스(Redis, MySQL, MongoDB)를 실시간으로 복제하고 동기화하는 강력한 도구입니다. 초기 데이터 복제 후에도 지속적으로 데이터 변경을 감지하고 처리하여 두 데이터베이스 간의 일관성을 유지합니다.

## 주요 기능

- 다중 데이터베이스 지원: Redis, MySQL, MongoDB
- 초기 전체 데이터 복제
- 실시간 데이터 변경 감지 및 동기화
- 유연한 설정 옵션
- 진행 상황 및 성능 모니터링

## 설치

1. 이 저장소를 클론합니다:
   ```
   git clone https://github.com/yourusername/RealtimeReplicator.git
   ```

2. 프로젝트 디렉토리로 이동합니다:
   ```
   cd RealtimeReplicator
   ```

3. 필요한 의존성을 설치합니다:
   ```
   pip install -r requirements.txt
   ```

## 사용법

기본 사용법:

```
python main.py --db-type [redis|mysql|mongodb] --source-host [SOURCE_HOST] --source-port [SOURCE_PORT] --target-host [TARGET_HOST] --target-port [TARGET_PORT] [추가 옵션]
```

예시:
```
python main.py --db-type redis --source-host localhost --source-port 6379 --target-host localhost --target-port 6380 --realtime
```

## 데이터베이스별 실시간 처리 조건

### Redis
- Redis 서버에서 PUBLISH/SUBSCRIBE 기능이 활성화되어 있어야 합니다.
- 복제 대상 키에 대한 키스페이스 알림이 활성화되어 있어야 합니다.

### MySQL
- MySQL 서버의 바이너리 로그 형식이 ROW로 설정되어 있어야 합니다.
- 복제를 위한 적절한 권한을 가진 사용자가 필요합니다.

### MongoDB
- MongoDB 서버에서 변경 스트림(Change Streams) 기능이 활성화되어 있어야 합니다.
- 복제셋 또는 샤딩된 클러스터 환경에서 실행되어야 합니다.

## 기여

버그 리포트, 기능 요청 및 풀 리퀘스트를 환영합니다. 주요 변경사항의 경우, 먼저 이슈를 열어 논의하고자 하는 변경 사항에 대해 이야기해 주세요.

## 라이선스

이 프로젝트는 MIT 라이선스 하에 있습니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

## 연락처

프로젝트 링크: [https://github.com/ingstory/RealtimeReplicator](https://github.com/yourusername/RealtimeReplicator)
