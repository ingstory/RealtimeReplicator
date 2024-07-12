# RealtimeReplicator

RealtimeReplicator는 다양한 유형의 데이터베이스(Redis, MySQL, MongoDB)를 실시간으로 복제하고 동기화하는 강력한 도구입니다. 초기 데이터 복제 후에도 지속적으로 데이터 변경을 감지하고 처리하여 두 데이터베이스 간의 일관성을 유지합니다.

## 주요 기능

- 다중 데이터베이스 지원: Redis, MySQL, MongoDB
- 초기 전체 데이터 복제
- 실시간 데이터 변경 감지 및 동기화
- 크로스 플랫폼 지원: Linux, Windows, macOS
- 병렬 빌드 프로세스

## 지원하는 데이터베이스 및 요구사항

### Redis
- Redis 서버에서 PUBLISH/SUBSCRIBE 기능이 활성화되어 있어야 합니다.
- 복제 대상 키에 대한 키스페이스 알림이 활성화되어 있어야 합니다.

### MySQL
- MySQL 서버의 바이너리 로그 형식이 ROW로 설정되어 있어야 합니다.
- 복제를 위한 적절한 권한을 가진 사용자가 필요합니다.

### MongoDB
- MongoDB 서버에서 변경 스트림(Change Streams) 기능이 활성화되어 있어야 합니다.
- 복제셋 또는 샤딩된 클러스터 환경에서 실행되어야 합니다.

## 설치

1. 이 저장소를 클론합니다:
   ```
   git clone https://github.com/ingstory/RealtimeReplicator.git
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

RealtimeReplicator는 명령줄 인터페이스를 통해 실행됩니다. 기본 사용법은 다음과 같습니다:

```
python main.py --db-type [redis|mysql|mongodb] --source-host [SOURCE_HOST] --source-port [SOURCE_PORT] --target-host [TARGET_HOST] --target-port [TARGET_PORT] [추가 옵션]
```

### 주요 옵션:

- `--db-type`: 복제할 데이터베이스 유형 (redis, mysql, mongodb)
- `--source-host`: 원본 데이터베이스 서버 호스트
- `--source-port`: 원본 데이터베이스 서버 포트
- `--target-host`: 대상 데이터베이스 서버 호스트
- `--target-port`: 대상 데이터베이스 서버 포트
- `--realtime`: 실시간 동기화 모드 활성화
- `--clear-target`: 복제 시작 전 대상 데이터베이스의 모든 데이터 삭제

### 데이터베이스별 추가 옵션:

#### Redis:
- `--num-dbs`: 동기화할 Redis 데이터베이스 수 (기본값: 16)

#### MySQL:
- `--user`: MySQL 사용자
- `--password`: MySQL 비밀번호
- `--database`: MySQL 데이터베이스 이름

#### MongoDB:
- `--mongodb-database`: MongoDB 데이터베이스 이름

### 예시:

1. Redis 복제:
   ```
   python main.py --db-type redis --source-host localhost --source-port 6379 --target-host localhost --target-port 6380 --realtime
   ```

2. MySQL 복제:
   ```
   python main.py --db-type mysql --source-host localhost --source-port 3306 --target-host localhost --target-port 3307 --user myuser --password mypassword --database mydb --realtime
   ```

3. MongoDB 복제:
   ```
   python main.py --db-type mongodb --source-host localhost --source-port 27017 --target-host localhost --target-port 27018 --mongodb-database mydb --realtime
   ```

## 빌드

여러 플랫폼용 실행 파일을 빌드하려면 다음 명령어를 실행하세요:

```
python build.py
```

이 명령은 다음과 같은 빌드 프로세스를 병렬로 수행합니다:

1. Linux용 Docker 빌드
2. Amazon Linux 2용 Docker 빌드
3. Amazon Linux 2023용 Docker 빌드
4. 현재 플랫폼(Windows 또는 macOS)용 네이티브 빌드

빌드 프로세스는 각 플랫폼별로 필요한 의존성을 설치하고, PyInstaller를 사용하여 실행 파일을 생성합니다. Docker 빌드의 경우, 해당 Linux 배포판에 맞는 Dockerfile을 사용합니다.

빌드된 실행 파일은 'dist' 폴더에 저장됩니다.

### 주의사항

- Docker를 사용한 빌드를 위해서는 Docker가 설치되어 있어야 합니다.
- 각 데이터베이스 유형별로 특정 설정이 필요할 수 있습니다. 자세한 내용은 위의 '지원하는 데이터베이스 및 요구사항' 섹션을 참조하세요.
- 실제 운영 환경에서 사용하기 전에 충분한 테스트를 진행하세요.

## 기여

버그 리포트, 기능 요청 및 풀 리퀘스트를 환영합니다. 주요 변경사항의 경우, 먼저 이슈를 열어 논의하고자 하는 변경 사항에 대해 이야기해 주세요.

## 라이선스

이 프로젝트는 MIT 라이선스 하에 있습니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.
