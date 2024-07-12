import os
import platform
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


def run_command(command, process_name):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True,
                               universal_newlines=True)

    def print_output(proc, name):
        for line in proc.stdout:
            print(f"[{name}] {line.strip()}")

    thread = threading.Thread(target=print_output, args=(process, process_name))
    thread.start()

    return_code = process.wait()
    thread.join()

    return return_code


def build_for_linux(dockerfile):
    print(f"\n{dockerfile} 빌드 시작")
    image_name = f"replicator-{dockerfile.lower()}"
    build_command = f"docker build -f dockers/Dockerfile.{dockerfile} -t {image_name} ."
    run_command(build_command, f"Docker Build {dockerfile}")

    volume_mount = f"-v {os.getcwd()}/dist:/app/dist"
    return run_command(f"docker run --rm {volume_mount} {image_name}", f"Docker Run {dockerfile}")


def build_for_mac():
    print("\n맥용 빌드 시작")
    run_command("pip install -r requirements.txt", "Mac pip install")
    run_command("pip install pyinstaller", "Mac PyInstaller install")
    return run_command("pyinstaller --onefile --name RealtimeReplicator_mac main.py", "Mac Build")


def build_for_windows():
    print("\n윈도우용 빌드 시작")
    run_command("pip install -r requirements.txt", "Windows pip install")
    run_command("pip install pyinstaller", "Windows PyInstaller install")
    return run_command("pyinstaller --onefile --name RealtimeReplicator_win.exe main.py", "Windows Build")


def main():
    if not os.path.exists("dist"):
        os.makedirs("dist")

    current_platform = platform.system()

    build_functions = {
        "Linux": lambda: build_for_linux("linux"),
        "Amazon Linux 2": lambda: build_for_linux("amazonlinux2"),
        "Amazon Linux 2023": lambda: build_for_linux("amazonlinux2023")
    }

    if current_platform == "Darwin":
        build_functions["Mac"] = build_for_mac
    elif current_platform == "Windows":
        build_functions["Windows"] = build_for_windows

    with ThreadPoolExecutor(max_workers=len(build_functions)) as executor:
        future_to_platform = {executor.submit(func): plat for plat, func in build_functions.items()}
        for future in as_completed(future_to_platform):
            plat = future_to_platform[future]
            try:
                return_code = future.result()
                if return_code == 0:
                    print(f"\n{plat} 빌드 성공")
                else:
                    print(f"\n{plat} 빌드 실패 (리턴 코드: {return_code})")
            except Exception as exc:
                print(f"\n{plat} 빌드 중 오류 발생: {exc}")

    print("\n모든 빌드 프로세스 완료")
    print("빌드된 실행 파일은 'dist' 폴더에 있습니다.")


if __name__ == "__main__":
    main()
