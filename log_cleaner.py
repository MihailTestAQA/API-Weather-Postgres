from log_config import cleanup_old_logs
import sys

if __name__ == "__main__":
    print("Запуск очистки старых логов...")
    cleanup_old_logs()
    print("Очистка завершена")