from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import time
import subprocess
import sys
from log_config import setup_logging
from datetime import datetime

setup_logging()

def run_parser():
    """Запускает main.py как отдельный процесс"""
    try:
        logging.info("Начинаю запуск main.py...")
        
        # Открываем лог-файл для записи вывода main.py
        current_date = datetime.now().strftime("%Y-%m-%d")
        log_file = open(f"logs/parser_{current_date}.log", "a", encoding='utf-8')
        
        # Запускаем main.py и перенаправляем вывод в лог-файл
        result = subprocess.run(
            [sys.executable, "main.py"], 
            timeout=170,
            stdout=log_file,  # Перенаправляем стандартный вывод
            stderr=log_file,  # Перенаправляем ошибки
            text=True
        )
        
        log_file.close()
        
        if result.returncode == 0:
            logging.info("main.py завершил работу успешно")
            return True
        else:
            logging.error(f"main.py завершился с ошибкой. Код: {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error("main.py не ответил за 2 минуты 50 секунд - ПРЕВЫШЕНИЕ ВРЕМЕНИ")
        return False
    except Exception as e:
        logging.error(f"Критическая ошибка при запуске main.py: {e}")
        return False

def run_parser_with_restart():
    """Запуск main.py с проверкой и перезапуском"""
    max_attempts = 3
    timeout = 120
    
    logging.info(f"Начало процедуры запуска (макс. попыток: {max_attempts})")
    
    for attempt in range(max_attempts):
        logging.info(f"🔹 Попытка запуска #{attempt + 1}")
        
        start_time = time.time()
        success = run_parser()
        
        elapsed_time = time.time() - start_time
        if success and elapsed_time < timeout:
            logging.info(f"УСПЕХ! main.py выполнен за {elapsed_time:.1f} сек.")
            logging.info("─" * 50)
            return
        else:
            if not success:
                logging.warning("Запуск завершился с ошибкой")
            else:
                logging.warning(f"Работа заняла слишком много времени: {elapsed_time:.1f} сек.")
                
            if attempt < max_attempts - 1:
                logging.info("Перезапуск через 10 секунд...")
                time.sleep(10)
            else:
                logging.error("Достигнут лимит попыток перезапуска")
    
    logging.error("ВСЕ ПОПЫТКИ ЗАПУСКА ПРОВАЛИЛИСЬ")
    logging.info("─" * 50)

def main():
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_path = f"logs/parser_{current_date}.log"
    
    print("Запуск планировщика парсера...")
    print(f"Логи пишутся в файл: {log_path}")
    print("Логи старше 7 дней автоматически удаляются")
    print("⏹Для остановки нажмите Ctrl+C")
    print("-" * 50)
    
    scheduler = BlockingScheduler()
    
    times = ["10:02", "12:02", "13:02", "16:02", "16:22", "18:02", "20:02", "22:02"]
    
    for time_str in times:
        scheduler.add_job(
            run_parser_with_restart,
            'cron',
            hour=int(time_str.split(':')[0]),
            minute=int(time_str.split(':')[1])
        )
        logging.info(f"Запланирован запуск в {time_str}")
    
    logging.info("ПЛАНИРОВЩИК ЗАПУЩЕН И РАБОТАЕТ")
    logging.info("=" * 60)
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logging.info("Планировщик остановлен пользователем")
        print("\nПланировщик остановлен")
    except Exception as e:
        logging.error(f"Ошибка в работе планировщика: {e}")

if __name__ == "__main__":
    main()