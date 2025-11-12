#  
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_parser():
    """Основная функция парсера"""
    try:
        logging.info("Запуск парсера...")
        
        # ТВОЙ КОД ПАРСЕРА ЗДЕСЬ
        print(f"Парсер работает: {datetime.now()}")
        time.sleep(10)  # Замени на реальный код
        
        logging.info("Парсер завершил работу")
        return True
        
    except Exception as e:
        logging.error(f"Ошибка парсера: {e}")
        return False

def run_parser_with_restart():
    """Запуск парсера с проверкой и перезапуском"""
    max_attempts = 3
    timeout = 180  # 3 минуты
    
    for attempt in range(max_attempts):
        logging.info(f"Попытка {attempt + 1} запуска парсера")
        
        # Запускаем парсер с таймаутом
        start_time = time.time()
        success = run_parser()
        
        # Проверяем результат
        if success and (time.time() - start_time) < timeout:
            logging.info("Парсер успешно завершил работу")
            return
        else:
            logging.warning("Парсер не ответил или завершился с ошибкой")
            if attempt < max_attempts - 1:
                logging.info("Перезапуск через 10 секунд...")
                time.sleep(10)
    
    logging.error("Все попытки запуска парсера провалились")

def main():
    scheduler = BlockingScheduler()
    
    # Расписание
    times = ["10:00", "12:00", "13:00", "16:00", "16:20", "18:00", "20:00", "22:00"]
    
    for time_str in times:
        scheduler.add_job(
            run_parser_with_restart,
            'cron',
            hour=int(time_str.split(':')[0]),
            minute=int(time_str.split(':')[1])
        )
    
    logging.info("Планировщик запущен")
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logging.info("Остановлено")

if __name__ == "__main__":
    main()