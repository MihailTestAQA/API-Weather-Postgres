from apscheduler.schedulers.background import BackgroundScheduler
import logging
import time
import subprocess
import sys
import signal
import threading
from log_config import setup_logging
from datetime import datetime

setup_logging()

is_running = True
# ПЕРЕКЛЮЧАТЕЛЬ: True - вывод в консоль, False - только в файл логов
CONSOLE_OUTPUT = True

def signal_handler(signum, frame):
    global is_running
    if CONSOLE_OUTPUT:
        print(f"Получен сигнал {signum}. Останавливаю планировщик...")
    logging.info(f"Получен сигнал остановки {signum}")
    is_running = False

def setup_signal_handlers():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def run_script(script_name):
    """Запускает скрипт и возвращает результат"""
    try:
        if CONSOLE_OUTPUT:
            print(f"Запуск {script_name}...")
        logging.info(f"Запуск {script_name}...")
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        log_filename = f"logs/{script_name.replace('.py', '')}_{current_date}.log"
        
        import os
        os.makedirs("logs", exist_ok=True)
        
        # Запускаем скрипт и перехватываем вывод
        result = subprocess.run(
            [sys.executable, script_name], 
            timeout=170,
            capture_output=True,
            text=True,
            encoding='utf-8',
            cwd=os.getcwd()
        )
        
        # Записываем вывод в лог-файл (ВСЕГДА)
        with open(log_filename, "a", encoding='utf-8') as log_file:
            log_file.write(f"=== Запуск {script_name} в {datetime.now()} ===\n")
            if result.stdout:
                log_file.write("STDOUT:\n" + result.stdout + "\n")
            if result.stderr:
                log_file.write("STDERR:\n" + result.stderr + "\n")
            log_file.write(f"Код возврата: {result.returncode}\n")
            log_file.write("=" * 50 + "\n\n")
        
        # Выводим результат в консоль (ТОЛЬКО ЕСЛИ CONSOLE_OUTPUT = True)
        if result.returncode == 0:
            if CONSOLE_OUTPUT:
                print(f"{script_name} завершен успешно")
                if result.stdout:
                    print(f"Вывод {script_name}:\n{result.stdout}")
            logging.info(f"{script_name} завершен успешно")
            return True
        else:
            if CONSOLE_OUTPUT:
                print(f"{script_name} завершен с ошибкой. Код: {result.returncode}")
                if result.stderr:
                    print(f"Ошибка {script_name}:\n{result.stderr}")
            logging.error(f"{script_name} завершен с ошибкой. Код: {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        if CONSOLE_OUTPUT:
            print(f"{script_name} превышено время выполнения (170 сек)")
        logging.error(f"{script_name} превышено время выполнения (170 сек)")
        return False
    except Exception as e:
        if CONSOLE_OUTPUT:
            print(f"Ошибка при запуске {script_name}: {e}")
        logging.error(f"Ошибка при запуске {script_name}: {e}")
        return False

def run_with_restart(script_name, max_attempts=3):
    """Запускает скрипт с перезапуском в случае неудачи"""
    if CONSOLE_OUTPUT:
        print(f"Запуск {script_name} с перезапуском (макс. попыток: {max_attempts})")
    logging.info(f"Запуск {script_name} с перезапуском (макс. попыток: {max_attempts})")
    
    for attempt in range(max_attempts):
        if not is_running:
            if CONSOLE_OUTPUT:
                print("Остановка - получен сигнал остановки")
            logging.info("Остановка - получен сигнал остановки")
            return False
            
        if CONSOLE_OUTPUT:
            print(f"Попытка {attempt + 1} для {script_name}")
        logging.info(f"Попытка {attempt + 1} для {script_name}")
        
        success = run_script(script_name)
        
        if success:
            if CONSOLE_OUTPUT:
                print(f"Успех! {script_name} выполнен с попытки {attempt + 1}")
            logging.info(f"Успех! {script_name} выполнен с попытки {attempt + 1}")
            return True
        else:
            if CONSOLE_OUTPUT:
                print(f"Запуск {script_name} завершился с ошибкой (попытка {attempt + 1})")
            logging.warning(f"Запуск {script_name} завершился с ошибкой (попытка {attempt + 1})")
            
            if attempt < max_attempts - 1:
                wait_time = 10
                if CONSOLE_OUTPUT:
                    print(f"Перезапуск через {wait_time} секунд...")
                logging.info(f"Перезапуск через {wait_time} секунд...")
                for i in range(wait_time):
                    if not is_running:
                        if CONSOLE_OUTPUT:
                            print("Прервано ожидание перезапуска")
                        logging.info("Прервано ожидание перезапуска")
                        return False
                    time.sleep(1)
            else:
                if CONSOLE_OUTPUT:
                    print(f"Все {max_attempts} попыток запуска {script_name} провалились")
                logging.error(f"Все {max_attempts} попыток запуска {script_name} провалились")
    
    return False

def run_lottery_parser():
    if not is_running:
        return
    if CONSOLE_OUTPUT:
        print("Запуск lottery_parser.py по расписанию")
    logging.info("Запуск lottery_parser.py по расписанию")
    run_with_restart("lottery_parser.py")

def run_main_parser():
    if not is_running:
        return
    if CONSOLE_OUTPUT:
        print("Запуск main.py по расписанию")
    logging.info("Запуск main.py по расписанию")
    run_with_restart("main.py")

def user_input_listener():
    global is_running
    while is_running:
        try:
            user_input = input().strip().lower()
            if user_input in ['stop', 'exit', 'quit', 'стоп', 'выход']:
                if CONSOLE_OUTPUT:
                    print("Получена команда остановки...")
                is_running = False
                break
            elif user_input in ['status', 'статус']:
                if CONSOLE_OUTPUT:
                    print(f"Статус: {'работает' if is_running else 'останавливается'}")
            elif user_input in ['help', 'помощь']:
                if CONSOLE_OUTPUT:
                    print("Команды: stop, exit, quit, стоп, выход - остановить")
                    print("status, статус - показать статус")
                    print("help, помощь - справка")
            else:
                if CONSOLE_OUTPUT:
                    print(f"Неизвестная команда: {user_input}")
        except (EOFError, KeyboardInterrupt):
            break
        except Exception as e:
            if CONSOLE_OUTPUT:
                print(f"Ошибка ввода: {e}")

def main():
    global is_running
    
    if CONSOLE_OUTPUT:
        print("Запуск планировщика парсеров")
        print("main.py - запускается в 10:02, 12:02, 13:02, 16:02, 16:22, 18:02, 20:02, 22:02")
        print("lottery_parser.py - запускается в 09:30")
        print("Каждый скрипт имеет 3 попытки запуска с перезапуском через 10 секунд")
        print("Логи в папке: logs/")
        print("Для остановки: Ctrl+C или команда 'stop'")
        print("-" * 50)
    
    import os
    if not os.path.exists("lottery_parser.py"):
        if CONSOLE_OUTPUT:
            print("Ошибка: lottery_parser.py не найден")
        logging.error("Файл lottery_parser.py не найден")
        return
    
    if not os.path.exists("main.py"):
        if CONSOLE_OUTPUT:
            print("Ошибка: main.py не найден")
        logging.error("Файл main.py не найден")
        return
    
    setup_signal_handlers()
    
    input_thread = threading.Thread(target=user_input_listener, daemon=True)
    input_thread.start()
    
    scheduler = BackgroundScheduler()
    
    main_times = ["10:02", "12:02", "13:02", "16:02", "16:22", "18:02", "20:02", "22:02"]
    
    for time_str in main_times:
        scheduler.add_job(
            run_main_parser,
            'cron',
            hour=int(time_str.split(':')[0]),
            minute=int(time_str.split(':')[1]),
            name=f'main_parser_{time_str}'
        )
        if CONSOLE_OUTPUT:
            print(f"Запланирован запуск main.py в {time_str}")
        logging.info(f"Запланирован запуск main.py в {time_str}")
    
    scheduler.add_job(
        run_lottery_parser,
        'cron',
        hour=9,
        minute=30,
        name='lottery_parser_daily'
    )
    if CONSOLE_OUTPUT:
        print("Запланирован запуск lottery_parser.py в 09:30")
    logging.info("Запланирован запуск lottery_parser.py в 09:30")
    
    scheduler.start()
    logging.info("Планировщик запущен и работает")
    if CONSOLE_OUTPUT:
        print("Планировщик запущен и работает")
        print("Ожидание заданий...")
    
    try:
        while is_running:
            time.sleep(1)
            
        if CONSOLE_OUTPUT:
            print("Остановка планировщика...")
        logging.info("Начало остановки планировщика")
        
    except KeyboardInterrupt:
        if CONSOLE_OUTPUT:
            print("Получен Ctrl+C, останавливаю...")
        is_running = False
    except Exception as e:
        logging.error(f"Ошибка в основном цикле: {e}")
        is_running = False
    finally:
        if CONSOLE_OUTPUT:
            print("Завершение работы...")
        scheduler.shutdown(wait=True)
        logging.info("Планировщик остановлен")
        if CONSOLE_OUTPUT:
            print("Планировщик остановлен")

if __name__ == "__main__":
    main()