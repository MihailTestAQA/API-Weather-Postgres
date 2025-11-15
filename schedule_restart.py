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

# Глобальная переменная для контроля работы скрипта
is_running = True

def signal_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown"""
    global is_running
    print(f"\nПолучен сигнал {signum}. Останавливаю планировщик...")
    logging.info(f"Получен сигнал остановки {signum}")
    is_running = False

def setup_signal_handlers():
    """Настройка обработчиков сигналов"""
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler) # kill command

def run_parser(script_name="main.py"):
    """Запускает указанный скрипт как отдельный процесс"""
    try:
        logging.info(f"Начинаю запуск {script_name}...")
        
        # Открываем лог-файл для записи вывода
        current_date = datetime.now().strftime("%Y-%m-%d")
        log_file = open(f"logs/{script_name.replace('.py', '')}_{current_date}.log", "a", encoding='utf-8')
        
        # Запускаем скрипт и перенаправляем вывод в лог-файл
        result = subprocess.run(
            [sys.executable, script_name], 
            timeout=170,
            stdout=log_file,
            stderr=log_file,
            text=True
        )
        
        log_file.close()
        
        if result.returncode == 0:
            logging.info(f"{script_name} завершил работу успешно")
            return True
        else:
            logging.error(f"{script_name} завершился с ошибкой. Код: {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error(f"{script_name} не ответил за 2 минуты 50 секунд - ПРЕВЫШЕНИЕ ВРЕМЕНИ")
        return False
    except Exception as e:
        logging.error(f"Критическая ошибка при запуске {script_name}: {e}")
        return False

def run_parser_with_restart(script_name="main.py"):
    """Запуск скрипта с проверкой и перезапуском"""
    max_attempts = 3
    timeout = 120
    
    logging.info(f"Начало процедуры запуска {script_name} (макс. попыток: {max_attempts})")
    
    for attempt in range(max_attempts):
        if not is_running:
            logging.info("Остановка запуска парсера - получен сигнал остановки")
            return False
            
        logging.info(f"🔹 Попытка запуска #{attempt + 1} для {script_name}")
        
        start_time = time.time()
        success = run_parser(script_name)
        
        elapsed_time = time.time() - start_time
        if success and elapsed_time < timeout:
            logging.info(f"УСПЕХ! {script_name} выполнен за {elapsed_time:.1f} сек.")
            logging.info("─" * 50)
            return True
        else:
            if not success:
                logging.warning(f"Запуск {script_name} завершился с ошибкой")
            else:
                logging.warning(f"Работа {script_name} заняла слишком много времени: {elapsed_time:.1f} сек.")
                
            if attempt < max_attempts - 1:
                logging.info("Перезапуск через 10 секунд...")
                # Проверяем флаг каждую секунду во время ожидания
                for _ in range(10):
                    if not is_running:
                        logging.info("Прервано ожидание перезапуска - получен сигнал остановки")
                        return False
                    time.sleep(1)
            else:
                logging.error(f"Достигнут лимит попыток перезапуска для {script_name}")
    
    logging.error(f"ВСЕ ПОПЫТКИ ЗАПУСКА {script_name} ПРОВАЛИЛИСЬ")
    logging.info("─" * 50)
    return False

def run_lottery_parser():
    """Запускает lottery_parser_test.py один раз в сутки"""
    if not is_running:
        return
    logging.info("Запуск lottery_parser_test.py по расписанию")
    run_parser_with_restart("lottery_parser_test.py")

def run_main_parser():
    """Запускает main.py по обычному расписанию"""
    if not is_running:
        return
    logging.info("Запуск main.py по расписанию")
    run_parser_with_restart("main.py")

def user_input_listener():
    """Слушатель ввода пользователя для ручной остановки"""
    global is_running
    while is_running:
        try:
            user_input = input().strip().lower()
            if user_input in ['stop', 'exit', 'quit', 'стоп', 'выход']:
                print("Получена команда остановки...")
                is_running = False
                break
            elif user_input in ['status', 'статус']:
                print(f"Статус: {'работает' if is_running else 'останавливается'}")
            elif user_input in ['help', 'помощь']:
                print("Доступные команды:")
                print("  stop, exit, quit, стоп, выход - остановить скрипт")
                print("  status, статус - показать статус")
                print("  help, помощь - показать эту справку")
            else:
                print(f"Неизвестная команда: {user_input}")
                print("Введите 'help' для списка команд")
        except (EOFError, KeyboardInterrupt):
            break
        except Exception as e:
            print(f"Ошибка ввода: {e}")

def main():
    global is_running
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    print("=" * 60)
    print("ЗАПУСК ПЛАНИРОВЩИКА ПАРСЕРОВ")
    print("=" * 60)
    print("Логи пишутся в файлы: logs/")
    print("main.py - запускается несколько раз в день")
    print("lottery_parser_test.py - запускается 1 раз в день в 9:30")
    print("\nСПОСОБЫ ОСТАНОВКИ:")
    print("  1. Нажмите Ctrl+C")
    print("  2. Введите команду: stop, exit, quit, стоп, выход")
    print("  3. Откройте новую консоль и выполните: pkill -f название_скрипта.py")
    print("\nДОПОЛНИТЕЛЬНЫЕ КОМАНДЫ:")
    print("  status, статус - показать статус работы")
    print("  help, помощь - показать эту справку")
    print("-" * 60)
    
    # Настраиваем обработчики сигналов
    setup_signal_handlers()
    
    # Запускаем слушатель ввода в отдельном потоке
    input_thread = threading.Thread(target=user_input_listener, daemon=True)
    input_thread.start()
    
    # Создаем и настраиваем планировщик
    scheduler = BackgroundScheduler()
    
    # Расписание для main.py (несколько раз в день)
    main_times = ["10:02", "12:02", "13:02", "16:02", "16:22", "18:02", "20:02", "22:02"]
    
    for time_str in main_times:
        scheduler.add_job(
            run_main_parser,
            'cron',
            hour=int(time_str.split(':')[0]),
            minute=int(time_str.split(':')[1]),
            name=f'main_parser_{time_str}'
        )
        logging.info(f"Запланирован запуск main.py в {time_str}")
    
    # Расписание для lottery_parser_test.py (один раз в день в 9:30)
    scheduler.add_job(
        run_lottery_parser,
        'cron',
        hour=9,
        minute=30,
        name='lottery_parser_daily'
    )
    logging.info("Запланирован запуск lottery_parser.py в 09:30")
    
    # Запускаем планировщик
    scheduler.start()
    logging.info("ПЛАНИРОВЩИК ЗАПУЩЕН И РАБОТАЕТ")
    print("ПЛАНИРОВЩИК ЗАПУЩЕН И РАБОТАЕТ")
    print("Ожидание заданий или команды остановки...")
    
    try:
        # Основной цикл работы
        while is_running:
            time.sleep(1)
            
        # Graceful shutdown
        print("\nОСТАНОВКА ПЛАНИРОВЩИКА...")
        logging.info("Начало остановки планировщика")
        
    except KeyboardInterrupt:
        print("\nПолучен Ctrl+C, останавливаю...")
        is_running = False
    except Exception as e:
        logging.error(f"Ошибка в основном цикле: {e}")
        is_running = False
    finally:
        # Останавливаем планировщик
        print("Завершаем выполнение задач...")
        scheduler.shutdown(wait=True)
        logging.info("Планировщик остановлен")
        print("ПЛАНИРОВЩИК ОСТАНОВЛЕН")
        print("До свидания!")

if __name__ == "__main__":
    main()