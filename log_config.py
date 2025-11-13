# Импортируем модуль для логирования
import logging
# Импортируем модуль для работы с операционной системой (файлы, папки)
import os
# Импортируем модуль для поиска файлов по шаблону
import glob
# Импортируем классы для работы с датой и временем
from datetime import datetime, timedelta

def cleanup_old_logs():
    """Очищает логи старше 7 дней"""
    # Задаем имя папки для логов
    log_dir = "logs"
    # Проверяем существует ли папка logs
    if not os.path.exists(log_dir):
        return  # Если папки нет, выходим из функции
    
    # Вычисляем дату, которая была 7 дней назад
    cutoff_date = datetime.now() - timedelta(days=7)
    # Ищем все файлы логов по шаблону "parser_*.log" в папке logs
    log_files = glob.glob(os.path.join(log_dir, "parser_*.log"))
    
    # Счетчик удаленных файлов
    deleted_count = 0
    # Перебираем все найденные лог-файлы
    for log_file in log_files:
        try:
            # Получаем только имя файла без пути
            filename = os.path.basename(log_file)
            # Извлекаем дату из имени файла: parser_2024-01-15.log -> 2024-01-15
            date_str = filename.replace("parser_", "").replace(".log", "")
            # Преобразуем строку с датой в объект datetime
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
            
            # Проверяем, старше ли файл 7 дней
            if file_date < cutoff_date:
                # Удаляем файл
                os.remove(log_file)
                # Увеличиваем счетчик удаленных файлов
                deleted_count += 1
        except (ValueError, Exception):
            # Если возникла ошибка (неправильный формат даты и т.д.), пропускаем файл
            continue

def setup_logging():
    """Настройка логирования ТОЛЬКО в файл"""
    
    # Очищаем старые логи при каждом запуске программы
    cleanup_old_logs()
    
    # Создаем папку для логов если её нет
    log_dir = "logs"
    if not os.path.exists(log_dir):
        # Создаем папку рекурсивно
        os.makedirs(log_dir)
    
    # Получаем текущую дату в формате ГГГГ-ММ-ДД
    current_date = datetime.now().strftime("%Y-%m-%d")
    # Формируем полный путь к файлу лога
    log_filename = f"{log_dir}/parser_{current_date}.log"
    
    # Создаем форматтер для понятного вывода логов
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(message)s',  # Формат: время | уровень | сообщение
        datefmt='%Y-%m-%d %H:%M:%S'  # Формат даты: ГГГГ-ММ-ДД ЧЧ:ММ:СС
    )
    
    # Получаем корневой логгер
    logger = logging.getLogger()
    # Устанавливаем уровень логирования - INFO и выше (INFO, WARNING, ERROR)
    logger.setLevel(logging.INFO)
    
    # Очищаем все существующие обработчики логов
    logger.handlers.clear()
    
    # Создаем обработчик для записи в файл
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    # Применяем наш форматтер к обработчику
    file_handler.setFormatter(formatter)
    # Устанавливаем уровень логирования для файлового обработчика
    file_handler.setLevel(logging.INFO)
    
    # Добавляем файловый обработчик к логгеру
    logger.addHandler(file_handler)
    
    # Получаем логгер библиотеки APScheduler
    aps_logger = logging.getLogger('apscheduler')
    # Устанавливаем более высокий уровень для APScheduler (только WARNING и ERROR)
    aps_logger.setLevel(logging.WARNING)
    # Отключаем распространение логов APScheduler в корневой логгер
    aps_logger.propagate = False
    
    # Логируем информацию о запуске системы логирования
    logging.info("=" * 60)  # Разделительная линия
    logging.info("СИСТЕМА ЛОГИРОВАНИЯ ЗАПУЩЕНА")  # Заголовок
    logging.info(f"Логи сохраняются в: {log_filename}")  # Путь к файлу логов
    logging.info(f"Автоочистка: логи старше 7 дней удаляются автоматически")  # Инфо об очистке
    logging.info("=" * 60)  # Закрывающая разделительная линия