# Импорт необходимых библиотек
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import psycopg2
from datetime import datetime
import re
import os
import sys
from dotenv import load_dotenv

# Установка UTF-8 кодировки для Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Загружаем переменные из .env
load_dotenv()

# Конфигурация БД
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def test_db_connection():
    """Проверяем подключение к БД перед началом работы"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print("[OK] Подключение к БД успешно")
        return True
    except Exception as e:
        print(f"[ERROR] Ошибка подключения к БД: {e}")
        return False

def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

def draw_exists(draw_number):
    conn = connect_db()
    if conn is None:
        return True
    
    try:
        with conn.cursor() as cursor:
            check_query = 'SELECT 1 FROM lottery_4x20 WHERE "номер_тиража" = %s'
            cursor.execute(check_query, (draw_number,))
            return cursor.fetchone() is not None
    except Exception as e:
        print(f"Ошибка при проверке тиража: {e}")
        return True
    finally:
        if conn:
            conn.close()

def add_draw_to_db(draw_data):
    if draw_exists(draw_data['номер_тиража']):
        return False
    
    conn = connect_db()
    if conn is None:
        return False
    
    try:
        with conn.cursor() as cursor:
            insert_query = '''
            INSERT INTO lottery_4x20 
            ("номер_тиража", "дата_время_тиража", "шар1", "шар2", "шар3", "шар4", "шар5", "шар6", "шар7", "шар8")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            cursor.execute(insert_query, (
                draw_data['номер_тиража'],
                draw_data['дата_время_тиража'],
                draw_data['шар1'],
                draw_data['шар2'],
                draw_data['шар3'],
                draw_data['шар4'],
                draw_data['шар5'],
                draw_data['шар6'],
                draw_data['шар7'],
                draw_data['шар8']
            ))
            conn.commit()
            print(f"[OK] Добавлен тираж {draw_data['номер_тиража']}")
            return True
    except Exception as e:
        print(f"[ERROR] Ошибка при добавлении тиража: {e}")
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def parse_datetime(date_str):
    try:
        date_str = date_str.strip()
        
        for fmt in ('%d.%m.%Y %H:%M', '%d.%m.%y %H:%M', '%Y-%m-%d %H:%M', 
                   '%d.%m.%Y', '%d.%m.%y', '%Y-%m-%d'):
            try:
                date_obj = datetime.strptime(date_str, fmt)
                if ':' not in date_str:
                    date_obj = datetime.combine(date_obj.date(), datetime.strptime("12:00", "%H:%M").time())
                return date_obj
            except ValueError:
                continue
        
        return datetime.now()
    except Exception as e:
        print(f"Ошибка парсинга даты '{date_str}': {e}")
        return datetime.now()

def parse_draw_number(draw_str):
    try:
        clean_draw = re.sub(r'\D', '', draw_str)
        return int(clean_draw) if clean_draw else 0
    except Exception as e:
        print(f"Ошибка парсинга номера тиража: {e}")
        return 0

def parser():
    print("Парсер лотереи 4x20")
    
    if not test_db_connection():
        print("Работа парсера прервана из-за ошибки подключения к БД")
        return
    
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_options)
    added_count = 0
    skipped_count = 0
    
    try:
        print("Загрузка страницы...")
        driver.get("https://www.lotonews.ru/draws/archive/4x20")
        
        driver.implicitly_wait(10)
        
        table = driver.find_element("tag name", "table")
        rows = table.find_elements("tag name", "tr")
        
        print(f"Найдено строк: {len(rows)}")
        print("Обработка данных...")
        
        for i in range(2, len(rows)):
            cells = rows[i].find_elements("tag name", "td")
            if len(cells) >= 4:
                date = cells[0].text
                draw = cells[1].text
                numbers = [num for num in cells[2].text.split('\n') if num.isdigit()]
                
                if len(numbers) >= 8:
                    draw_number = parse_draw_number(draw)
                    if draw_number == 0:
                        skipped_count += 1
                        continue
                    
                    numbers_int = [int(num) for num in numbers[:8]]
                    draw_datetime = parse_datetime(date)
                    
                    draw_data = {
                        'номер_тиража': draw_number,
                        'дата_время_тиража': draw_datetime,
                        'шар1': numbers_int[0],
                        'шар2': numbers_int[1],
                        'шар3': numbers_int[2],
                        'шар4': numbers_int[3],
                        'шар5': numbers_int[4],
                        'шар6': numbers_int[5],
                        'шар7': numbers_int[6],
                        'шар8': numbers_int[7]
                    }
                    
                    if add_draw_to_db(draw_data):
                        added_count += 1
                    else:
                        skipped_count += 1
        
        print(f"\n=== Результаты ===")
        print(f"Добавлено: {added_count}")
        print(f"Пропущено: {skipped_count}")
        
    except Exception as e:
        print(f"Ошибка парсера: {e}")
        driver.save_screenshot("error.png")
        
    finally:
        driver.quit()
        print("Парсер завершил работу")

if __name__ == "__main__":
    parser()