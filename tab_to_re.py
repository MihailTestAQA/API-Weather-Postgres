import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

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

def connect_db():
    """Подключение к базе данных"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

def compare_and_insert_data():
    """Сравнивает данные лотереи и погоды, вставляет результаты в total_results"""
    conn = connect_db()
    if conn is None:
        return
    
    try:
        # Получаем данные лотереи
        with conn.cursor() as cursor:
            lottery_query = '''
            SELECT номер_тиража, дата_время_тиража, шар1, шар2, шар3, шар4, шар5, шар6, шар7, шар8
            FROM lottery_4x20 
            ORDER BY дата_время_тиража
            '''
            cursor.execute(lottery_query)
            lottery_data = []
            for row in cursor.fetchall():
                lottery_data.append({
                    'номер_тиража': row[0],
                    'дата_время_тиража': row[1],
                    'шар1': row[2],
                    'шар2': row[3],
                    'шар3': row[4],
                    'шар4': row[5],
                    'шар5': row[6],
                    'шар6': row[7],
                    'шар7': row[8],
                    'шар8': row[9]
                })
            
            print(f"Получено записей лотереи: {len(lottery_data)}")
        
        # Получаем данные погоды с правильной структурой
        with conn.cursor() as cursor:
            weather_query = '''
            SELECT время_наблюдения, температура, влажность, давление, скорость_ветра, 
                   направление_ветра, тип_осадков
            FROM data_weather 
            ORDER BY время_наблюдения
            '''
            
            cursor.execute(weather_query)
            weather_data = []
            
            for row in cursor.fetchall():
                weather_data.append({
                    'время_наблюдения': row[0],
                    'температура': row[1],
                    'влажность': row[2],
                    'давление': row[3],
                    'ветер_скорость': row[4],  # скорость_ветра -> ветер_скорость
                    'ветер_направление': row[5],  # направление_ветра -> ветер_направление
                    'погодные_условия': row[6]  # тип_осадков -> погодные_условия
                })
            
            print(f"Получено записей погоды: {len(weather_data)}")
        
        # Очищаем таблицу перед заполнением
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE total_results RESTART IDENTITY")
        
        # Сравниваем данные и вставляем результаты
        matched_count = 0
        unmatched_count = 0
        
        for lottery in lottery_data:
            closest_weather = None
            min_time_diff = timedelta(minutes=10 + 1)  # Допуск ±10 минут
            
            for weather in weather_data:
                weather_time = weather['время_наблюдения']
                time_diff = abs(lottery['дата_время_тиража'] - weather_time)
                
                # Ищем данные в пределах 10 минут
                if time_diff <= timedelta(minutes=10) and time_diff < min_time_diff:
                    min_time_diff = time_diff
                    closest_weather = weather
                    closest_weather['разница_времени_минуты'] = int(time_diff.total_seconds() / 60)
            
            if closest_weather:
                # Вставляем совпавшие данные
                with conn.cursor() as cursor:
                    insert_query = '''
                    INSERT INTO total_results (
                        номер_тиража, дата_время_тиража, шар1, шар2, шар3, шар4, шар5, шар6, шар7, шар8,
                        время_наблюдения_погоды, температура, влажность, давление, ветер_скорость, 
                        ветер_направление, погодные_условия, разница_времени_минуты
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    cursor.execute(insert_query, (
                        lottery['номер_тиража'],
                        lottery['дата_время_тиража'],
                        lottery['шар1'],
                        lottery['шар2'],
                        lottery['шар3'],
                        lottery['шар4'],
                        lottery['шар5'],
                        lottery['шар6'],
                        lottery['шар7'],
                        lottery['шар8'],
                        closest_weather['время_наблюдения'],
                        closest_weather['температура'],
                        closest_weather['влажность'],
                        closest_weather['давление'],
                        closest_weather['ветер_скорость'],
                        closest_weather['ветер_направление'],
                        closest_weather['погодные_условия'],
                        closest_weather['разница_времени_минуты']
                    ))
                matched_count += 1
                print(f"✓ Сопоставлен тираж {lottery['номер_тиража']} ({lottery['дата_время_тиража']})")
                print(f"  с погодой от {closest_weather['время_наблюдения']} (разница: {closest_weather['разница_времени_минуты']} мин)")
            else:
                unmatched_count += 1
                # Показываем ближайшие доступные данные погоды для отладки
                closest_times = []
                for weather in weather_data:
                    time_diff = abs(lottery['дата_время_тиража'] - weather['время_наблюдения'])
                    closest_times.append((time_diff, weather['время_наблюдения']))
                
                closest_times.sort()
                if closest_times:
                    best_diff = int(closest_times[0][0].total_seconds() / 60)
                    print(f"✗ Тираж {lottery['номер_тиража']} ({lottery['дата_время_тиража']})")
                    print(f"  Ближайшая погода: {closest_times[0][1]} (разница: {best_diff} мин)")
                else:
                    print(f"✗ Тираж {lottery['номер_тиража']} ({lottery['дата_время_тиража']}) - нет данных погоды")
        
        conn.commit()
        
        print(f"\n=== Результаты сопоставления ===")
        print(f"Успешно сопоставлено: {matched_count}")
        print(f"Не найдено погоды: {unmatched_count}")
        print(f"Всего обработано: {len(lottery_data)}")
        
    except Exception as e:
        print(f"✗ Ошибка при сравнении данных: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()

def show_total_results():
    """Показывает содержимое таблицы total_results"""
    conn = connect_db()
    if conn is None:
        return
    
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM total_results')
            total_count = cursor.fetchone()[0]
            
            if total_count > 0:
                print(f"\n=== Содержимое total_results ({total_count} записей) ===")
                cursor.execute('''
                SELECT номер_тиража, дата_время_тиража, время_наблюдения_погоды, 
                       температура, разница_времени_минуты
                FROM total_results 
                ORDER BY номер_тиража
                ''')
                records = cursor.fetchall()
                
                for record in records:
                    print(f"Тираж {record[0]}: лотерея {record[1]}, погода {record[2]}, темп: {record[3]}°C, разница: {record[4]} мин")
            else:
                print("\nТаблица total_results пуста")
                
    except Exception as e:
        print(f"Ошибка при получении данных: {e}")
    finally:
        if conn:
            conn.close()

def main():
    """Основная функция"""
    print("=== Сравнение данных лотереи и погоды ===")
    print("Настройка: допуск по времени ±10 минут")
    
    # Сравниваем и вставляем данные
    compare_and_insert_data()
    
    # Показываем результаты
    show_total_results()
    
    print("\n✓ Работа завершена!")

if __name__ == "__main__":
    main()