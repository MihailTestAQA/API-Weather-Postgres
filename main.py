import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

def save_weather_to_db(data_weather):
    """Сохраняет данные о погоде в базу данных PostgreSQL"""
    
    # Данные для подключения к БД.
    db_config = {
        'host': os.getenv('DB_HOST'),           # Берем только из .env
        'port': os.getenv('DB_PORT'),           
        'database': os.getenv('DB_NAME'),       
        'user': os.getenv('DB_USER'),           
        'password': os.getenv('DB_PASSWORD')  
    }
    
    try:
        # Подключаемся к базе данных
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # SQL запрос для вставки данных
        insert_query = """
        INSERT INTO data_weather (
            "время_наблюдения",
            "давление", 
            "влажность",
            "тип_осадков",
            "скорость_ветра",
            "температура",
            "направление_ветра"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        # Подготавливаем данные для вставки
        data_to_insert = (
            data_weather['observation_time'],  # время_наблюдения
            data_weather['pressure'],          # давление
            data_weather['humidity'],          # влажность
            data_weather['precipitation'],     # тип_осадков
            data_weather['wind_speed'],        # скорость_ветра
            data_weather['temperature'],       # температура
            data_weather['wind_direction']     # направление_ветра
        )
        
        # Выполняем запрос
        cursor.execute(insert_query, data_to_insert)
        
        # Подтверждаем изменения
        conn.commit()
        
        print("Данные успешно сохранены в базу данных!")
        
    except psycopg2.IntegrityError:
        # Если запись с таким временем наблюдения уже существует
        print("Запись с таким временем наблюдения уже существует в базе")
        conn.rollback()
        
    except psycopg2.Error as e:
        print(f"Ошибка при работе с базой данных: {e}")
        conn.rollback()
        
    finally:
        # Закрываем соединение
        if conn:
            cursor.close()
            conn.close()

# Основной код
from weather_api import WeatherAPI

# Создаем объект погодного API
weather_api = WeatherAPI()

# Получаем реальные данные о погоде
data_weather = weather_api.get_data_weather(datetime.now(), "Moscow")

if data_weather:
    # Сохраняем данные в базу данных
    save_weather_to_db(data_weather)
else:
    print("Не удалось получить данные о погоде")