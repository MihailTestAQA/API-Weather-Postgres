import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def lottery_db():
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'lottery_db'),
        'user': os.getenv('DB_USER', 'lottery_user'),
        'password': os.getenv('DB_PASSWORD', '123456')
    }
    
    print("Тестируем подключение к PostgreSQL...")
    print(f"Хост: {db_config['host']}")
    print(f"Порт: {db_config['port']}")
    print(f"База: {db_config['database']}")
    print(f"Пользователь: {db_config['user']}")
    print(f"Пароль: {'*' * len(db_config['password'])}")
    
    try:
        connection = psycopg2.connect(**db_config)
        print("Подключение к БД УСПЕШНО!")
        
        cursor = connection.cursor()

        # Создаем таблицу для данных о погоде
        # Связываем данные по дате/времени 
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_weather (
                "id" SERIAL PRIMARY KEY,
                "время_наблюдения" TIMESTAMP NOT NULL UNIQUE,
                "давление" INTEGER,
                "влажность" INTEGER,
                "тип_осадков" VARCHAR(20),
                "скорость_ветра" DECIMAL(5,2),
                "температура" DECIMAL(4,1),
                "направление_ветра" VARCHAR(20),
                "создано" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lottery_4x20 (
                "айди" SERIAL PRIMARY KEY,                                   -- Автоинкрементный первичный ключ
                "номер_тиража" INTEGER UNIQUE NOT NULL,                   -- Уникальный номер тиража
                "дата_время_тиража" TIMESTAMP NOT NULL,                   -- Дата и время тиража
                "шар1" INTEGER NOT NULL,                                -- Первое число
                "шар2" INTEGER NOT NULL,                                -- Второе число
                "шар3" INTEGER NOT NULL,                                -- Третье число
                "шар4" INTEGER NOT NULL,                                -- Четвертое число
                "шар5" INTEGER NOT NULL,                                -- Пятое число
                "шар6" INTEGER NOT NULL,                                -- Шестое число
                "шар7" INTEGER NOT NULL,                                -- Седьмое число
                "шар8" INTEGER NOT NULL,                                -- Восьмое число
                "создано" TIMESTAMP DEFAULT CURRENT_TIMESTAMP            -- Время создания записи
    )
""")
        connection.commit()
        print("Таблицы созданы успешно!")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        return False

if __name__ == "__main__":
    lottery_db()