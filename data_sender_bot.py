# Отправка данных из PostgreSQL в Telegram бот
#
import psycopg2
import requests
import json
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# Загрузка переменных из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BotDataSender:
    def __init__(self, db_config=None, bot_token=None, chat_id=None):
        # Получение настроек из .env если не переданы явно
        self.db_config = db_config or {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': int(os.getenv('DB_PORT', 5432))
        }
        
        self.bot_token = bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = chat_id or os.getenv('TELEGRAM_CHAT_ID')
        
        if not self.bot_token:
            raise ValueError("Токен бота не указан. Укажите в .env файле как TELEGRAM_BOT_TOKEN")
        if not self.chat_id:
            raise ValueError("ID чата не указан. Укажите в .env файле как TELEGRAM_CHAT_ID")
        
        self.telegram_url = f"https://api.telegram.org/bot{self.bot_token}/"
        logger.info("BotDataSender инициализирован")

    def get_data_from_postgres(self, query):
        # Получение данных из PostgreSQL
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            
            # Преобразование в список словарей
            result = []
            for row in data:
                result.append(dict(zip(columns, row)))
            
            cursor.close()
            conn.close()
            
            logger.info(f"Получено {len(result)} записей из базы данных")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при получении данных из PostgreSQL: {e}")
            return []

    def format_data_for_telegram(self, data, max_records=10):
        if not data:
            return "Нет данных для отображения"
        
        # Ограничиваем количество записей для сообщения
        display_data = data[:max_records]
        
        formatted_text = f"Найдено записей: {len(data)}\n"
        formatted_text += f"Время выгрузки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        for i, record in enumerate(display_data, 1):
            formatted_text += f"Запись #{i}:\n"
            for key, value in record.items():
                # Обрезаем длинные значения
                if value and len(str(value)) > 50:
                    value = str(value)[:47] + "..."
                formatted_text += f"   • {key}: {value}\n"
            formatted_text += "\n"
        
        # Если записей больше чем показываем, добавляем информацию
        if len(data) > max_records:
            formatted_text += f"... и еще {len(data) - max_records} записей\n"
        
        return formatted_text

    def split_long_message(self, text, max_length=4000):
        #Разделяет длинное сообщение на части
        if len(text) <= max_length:
            return [text]
        
        parts = []
        while text:
            if len(text) <= max_length:
                parts.append(text)
                break
            else:
                # Ищем точку разрыва
                split_pos = text.rfind('\n\n', 0, max_length)
                if split_pos == -1:
                    split_pos = text.rfind('\n', 0, max_length)
                if split_pos == -1:
                    split_pos = max_length
                    
                parts.append(text[:split_pos])
                text = text[split_pos:].lstrip()
        
        return parts

    def send_to_telegram(self, message):
        try:
            # Разделяем сообщение если оно слишком длинное
            message_parts = self.split_long_message(message)
            
            success = True
            for i, part in enumerate(message_parts):
                if len(message_parts) > 1:
                    part = f"Часть {i+1}/{len(message_parts)}\n\n{part}"
                
                url = self.telegram_url + "sendMessage"
                payload = {
                    'chat_id': self.chat_id,
                    'text': part,
                    'parse_mode': 'Markdown'
                }
                
                response = requests.post(url, json=payload)
                
                if response.status_code == 200:
                    logger.info(f"Часть {i+1} успешно отправлена")
                else:
                    logger.error(f"Ошибка отправки части {i+1}: {response.text}")
                    success = False
            
            return success
                
        except Exception as e:
            logger.error(f"Ошибка при отправке в Telegram: {e}")
            return False

    def send_data_as_file(self, data, filename="data.json", message="Данные из базы данных"):

        #Отправка данных как файла (для больших объемов)
        try:
            # Сначала отправляем поясняющее сообщение
            text_url = self.telegram_url + "sendMessage"
            text_payload = {
                'chat_id': self.chat_id,
                'text': f"{message}\n📎 Данные отправлены как файл"
            }
            requests.post(text_url, json=text_payload)
            
            # Создаем временный файл
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            
            # Отправка файла
            file_url = self.telegram_url + "sendDocument"
            with open(filename, 'rb') as file:
                files = {'document': (filename, file)}
                data_payload = {'chat_id': self.chat_id}
                response = requests.post(file_url, data=data_payload, files=files)
            
            # Удаляем временный файл
            os.remove(filename)
            
            if response.status_code == 200:
                logger.info("Файл успешно отправлен в Telegram")
                return True
            else:
                logger.error(f"Ошибка отправки файла: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при отправке файла: {e}")
            return False

    def migrate_data_to_bot(self, query, message="Миграция данных", send_as_file=False):
        # Отправка в ТГ
        try:
            # Получение данных из PostgreSQL
            data = self.get_data_from_postgres(query)
            if not data:
                logger.warning("Нет данных для отправки")
                self.send_to_telegram(f"❌ {message}: нет данных для отправки")
                return False
            
            logger.info(f"Обработано {len(data)} записей")
            
            if send_as_file or len(str(data)) > 3000:
                # Отправка как файла для больших данных
                return self.send_data_as_file(data, "database_export.json", message)
            else:
                # Отправка как читаемого сообщения
                formatted_data = self.format_data_for_telegram(data)
                full_message = f"✅ {message}\n\n{formatted_data}"
                return self.send_to_telegram(full_message)
            
        except Exception as e:
            error_msg = f"❌ Ошибка при миграции данных: {str(e)}"
            logger.error(error_msg)
            self.send_to_telegram(error_msg)
            return False


# =======================================================
if __name__ == "__main__":
    # Получаем SQL запрос из .env
    SQL_QUERY = os.getenv('SQL_QUERY')
    
    if not SQL_QUERY:
        logger.error("SQL_QUERY не указан в .env файле")
        exit(1)
    
    try:
        # Инициализация класса
        bot_sender = BotDataSender()
        
        # Миграция данных в бот
        success = bot_sender.migrate_data_to_bot(
            query=SQL_QUERY,
            message="Статистика из базы данных",
            send_as_file=False  # True для отправки файлом
        )
        
        if success:
            print("✅ Данные успешно отправлены в Telegram!")
        else:
            print("❌ Произошла ошибка при отправке данных.")
            
    except ValueError as e:
        logger.error(f"Ошибка конфигурации: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")