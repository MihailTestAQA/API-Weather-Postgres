import os  # Импорт модуля для работы с операционной системой
from dotenv import load_dotenv  # Импорт функции для загрузки переменных из .env файла
import requests  # Импорт библиотеки для выполнения HTTP-запросов
import time  # Импорт модуля для работы со временем (паузы)
from datetime import datetime  # Импорт класса для работы с датой и временем

# Загружаем переменные из .env файла
load_dotenv()  # Загружает все переменные из файла .env в окружение

class WeatherAPI:  # Объявление класса для работы с API погоды
    def __init__(self):  # Конструктор класса (вызывается при создании объекта)
        # Берем API ключ из .env
        self.api_key = os.getenv('OPENWEATHER_API_KEY')  # Получаем API ключ из переменной окружения
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"  # Базовый URL API погоды
        
        # Проверяем загрузку API ключа
        if not self.api_key:  # Если ключ не найден или пустой
            print("API ключ не найден в .env файле")  # Выводим сообщение об ошибке
        else:  # Если ключ найден
            print(f"API ключ загружен (длина: {len(self.api_key)} символов)")  # Выводим информацию о ключе

    def get_data_weather(self, target_datetime, city="Moscow"):  # Метод для получения данных о погоде
        """Получение погодных данных"""  # Строка документации метода
        if not self.api_key:  # Проверяем наличие API ключа
            print("API ключ погоды не найден")  # Сообщение об ошибке
            return None  # Возвращаем None если ключа нет
        
        try:  # Начало блока обработки исключений
            # Формируем URL запроса с параметрами
            url = f"{self.base_url}?q={city}&appid={self.api_key}&units=metric&lang=ru"
            
            print(f"Запрос к API погоды для города: {city}")  # Информация о запросе
            
            response = requests.get(url, timeout=10)  # Выполняем GET-запрос с таймаутом 10 секунд
            
            print(f"Статус ответа: {response.status_code}")  # Выводим статус HTTP-ответа
            
            if response.status_code == 200:  # Если запрос успешен (статус 200 OK)
                data = response.json()  # Парсим JSON-ответ в словарь Python
                
                # Проверяем структуру данных
                if not all(key in data for key in ['main', 'weather', 'wind']):  # Проверяем наличие обязательных ключей
                    print("Неверная структура данных API")  # Сообщение об ошибке структуры
                    return None  # Возвращаем None при неверной структуре
                
                # Создаем словарь с погодными данными
                weather_info = {
                    'temperature': data['main']['temp'],  # Температура
                    'pressure': self.pa_to_mmhg(data['main']['pressure']),  # Давление (конвертированное)
                    'humidity': data['main']['humidity'],  # Влажность
                    'wind_speed': data['wind'].get('speed', 0),  # Скорость ветра (значение по умолчанию 0)
                    'wind_direction': self.get_wind_direction(data['wind'].get('deg')),  # Направление ветра
                    'description': data['weather'][0]['description'],  # Текстовое описание погоды
                    'precipitation': self.get_precipitation_type(data),  # Тип осадков
                    'observation_time': datetime.now(),  # Время получения данных
                    'city': data.get('name', city)  # Название города (из ответа или переданное)
                }
                
                # Вывод данных в консоль
                self.print_weather_info(weather_info)  # Вызываем метод для красивого вывода
                return weather_info  # Возвращаем словарь с погодными данными
            else:  # Если HTTP-статус не 200
                print(f"Ошибка API: {response.status_code}")  # Выводим код ошибки
                if response.status_code == 401:  # Если ошибка авторизации
                    print("Неверный API ключ. Проверьте WEATHER_API_KEY в .env файле")  # Сообщение об ошибке ключа
                elif response.status_code == 404:  # Если город не найден
                    print("Город не найден. Попробуйте другое название")  # Сообщение об ошибке города
                else:  # Для других ошибок
                    print(f"Ответ сервера: {response.text}")  # Выводим текст ответа сервера
                return None  # Возвращаем None при ошибке
                
        except requests.exceptions.RequestException as e:  # Обработка ошибок сети
            print(f"Ошибка сети: {e}")  # Выводим сообщение об ошибке сети
            return None  # Возвращаем None
        except Exception as e:  # Обработка всех остальных исключений
            print(f"Неожиданная ошибка: {e}")  # Выводим сообщение об ошибке
            return None  # Возвращаем None

    def print_weather_info(self, weather_info):  # Метод для вывода погодных данных
        """Красивый вывод погодных данных в консоль"""  # Строка документации
        print("Погода")  # Заголовок
        print(f"Город: {weather_info['city']}")  # Вывод названия города
        print(f"Время наблюдения: {weather_info['observation_time'].strftime('%Y-%m-%d %H:%M:%S')}")  # Время в формате
        print(f"Температура: {weather_info['temperature']}°C")  # Температура
        print(f"Давление: {weather_info['pressure']} мм рт.ст.")  # Давление
        print(f"Влажность: {weather_info['humidity']}%")  # Влажность
        print(f"Ветер: {weather_info['wind_speed']} м/с, {weather_info['wind_direction']}")  # Ветер
        print(f"Описание: {weather_info['description']}")  # Описание погоды
        print(f"Осадки: {weather_info['precipitation']}")  # Тип осадков

    def pa_to_mmhg(self, pressure_pa):  # Метод конвертации давления
        """Конвертация давления из Па в мм рт.ст."""  # Строка документации
        return round(pressure_pa * 0.750062)  # Возвращаем округленное значение после конвертации

    def get_wind_direction(self, degrees):  # Метод определения направления ветра
        """Определение направления ветра по градусам"""  # Строка документации
        if degrees is None:  # Если градусы не указаны
            return 'неизвестно'  # Возвращаем "неизвестно"
        
        # Список направлений ветра
        directions = ['северный', 'северо-восточный', 'восточный', 'юго-восточный', 
                     'южный', 'юго-западный', 'западный', 'северо-западный']
        index = round(degrees / 45) % 8  # Вычисляем индекс направления (0-7)
        return directions[index]  # Возвращаем направление по индексу

    def get_precipitation_type(self, weather_data):  # Метод определения типа осадков
        """Определение типа осадков по ID погоды"""  # Строка документации
        weather_id = weather_data['weather'][0]['id']  # Получаем ID погоды из данных
        
        # Определяем тип осадков по диапазонам ID
        if 200 <= weather_id <= 232:  # Гроза
            return 'гроза'
        elif 300 <= weather_id <= 321:  # Мелкий дождь
            return 'мелкий дождь'
        elif 500 <= weather_id <= 531:  # Дождь
            return 'дождь'
        elif 600 <= weather_id <= 622:  # Снег
            return 'снег'
        elif 701 <= weather_id <= 781:  # Туман и другие явления
            return 'туман'
        else:  # Для всех остальных случаев
            return 'перем облачность'  # Переменная облачность


# Пример использования
if __name__ == "__main__":  # Проверяем, запущен ли файл напрямую
    weather_api = WeatherAPI()  # Создаем объект класса WeatherAPI
    
    #print("Тестовый запрос погодных данных...")
    test_data = weather_api.get_weather_data(datetime.now(), "Moscow")  # Получаем данные о погоде в Москве