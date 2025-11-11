# Импорт необходимых библиотек Selenium
from selenium import webdriver  # Основной модуль для автоматизации браузера
from selenium.webdriver.chrome.options import Options  # Для настройки опций Chrome браузера

# Определение функции парсера
def parser():
    print("Лотерея 4x20 парсер")
    
    # Создание объекта для настройки опций Chrome браузера
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # Добавление аргумента для запуска браузера в headless режиме (без графического интерфейса)
    chrome_options.add_argument("--remote-debugging-port=9222") # Добавление аргумента для удаленной отладки на порту 9222
    chrome_options.add_argument("--window-size=1920,1080")# Установка размера окна браузера для headless режима
    
    # Создание экземпляра драйвера Chrome с указанными настройками
    driver = webdriver.Chrome(options=chrome_options)
    
    # Начало блока обработки исключений
    try:
        # Вывод сообщения о начале загрузки страницы
        print("Загружаем страницу...")
        # Открытие целевой веб-страницы в браузере
        driver.get("https://www.lotonews.ru/draws/archive/4x20") #Страница для браузера (архив за 10+, тиражей)
        
        # Поиск элемента таблицы на странице по тегу table
        table = driver.find_element("tag name", "table")
        # Поиск всех строк в таблице по тегу tr
        rows = table.find_elements("tag name", "tr")
        
        print(f" Найдено строк: {len(rows)}") # Вывод количества найденных строк в таблице
        
        # Цикл по строкам таблицы, начиная с третьей строки (индекс 2)
        for i in range(2, len(rows)):
            # Поиск всех ячеек в текущей строке по тегу td
            cells = rows[i].find_elements("tag name", "td")
            # Проверка что в строке есть как минимум 4 ячейки
            if len(cells) >= 4:
                # Получение текста из первой ячейки (дата тиража)
                date = cells[0].text
                # Получение текста из второй ячейки (номер тиража)
                draw = cells[1].text
                # Извлечение чисел из третьей ячейки: разбиваем текст по переносам строк и оставляем только цифры
                numbers = [num for num in cells[2].text.split('\n') if num.isdigit()]
                
                # Проверка что найдено как минимум 8 чисел
                if len(numbers) >= 8:
                    numbers_str = ", ".join(numbers[:8])  # Объединение первых 8 чисел в строку через запятую
                    print(f" {date} |  {draw} |  {numbers_str}") # Вывод информации о тираже в консоль
                                 
    # Обработка исключений которые могут возникнуть при работе парсера
    except Exception as e:
        print(f"Ошибка: {e}", "Скриншот ошибки в error.png")  # Вывод сообщения об ошибке
        driver.save_screenshot("error.png") # Сохранение скриншота страницы в файл для отладки
        
    # Блок finally выполняется всегда, даже при возникновении ошибки
    finally:
        # Закрытие браузера и освобождение ресурсов
        driver.quit()

# Проверка что скрипт запущен напрямую, а не импортирован как модуль
if __name__ == "__main__":
    parser()