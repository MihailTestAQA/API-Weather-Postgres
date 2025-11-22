import requests
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    
    print("инструменты:")
    print("1. Откройте Telegram на телефоне")
    print("2. Найдите вашего бота: https://t.me/ИМЯ_ВАШЕГО_БОТА")
    print("3. Нажмите 'START' или отправьте ЛЮБОЕ сообщение")
    print("4. Затем нажмите Enter здесь...")
    
    input()  # Ждем 
    
    # Получаем обновления
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        if data['result']:
            latest = data['result'][-1]
            chat_id = latest['message']['chat']['id']
            first_name = latest['message']['chat'].get('first_name', '')
            
            print(f"Отлично! Найден Chat ID: {chat_id}")
            print(f"Пользователь: {first_name}")
            print(f"\nДобавьте в .env файл:")
            print(f"TELEGRAM_CHAT_ID={chat_id}")
            
            # Тестовое сообщение
            send_url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                'chat_id': chat_id,
                'text': 'бот работает! Это тестовое сообщение из Python скрипта.'
            }
            requests.post(send_url, json=payload)
            print("Отправлено тестовое сообщение в Telegram")
            
        else:
            print("Сообщений не найдено. Убедитесь, что отправили сообщение боту")
    else:
        print("Ошибка при получении данных")

if __name__ == "__main__":
    main()