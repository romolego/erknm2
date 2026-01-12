"""Тестовый скрипт для проверки скачивания мета-XML файлов"""
from pathlib import Path
from playwright.sync_api import sync_playwright
import time
import random

TEST_URL = "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-1.xml"
OUTPUT_FILE = Path("test_meta_download.xml")

def test_method_1_direct_goto():
    """Метод 1: Прямой переход через page.goto()"""
    print("\n=== Метод 1: Прямой переход через page.goto() ===")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            print(f"Переход на URL: {TEST_URL}")
            response = page.goto(TEST_URL, wait_until="networkidle", timeout=60000)
            
            if not response:
                print("[ERROR] Не удалось получить ответ")
                browser.close()
                return False
            
            print(f"Статус ответа: {response.status}")
            
            if response.status >= 400:
                print(f"[ERROR] HTTP ошибка: {response.status}")
                browser.close()
                return False
            
            # Небольшая задержка
            time.sleep(random.uniform(1.0, 2.0))
            
            # Получаем тело ответа
            body = response.body()
            print(f"Размер ответа: {len(body)} байт")
            
            if not body or len(body) == 0:
                print("[ERROR] Пустой ответ")
                browser.close()
                return False
            
            # Проверяем, что это XML
            if b'<?xml' in body[:100] or b'<meta' in body[:200]:
                OUTPUT_FILE.write_bytes(body)
                print(f"[OK] Файл успешно сохранен: {OUTPUT_FILE} ({len(body)} байт)")
                browser.close()
                return True
            else:
                print("[ERROR] Ответ не является XML")
                print(f"Первые 200 байт: {body[:200]}")
                browser.close()
                return False
                
    except Exception as e:
        print(f"[ERROR] Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_method_2_with_headers():
    """Метод 2: С дополнительными заголовками"""
    print("\n=== Метод 2: С дополнительными заголовками ===")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept': 'application/xml, text/xml, */*',
                    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Referer': 'https://proverki.gov.ru/portal/public-open-data'
                }
            )
            page = context.new_page()
            
            print(f"Переход на URL: {TEST_URL}")
            response = page.goto(TEST_URL, wait_until="networkidle", timeout=60000)
            
            if not response or response.status >= 400:
                print(f"[ERROR] Ошибка: статус {response.status if response else 'no response'}")
                browser.close()
                return False
            
            time.sleep(random.uniform(1.0, 2.0))
            body = response.body()
            
            if body and len(body) > 0 and (b'<?xml' in body[:100] or b'<meta' in body[:200]):
                OUTPUT_FILE.write_bytes(body)
                print(f"[OK] Файл успешно сохранен: {OUTPUT_FILE} ({len(body)} байт)")
                browser.close()
                return True
            else:
                print("[ERROR] Не удалось получить валидный XML")
                browser.close()
                return False
                
    except Exception as e:
        print(f"[ERROR] Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_method_3_request_interception():
    """Метод 3: С перехватом запроса"""
    print("\n=== Метод 3: С перехватом запроса ===")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            # Перехватываем ответ
            response_body = None
            
            def handle_response(response):
                nonlocal response_body
                if response.url == TEST_URL:
                    response_body = response.body()
            
            page.on("response", handle_response)
            
            print(f"Переход на URL: {TEST_URL}")
            response = page.goto(TEST_URL, wait_until="networkidle", timeout=60000)
            
            if not response or response.status >= 400:
                print(f"[ERROR] Ошибка: статус {response.status if response else 'no response'}")
                browser.close()
                return False
            
            time.sleep(2.0)
            
            # Используем перехваченный body или response.body()
            body = response_body if response_body else response.body()
            
            if body and len(body) > 0 and (b'<?xml' in body[:100] or b'<meta' in body[:200]):
                OUTPUT_FILE.write_bytes(body)
                print(f"[OK] Файл успешно сохранен: {OUTPUT_FILE} ({len(body)} байт)")
                browser.close()
                return True
            else:
                print("[ERROR] Не удалось получить валидный XML")
                browser.close()
                return False
                
    except Exception as e:
        print(f"[ERROR] Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_method_4_wait_for_content():
    """Метод 4: С ожиданием контента"""
    print("\n=== Метод 4: С ожиданием контента ===")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            print(f"Переход на URL: {TEST_URL}")
            response = page.goto(TEST_URL, wait_until="domcontentloaded", timeout=60000)
            
            if not response or response.status >= 400:
                print(f"[ERROR] Ошибка: статус {response.status if response else 'no response'}")
                browser.close()
                return False
            
            # Ждем загрузки контента
            try:
                page.wait_for_selector('body', timeout=10000)
            except:
                pass  # Игнорируем, если не найден body
            
            time.sleep(2.0)
            
            # Пробуем получить через content
            content = page.content()
            if content and ('<?xml' in content or '<meta' in content):
                # Извлекаем XML из content
                body = content.encode('utf-8')
                OUTPUT_FILE.write_bytes(body)
                print(f"[OK] Файл успешно сохранен через content: {OUTPUT_FILE} ({len(body)} байт)")
                browser.close()
                return True
            
            # Если не получилось через content, пробуем response.body()
            body = response.body()
            if body and len(body) > 0 and (b'<?xml' in body[:100] or b'<meta' in body[:200]):
                OUTPUT_FILE.write_bytes(body)
                print(f"[OK] Файл успешно сохранен через response.body(): {OUTPUT_FILE} ({len(body)} байт)")
                browser.close()
                return True
            else:
                print("[ERROR] Не удалось получить валидный XML")
                print(f"Content length: {len(content) if content else 0}")
                print(f"Body length: {len(body) if body else 0}")
                browser.close()
                return False
                
    except Exception as e:
        print(f"[ERROR] Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    print("=" * 60)
    print("Тестирование методов скачивания мета-XML файлов")
    print("=" * 60)
    print(f"URL: {TEST_URL}")
    print(f"Выходной файл: {OUTPUT_FILE}")
    print("=" * 60)
    
    methods = [
        test_method_1_direct_goto,
        test_method_2_with_headers,
        test_method_3_request_interception,
        test_method_4_wait_for_content
    ]
    
    for method in methods:
        try:
            if method():
                print(f"\n[OK] Успешно! Метод {method.__name__} работает")
                break
        except Exception as e:
            print(f"\n[ERROR] Метод {method.__name__} завершился с ошибкой: {str(e)}")
            continue
    
    # Проверяем результат
    if OUTPUT_FILE.exists():
        print(f"\n[OK] Файл сохранен: {OUTPUT_FILE}")
        print(f"Размер: {OUTPUT_FILE.stat().st_size} байт")
        
        # Проверяем содержимое
        content = OUTPUT_FILE.read_bytes()
        if b'<?xml' in content[:100] and b'<meta' in content[:200]:
            print("[OK] Файл содержит валидный XML")
            # Показываем первые строки
            try:
                text = content[:500].decode('utf-8', errors='ignore')
                print(f"\nПервые 500 символов:\n{text}")
            except:
                pass
        else:
            print("[WARNING] Файл может быть поврежден")
    else:
        print("\n[ERROR] Файл не был сохранен")

