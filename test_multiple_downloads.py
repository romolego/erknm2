"""Тестовый скрипт для проверки скачивания нескольких мета-XML файлов подряд"""
from pathlib import Path
from playwright.sync_api import sync_playwright
import time
import random

TEST_URLS = [
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-1.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-2.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-3.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-4.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-5.xml",
]

def download_file(url, output_path, delay=0):
    """Скачать один файл"""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            if delay > 0:
                time.sleep(delay)
            
            response = page.goto(url, wait_until="networkidle", timeout=60000)
            
            if not response or response.status >= 400:
                browser.close()
                return False, f"HTTP {response.status if response else 'no response'}"
            
            time.sleep(random.uniform(1.0, 2.0))
            body = response.body()
            
            if not body or len(body) == 0:
                browser.close()
                return False, "Empty response"
            
            if not (b'<?xml' in body[:100] or b'<meta' in body[:200]):
                browser.close()
                return False, "Not valid XML"
            
            output_path.write_bytes(body)
            browser.close()
            return True, f"{len(body)} bytes"
            
    except Exception as e:
        return False, str(e)

def test_batch_download(delay_between=5):
    """Тест скачивания нескольких файлов с задержкой"""
    print(f"\n{'='*60}")
    print(f"Тест скачивания {len(TEST_URLS)} файлов с задержкой {delay_between}с")
    print(f"{'='*60}\n")
    
    results = []
    output_dir = Path("test_downloads")
    output_dir.mkdir(exist_ok=True)
    
    for i, url in enumerate(TEST_URLS, 1):
        filename = url.split('/')[-1]
        output_path = output_dir / filename
        
        print(f"[{i}/{len(TEST_URLS)}] Скачивание {filename}...", end=" ")
        
        success, message = download_file(url, output_path, delay=0)
        
        if success:
            print(f"[OK] {message}")
            results.append(True)
        else:
            print(f"[ERROR] {message}")
            results.append(False)
        
        # Задержка между файлами (кроме последнего)
        if i < len(TEST_URLS):
            print(f"  Пауза {delay_between}с...")
            time.sleep(delay_between)
    
    success_count = sum(results)
    print(f"\n{'='*60}")
    print(f"Результат: {success_count}/{len(TEST_URLS)} успешно")
    print(f"{'='*60}\n")
    
    return success_count == len(TEST_URLS)

def test_with_reused_browser(delay_between=5):
    """Тест с переиспользованием браузера"""
    print(f"\n{'='*60}")
    print(f"Тест с переиспользованием браузера, задержка {delay_between}с")
    print(f"{'='*60}\n")
    
    results = []
    output_dir = Path("test_downloads_reused")
    output_dir.mkdir(exist_ok=True)
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            
            for i, url in enumerate(TEST_URLS, 1):
                filename = url.split('/')[-1]
                output_path = output_dir / filename
                
                print(f"[{i}/{len(TEST_URLS)}] Скачивание {filename}...", end=" ")
                
                try:
                    page = context.new_page()
                    response = page.goto(url, wait_until="networkidle", timeout=60000)
                    
                    if not response or response.status >= 400:
                        print(f"[ERROR] HTTP {response.status if response else 'no response'}")
                        results.append(False)
                        page.close()
                        continue
                    
                    time.sleep(random.uniform(1.0, 2.0))
                    body = response.body()
                    
                    if not body or len(body) == 0:
                        print(f"[ERROR] Empty response")
                        results.append(False)
                        page.close()
                        continue
                    
                    if not (b'<?xml' in body[:100] or b'<meta' in body[:200]):
                        print(f"[ERROR] Not valid XML")
                        results.append(False)
                        page.close()
                        continue
                    
                    output_path.write_bytes(body)
                    print(f"[OK] {len(body)} bytes")
                    results.append(True)
                    page.close()
                    
                except Exception as e:
                    print(f"[ERROR] {str(e)}")
                    results.append(False)
                
                # Задержка между файлами (кроме последнего)
                if i < len(TEST_URLS):
                    print(f"  Пауза {delay_between}с...")
                    time.sleep(delay_between)
            
            browser.close()
    
    except Exception as e:
        print(f"[ERROR] Browser error: {str(e)}")
        return False
    
    success_count = sum(results)
    print(f"\n{'='*60}")
    print(f"Результат: {success_count}/{len(TEST_URLS)} успешно")
    print(f"{'='*60}\n")
    
    return success_count == len(TEST_URLS)

if __name__ == '__main__':
    print("=" * 60)
    print("Тестирование скачивания нескольких файлов подряд")
    print("=" * 60)
    
    # Тест 1: С задержкой 5 секунд, новый браузер для каждого файла
    print("\n>>> Тест 1: Новый браузер для каждого файла, задержка 5с")
    result1 = test_batch_download(delay_between=5)
    
    time.sleep(10)  # Пауза между тестами
    
    # Тест 2: С задержкой 10 секунд
    print("\n>>> Тест 2: Новый браузер для каждого файла, задержка 10с")
    result2 = test_batch_download(delay_between=10)
    
    time.sleep(10)  # Пауза между тестами
    
    # Тест 3: С переиспользованием браузера, задержка 5с
    print("\n>>> Тест 3: Переиспользование браузера, задержка 5с")
    result3 = test_with_reused_browser(delay_between=5)
    
    time.sleep(10)  # Пауза между тестами
    
    # Тест 4: С переиспользованием браузера, задержка 10с
    print("\n>>> Тест 4: Переиспользование браузера, задержка 10с")
    result4 = test_with_reused_browser(delay_between=10)
    
    print("\n" + "=" * 60)
    print("ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
    print("=" * 60)
    print(f"Тест 1 (новый браузер, 5с): {'[OK]' if result1 else '[FAILED]'}")
    print(f"Тест 2 (новый браузер, 10с): {'[OK]' if result2 else '[FAILED]'}")
    print(f"Тест 3 (переиспользование, 5с): {'[OK]' if result3 else '[FAILED]'}")
    print(f"Тест 4 (переиспользование, 10с): {'[OK]' if result4 else '[FAILED]'}")
    print("=" * 60)








