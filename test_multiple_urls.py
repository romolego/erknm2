"""Тестирование скачивания нескольких мета-XML файлов подряд"""
from pathlib import Path
from playwright.sync_api import sync_playwright
import time
import random

# Тестовые URL из списка
TEST_URLS = [
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-1.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-2.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-3.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-4.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-5.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-6.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-7.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-8.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-9.xml",
    "https://proverki.gov.ru/blob/opendata/7710146102-inspection-2021-10.xml",
]

OUTPUT_DIR = Path("test_downloads")
OUTPUT_DIR.mkdir(exist_ok=True)

def download_single_file(url, output_path, delay_before=0, delay_after=0):
    """Скачать один файл"""
    try:
        print(f"\n[{time.strftime('%H:%M:%S')}] Скачивание: {url}")
        
        if delay_before > 0:
            print(f"  Задержка перед запросом: {delay_before:.1f}с")
            time.sleep(delay_before)
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            start_time = time.time()
            response = page.goto(url, wait_until="networkidle", timeout=60000)
            elapsed = time.time() - start_time
            
            if not response:
                print(f"  [ERROR] Не удалось получить ответ")
                browser.close()
                return False
            
            if response.status >= 400:
                print(f"  [ERROR] HTTP {response.status}: {response.status_text}")
                browser.close()
                return False
            
            # Задержка после получения ответа
            time.sleep(random.uniform(1.0, 2.0))
            
            body = response.body()
            
            if not body or len(body) == 0:
                print(f"  [ERROR] Пустой ответ")
                browser.close()
                return False
            
            # Проверка XML
            if not (b'<?xml' in body[:100] or b'<meta' in body[:200] or b'<dataset' in body[:200]):
                print(f"  [ERROR] Не валидный XML")
                browser.close()
                return False
            
            output_path.write_bytes(body)
            file_size = output_path.stat().st_size
            
            browser.close()
            
            print(f"  [OK] Успешно: {file_size} байт за {elapsed:.1f}с")
            
            if delay_after > 0:
                print(f"  Задержка после запроса: {delay_after:.1f}с")
                time.sleep(delay_after)
            
            return True
            
    except Exception as e:
        print(f"  [ERROR] Исключение: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_strategy_1_no_delay():
    """Стратегия 1: Без задержек"""
    print("\n" + "="*60)
    print("СТРАТЕГИЯ 1: Без задержек между запросами")
    print("="*60)
    
    success_count = 0
    for i, url in enumerate(TEST_URLS[:5], 1):  # Тестируем первые 5
        output_path = OUTPUT_DIR / f"test1_{i}.xml"
        if download_single_file(url, output_path, delay_before=0, delay_after=0):
            success_count += 1
    
    print(f"\nРезультат: {success_count}/5 успешно")
    return success_count

def test_strategy_2_short_delay():
    """Стратегия 2: Короткие задержки (3-5 секунд)"""
    print("\n" + "="*60)
    print("СТРАТЕГИЯ 2: Задержки 3-5 секунд между запросами")
    print("="*60)
    
    success_count = 0
    for i, url in enumerate(TEST_URLS[:5], 1):
        output_path = OUTPUT_DIR / f"test2_{i}.xml"
        delay = random.uniform(3, 5)
        if download_single_file(url, output_path, delay_before=0, delay_after=delay):
            success_count += 1
    
    print(f"\nРезультат: {success_count}/5 успешно")
    return success_count

def test_strategy_3_medium_delay():
    """Стратегия 3: Средние задержки (5-10 секунд)"""
    print("\n" + "="*60)
    print("СТРАТЕГИЯ 3: Задержки 5-10 секунд между запросами")
    print("="*60)
    
    success_count = 0
    for i, url in enumerate(TEST_URLS[:5], 1):
        output_path = OUTPUT_DIR / f"test3_{i}.xml"
        delay = random.uniform(5, 10)
        if download_single_file(url, output_path, delay_before=0, delay_after=delay):
            success_count += 1
    
    print(f"\nРезультат: {success_count}/5 успешно")
    return success_count

def test_strategy_4_long_delay():
    """Стратегия 4: Длинные задержки (10-15 секунд)"""
    print("\n" + "="*60)
    print("СТРАТЕГИЯ 4: Задержки 10-15 секунд между запросами")
    print("="*60)
    
    success_count = 0
    for i, url in enumerate(TEST_URLS[:5], 1):
        output_path = OUTPUT_DIR / f"test4_{i}.xml"
        delay = random.uniform(10, 15)
        if download_single_file(url, output_path, delay_before=0, delay_after=delay):
            success_count += 1
    
    print(f"\nРезультат: {success_count}/5 успешно")
    return success_count

def test_strategy_5_batch_with_pause():
    """Стратегия 5: Порции по 2 файла с паузой 30 секунд"""
    print("\n" + "="*60)
    print("СТРАТЕГИЯ 5: Порции по 2 файла, пауза 30 секунд")
    print("="*60)
    
    success_count = 0
    batch_size = 2
    pause_between_batches = 30
    
    for batch_start in range(0, min(6, len(TEST_URLS)), batch_size):
        batch_end = min(batch_start + batch_size, len(TEST_URLS))
        print(f"\n--- Порция {batch_start + 1}-{batch_end} ---")
        
        for i in range(batch_start, batch_end):
            url = TEST_URLS[i]
            output_path = OUTPUT_DIR / f"test5_{i+1}.xml"
            if download_single_file(url, output_path, delay_before=0, delay_after=random.uniform(2, 4)):
                success_count += 1
        
        if batch_end < len(TEST_URLS):
            print(f"\nПауза {pause_between_batches}с перед следующей порцией...")
            time.sleep(pause_between_batches)
    
    print(f"\nРезультат: {success_count}/{min(6, len(TEST_URLS))} успешно")
    return success_count

if __name__ == '__main__':
    print("="*60)
    print("ТЕСТИРОВАНИЕ СКАЧИВАНИЯ МНОЖЕСТВЕННЫХ МЕТА-XML ФАЙЛОВ")
    print("="*60)
    print(f"Тестируем {len(TEST_URLS)} URL")
    print(f"Выходная директория: {OUTPUT_DIR}")
    print("="*60)
    
    results = {}
    
    # Тестируем разные стратегии
    try:
        results['strategy_1'] = test_strategy_1_no_delay()
        time.sleep(10)  # Пауза между стратегиями
        
        results['strategy_2'] = test_strategy_2_short_delay()
        time.sleep(10)
        
        results['strategy_3'] = test_strategy_3_medium_delay()
        time.sleep(10)
        
        results['strategy_4'] = test_strategy_4_long_delay()
        time.sleep(10)
        
        results['strategy_5'] = test_strategy_5_batch_with_pause()
    except KeyboardInterrupt:
        print("\n\nТестирование прервано пользователем")
    except Exception as e:
        print(f"\n\nКритическая ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # Итоговый отчет
    print("\n" + "="*60)
    print("ИТОГОВЫЕ РЕЗУЛЬТАТЫ")
    print("="*60)
    for strategy, count in results.items():
        print(f"{strategy}: {count}/5 успешно")
    
    best_strategy = max(results.items(), key=lambda x: x[1])
    print(f"\nЛучшая стратегия: {best_strategy[0]} ({best_strategy[1]}/5)")

