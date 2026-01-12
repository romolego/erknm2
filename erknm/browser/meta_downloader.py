"""Загрузка мета-XML файлов через браузер"""
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import time
import random
from erknm.db.models import OperationLog


def download_meta_xml_browser(url: str, output_path: Path, sync_run_id=None, max_retries=5, delay=10.0, timeout=60000):
    """
    Скачать мета-XML файл через браузерную автоматизацию с имитацией человеческого поведения
    
    Проверенная логика из тестов: прямой переход через page.goto() с задержками 10+ секунд
    
    Args:
        url: URL мета-XML файла
        output_path: Путь для сохранения файла
        sync_run_id: ID запуска синхронизации для логирования (опционально)
        max_retries: Максимальное количество попыток
        delay: Базовая задержка между попытками в секундах (10 секунд - проверено в тестах)
        timeout: Таймаут в миллисекундах
    
    Returns:
        Path к скачанному файлу
    
    Raises:
        Exception: Если не удалось скачать файл после всех попыток
    """
    # Проверяем, не скачан ли уже файл
    if output_path.exists():
        if sync_run_id:
            OperationLog.log(sync_run_id, "dataset", f"Файл уже существует: {output_path.name}", stage='dataset')
        return output_path
    
    # Создаем директорию если нужно
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Задержка между попытками (кроме первой)
            if attempt > 0:
                # Экспоненциальная задержка с jitter
                wait_time = delay * (2 ** attempt) + random.uniform(0, 2)
                if sync_run_id:
                    OperationLog.log(sync_run_id, "dataset", 
                                   f"Повторная попытка {attempt + 1}/{max_retries} через {wait_time:.1f}с", stage='dataset')
                time.sleep(wait_time)
            
            if sync_run_id:
                OperationLog.log(sync_run_id, "dataset", f"Открытие страницы {url} через Playwright (попытка {attempt + 1}/{max_retries})", stage='dataset')
            
            # Проверенная логика из тестов: прямой переход через page.goto()
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    # Имитируем реальный браузер
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    viewport={'width': 1920, 'height': 1080}
                )
                page = context.new_page()
                
                try:
                    # Прямой переход на URL (проверенный метод)
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "dataset", f"Переход на URL: {url}", stage='dataset')
                    
                    response = page.goto(url, wait_until="networkidle", timeout=timeout)
                    
                    if not response:
                        raise Exception("Не удалось получить ответ от сервера")
                    
                    # Проверяем статус ответа
                    if response.status >= 400:
                        raise Exception(f"HTTP ошибка {response.status}: {response.status_text}")
                    
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "dataset", f"Получен ответ со статусом {response.status}", stage='dataset')
                    
                    # Небольшая задержка для имитации человеческого поведения
                    time.sleep(random.uniform(1.0, 2.5))
                    
                    # Получаем тело ответа напрямую (проверенный метод)
                    body = response.body()
                    
                    if not body or len(body) == 0:
                        raise Exception("Получен пустой ответ от сервера")
                    
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "dataset", f"Получено {len(body)} байт данных", stage='dataset')
                    
                    # Проверяем, что это действительно XML
                    if not (b'<?xml' in body[:100] or b'<meta' in body[:200] or b'<dataset' in body[:200]):
                        raise Exception("Полученный ответ не является валидным XML")
                    
                    # Сохраняем файл
                    output_path.write_bytes(body)
                    
                    # Проверяем, что файл создан и не пустой
                    if not output_path.exists() or output_path.stat().st_size == 0:
                        raise Exception("Файл не был создан или пуст")
                    
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "dataset", 
                                       f"Файл {output_path.name} успешно скачан ({output_path.stat().st_size} байт)", stage='dataset')
                    
                    browser.close()
                    
                    # КРИТИЧЕСКИ ВАЖНО: Задержка перед следующим запросом
                    # Тесты показали, что задержка 10 секунд работает надежно
                    wait_time = delay + random.uniform(3, 7)  # 10-17 секунд
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "dataset", 
                                       f"Задержка {wait_time:.1f}с перед следующим запросом", stage='dataset')
                    time.sleep(wait_time)
                    
                    return output_path
                    
                except PlaywrightTimeoutError as e:
                    browser.close()
                    last_error = f"Таймаут при загрузке страницы: {str(e)}"
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "dataset", last_error, level="WARNING", stage='dataset')
                    if attempt == max_retries - 1:
                        raise Exception(last_error)
                    continue
                    
                except Exception as e:
                    browser.close()
                    error_str = str(e)
                    # Проверяем, является ли это ошибкой соединения
                    is_connection_error = any(keyword in error_str.lower() for keyword in [
                        'connection', 'reset', 'aborted', 'closed', 'timeout', 'network'
                    ])
                    
                    if is_connection_error:
                        last_error = f"Ошибка соединения при скачивании: {error_str}"
                    else:
                        last_error = f"Ошибка при скачивании: {error_str}"
                    
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "dataset", last_error, level="WARNING", stage='dataset')
                    
                    # Для ошибок соединения делаем более длинную задержку перед повтором
                    if is_connection_error and attempt < max_retries - 1:
                        extra_wait = delay * (2 ** attempt) + random.uniform(2, 5)
                        if sync_run_id:
                            OperationLog.log(sync_run_id, "dataset", 
                                           f"Дополнительная задержка {extra_wait:.1f}с из-за ошибки соединения", stage='dataset')
                        time.sleep(extra_wait)
                    
                    if attempt == max_retries - 1:
                        raise Exception(last_error)
                    continue
                    
        except Exception as e:
            last_error = str(e)
            if sync_run_id:
                OperationLog.log(sync_run_id, "dataset", 
                               f"Ошибка попытки {attempt + 1}/{max_retries}: {last_error}", 
                               level="WARNING", stage='dataset')
            if attempt == max_retries - 1:
                raise Exception(f"Не удалось скачать {url} после {max_retries} попыток: {last_error}")
            continue
    
    raise Exception(f"Не удалось скачать {url} после {max_retries} попыток: {last_error}")
