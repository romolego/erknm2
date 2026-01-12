"""Загрузка list.xml через браузер"""
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import requests
import time
from erknm.config import SOURCE_URL, DOWNLOAD_DIR
from erknm.db.models import OperationLog


def download_list_xml(sync_run_id=None, timeout=30000):
    """
    Скачать list.xml через браузерную автоматизацию
    
    Args:
        sync_run_id: ID запуска синхронизации для логирования
        timeout: Таймаут в миллисекундах
    
    Returns:
        Path к скачанному файлу или None при ошибке
    """
    output_path = DOWNLOAD_DIR / "list.xml"
    
    browser = None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            
            # Сначала пробуем получить list.xml напрямую - пробуем несколько вариантов URL
            list_xml_urls = [
                f"{SOURCE_URL.rstrip('/')}/list.xml",
                "https://proverki.gov.ru/portal/public-open-data/list.xml",
                "https://proverki.gov.ru/list.xml",
                "https://proverki.gov.ru/portal/list.xml"
            ]
            
            for list_xml_url in list_xml_urls:
                if sync_run_id:
                    OperationLog.log(sync_run_id, "list", f"Попытка прямого получения list.xml: {list_xml_url}", stage='list')
                
                try:
                    response = page.goto(list_xml_url, wait_until="networkidle", timeout=timeout)
                    if response and response.status < 400:
                        body = response.body()
                        if body and len(body) > 0:
                            # Проверяем, что это действительно XML
                            if b'<?xml' in body[:100] or b'<datasets' in body[:200] or b'<dataset' in body[:200]:
                                output_path.write_bytes(body)
                                if output_path.exists() and output_path.stat().st_size > 0:
                                    if sync_run_id:
                                        OperationLog.log(sync_run_id, "list", 
                                                       f"Файл list.xml успешно скачан напрямую с {list_xml_url} ({output_path.stat().st_size} байт)", stage='list')
                                    browser.close()
                                    return output_path
                except Exception as e:
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", 
                                       f"Прямое получение с {list_xml_url} не удалось: {str(e)}", 
                                       level="WARNING", stage='list')
                    continue
            
            if sync_run_id:
                OperationLog.log(sync_run_id, "list", "Все прямые URL не сработали, пробуем через страницу с кнопкой", stage='list')
            
            # Если прямой способ не сработал, пробуем через страницу с кнопкой
            if sync_run_id:
                OperationLog.log(sync_run_id, "list", f"Открытие страницы {SOURCE_URL} для поиска кнопки", stage='list')
            
            try:
                page.goto(SOURCE_URL, wait_until="networkidle", timeout=timeout)
            except Exception as e:
                if sync_run_id:
                    OperationLog.log(sync_run_id, "list", 
                                   f"Ошибка при открытии страницы {SOURCE_URL}: {str(e)}", 
                                   level="ERROR", stage='list')
                raise Exception(f"Не удалось открыть страницу {SOURCE_URL}: {str(e)}")
            
            # Ищем кнопку "Скачать" для list.xml
            # По описанию изображения, кнопка находится в разделе "Реестр наборов данных"
            try:
                if sync_run_id:
                    OperationLog.log(sync_run_id, "list", "Начинаем поиск кнопки 'Скачать'", stage='list')
                
                # Пробуем разные варианты поиска кнопки
                download_button = None
                
                # Вариант 1: Ищем в разделе "Реестр наборов данных" (самый специфичный)
                if sync_run_id:
                    OperationLog.log(sync_run_id, "list", "Поиск в разделе 'Реестр наборов данных'", stage='list')
                section = page.locator('text="Реестр наборов данных"')
                if section.count() > 0:
                    # Ищем родительский элемент раздела
                    parent = section.locator('..')
                    if parent.count() > 0:
                        # Ищем ссылку или кнопку с текстом "Скачать" в этом разделе
                        buttons_in_section = parent.locator('a:has-text("Скачать"), button:has-text("Скачать")')
                        if buttons_in_section.count() > 0:
                            download_button = buttons_in_section.first
                            if sync_run_id:
                                OperationLog.log(sync_run_id, "list", "Найдена кнопка в разделе 'Реестр наборов данных'", stage='list')
                
                # Вариант 2: По тексту "Скачать" (более общий поиск)
                if not download_button or download_button.count() == 0:
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", "Поиск по тексту 'Скачать'", stage='list')
                    buttons = page.locator('a:has-text("Скачать"), button:has-text("Скачать")')
                    count = buttons.count()
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", f"Найдено элементов с текстом 'Скачать': {count}", stage='list')
                    if count > 0:
                        download_button = buttons.first
                
                # Вариант 3: По ссылке на list.xml
                if not download_button or download_button.count() == 0:
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", "Поиск по ссылке на list.xml", stage='list')
                    links = page.locator('a[href*="list.xml"]')
                    count = links.count()
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", f"Найдено ссылок на list.xml: {count}", stage='list')
                    if count > 0:
                        download_button = links.first
                
                # Вариант 4: По атрибуту download
                if not download_button or download_button.count() == 0:
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", "Поиск по атрибуту download", stage='list')
                    links = page.locator('[download]')
                    count = links.count()
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", f"Найдено элементов с атрибутом download: {count}", stage='list')
                    if count > 0:
                        download_button = links.first
                
                # Вариант 5: Ищем любую ссылку с XML иконкой или в контексте "Реестр наборов данных"
                if not download_button or download_button.count() == 0:
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", "Поиск ссылок в контексте раздела", stage='list')
                    # Ищем все ссылки рядом с текстом "Реестр наборов данных"
                    section_text = page.locator('text="Реестр наборов данных"')
                    if section_text.count() > 0:
                        # Ищем все ссылки в родительском контейнере
                        container = section_text.locator('xpath=ancestor::div[1] | ancestor::section[1] | ancestor::article[1]')
                        if container.count() > 0:
                            all_links = container.locator('a')
                            link_count = all_links.count()
                            if sync_run_id:
                                OperationLog.log(sync_run_id, "list", f"Найдено ссылок в контейнере: {link_count}", stage='list')
                            if link_count > 0:
                                # Берем первую ссылку, которая может быть кнопкой скачивания
                                download_button = all_links.first
                
                # Проверяем, что кнопка действительно найдена и видима
                if download_button and download_button.count() > 0:
                    try:
                        # Проверяем видимость
                        is_visible = download_button.is_visible()
                        if sync_run_id:
                            OperationLog.log(sync_run_id, "list", 
                                           f"Найдена кнопка скачивания, видима: {is_visible}", stage='list')
                        
                        if not is_visible:
                            # Пробуем прокрутить к элементу
                            download_button.scroll_into_view_if_needed()
                            time.sleep(0.5)
                        
                        # Настраиваем загрузку файла
                        if sync_run_id:
                            OperationLog.log(sync_run_id, "list", "Клик по кнопке скачивания", stage='list')
                        
                        with page.expect_download(timeout=timeout) as download_info:
                            download_button.click()
                        
                        download = download_info.value
                        download.save_as(output_path)
                        
                        if sync_run_id:
                            OperationLog.log(sync_run_id, "list", 
                                           f"Файл list.xml успешно скачан: {output_path} ({output_path.stat().st_size} байт)", stage='list')
                        
                        browser.close()
                        return output_path
                    except Exception as click_error:
                        if sync_run_id:
                            OperationLog.log(sync_run_id, "list", 
                                           f"Ошибка при клике на кнопку: {str(click_error)}", 
                                           level="WARNING", stage='list')
                        # Продолжаем к следующему варианту
                        pass
                
                # Если кнопка не найдена или клик не сработал
                if sync_run_id:
                    OperationLog.log(sync_run_id, "list", 
                                   "Кнопка не найдена или клик не сработал, пробуем прямой переход", stage='list')
                
                # Сохраняем скриншот для отладки
                try:
                    debug_screenshot = DOWNLOAD_DIR / "debug_page.png"
                    page.screenshot(path=str(debug_screenshot), full_page=True)
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", 
                                       f"Скриншот страницы сохранен: {debug_screenshot}", stage='list')
                except Exception as screenshot_error:
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", 
                                       f"Не удалось сохранить скриншот: {str(screenshot_error)}", 
                                       level="WARNING", stage='list')
                
                # Закрываем текущую страницу и создаем новую для прямого перехода
                page.close()
                page = context.new_page()
                
                # Пробуем все варианты URL для list.xml
                list_xml_urls = [
                    f"{SOURCE_URL.rstrip('/')}/list.xml",
                    "https://proverki.gov.ru/portal/public-open-data/list.xml",
                    "https://proverki.gov.ru/list.xml",
                    "https://proverki.gov.ru/portal/list.xml"
                ]
                
                for list_xml_url in list_xml_urls:
                    try:
                        if sync_run_id:
                            OperationLog.log(sync_run_id, "list", f"Переход на URL: {list_xml_url}", stage='list')
                        
                        response = page.goto(list_xml_url, wait_until="networkidle", timeout=timeout)
                        
                        if not response:
                            continue
                        
                        if sync_run_id:
                            OperationLog.log(sync_run_id, "list", f"Получен ответ со статусом: {response.status}", stage='list')
                        
                        if response.status >= 400:
                            continue
                        
                        # Получаем тело ответа напрямую
                        body = response.body()
                        if not body or len(body) == 0:
                            continue
                        
                        # Проверяем, что это действительно XML
                        if b'<?xml' in body[:100] or b'<datasets' in body[:200] or b'<dataset' in body[:200]:
                            if sync_run_id:
                                OperationLog.log(sync_run_id, "list", f"Получено {len(body)} байт данных из list.xml", stage='list')
                            
                            # Сохраняем файл
                            output_path.write_bytes(body)
                            
                            if output_path.exists() and output_path.stat().st_size > 0:
                                if sync_run_id:
                                    OperationLog.log(sync_run_id, "list", 
                                                   f"Файл list.xml успешно скачан через прямой URL {list_xml_url} ({output_path.stat().st_size} байт)", stage='list')
                                browser.close()
                                return output_path
                    
                    except Exception as url_error:
                        if sync_run_id:
                            OperationLog.log(sync_run_id, "list", 
                                           f"Ошибка при переходе на {list_xml_url}: {str(url_error)}", 
                                           level="WARNING", stage='list')
                        continue
                
                # Если все URL не сработали, пробуем последний вариант - через requests
                try:
                    browser.close()
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", 
                                       "Пробуем fallback через requests (может быть заблокирован)", stage='list')
                    response = requests.get(f"{SOURCE_URL.rstrip('/')}/list.xml", timeout=30)
                    if response.status_code == 200:
                        output_path.write_bytes(response.content)
                        if sync_run_id:
                            OperationLog.log(sync_run_id, "list", 
                                           f"Файл list.xml скачан через requests fallback", stage='list')
                        return output_path
                    else:
                        raise Exception(f"HTTP {response.status_code}")
                except Exception as e2:
                    if sync_run_id:
                        OperationLog.log(sync_run_id, "list", 
                                       f"Fallback через requests также не удался: {str(e2)}", 
                                       level="WARNING", stage='list')
                
                raise Exception("Не удалось получить list.xml ни по одному из URL и методов")
                    
            except PlaywrightTimeoutError as e:
                if browser:
                    browser.close()
                raise Exception(f"Таймаут при ожидании загрузки файла (>{timeout}ms): {str(e)}")
            
    except Exception as e:
        if browser:
            try:
                browser.close()
            except:
                pass
        error_msg = f"Ошибка при скачивании list.xml: {str(e)}"
        if sync_run_id:
            OperationLog.log(sync_run_id, "list", error_msg, level="ERROR", stage='list')
        raise Exception(error_msg)
    
    return None

