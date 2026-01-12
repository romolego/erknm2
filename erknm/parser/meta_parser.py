"""Парсер мета-XML файлов"""
from pathlib import Path
from lxml import etree
from typing import List, Dict, Optional
import time
import random
from erknm.browser.meta_downloader import download_meta_xml_browser


def download_meta_xml(url: str, output_path: Path, max_retries=5, delay=10.0, sync_run_id=None) -> Path:
    """
    Скачать мета-XML файл через браузерную автоматизацию с retry механизмом
    
    Args:
        url: URL файла
        output_path: Путь для сохранения
        max_retries: Максимальное количество попыток
        delay: Базовая задержка между попытками в секундах (3-5 секунд)
        sync_run_id: ID запуска синхронизации для логирования (опционально)
    
    Returns:
        Path к скачанному файлу
    """
    # Используем браузерную автоматизацию через Playwright
    return download_meta_xml_browser(url, output_path, sync_run_id, max_retries, delay)


def parse_meta_xml(file_path: Path) -> Dict:
    """
    Парсить мета-XML файл набора данных
    
    Returns:
        Словарь с метаданными и списком версий данных
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")
    
    tree = etree.parse(str(file_path))
    root = tree.getroot()
    
    # Извлекаем метаданные
    meta = {
        'identifier': root.xpath('.//identifier/text()')[0] if root.xpath('.//identifier') else '',
        'title': root.xpath('.//title/text()')[0] if root.xpath('.//title') else '',
        'description': root.xpath('.//description/text()')[0] if root.xpath('.//description') else '',
        'creator': root.xpath('.//creator/text()')[0] if root.xpath('.//creator') else '',
        'subject': root.xpath('.//subject/text()')[0] if root.xpath('.//subject') else '',
    }
    
    # Извлекаем версии данных
    data_versions = []
    for dataversion in root.xpath('.//dataversion'):
        source = dataversion.xpath('.//source/text()')
        created = dataversion.xpath('.//created/text()')
        provenance = dataversion.xpath('.//provenance/text()')
        structure = dataversion.xpath('.//structure/text()')
        
        if source:
            data_versions.append({
                'source': source[0],
                'created': created[0] if created else '',
                'provenance': provenance[0] if provenance else '',
                'structure': structure[0] if structure else ''
            })
    
    meta['data_versions'] = data_versions
    
    return meta


