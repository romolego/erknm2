"""Парсер list.xml"""
from pathlib import Path
from lxml import etree
from typing import List, Dict


def parse_list_xml(file_path: Path) -> List[Dict]:
    """
    Парсить list.xml и извлечь список наборов данных
    
    Returns:
        Список словарей с ключами: identifier, title, link, format
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")
    
    tree = etree.parse(str(file_path))
    root = tree.getroot()
    
    datasets = []
    
    # Ищем все item в standardversion
    for item in root.xpath('.//item'):
        datasets.append({
            'identifier': item.get('identifier', ''),
            'title': item.get('title', ''),
            'link': item.get('link', ''),
            'format': item.get('format', 'xml')
        })
    
    return datasets








