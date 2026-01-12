"""Классификатор данных"""
from typing import Optional
from pathlib import Path
from lxml import etree


def classify_dataset(identifier: str, title: str, link: str) -> Optional[str]:
    """
    Классифицировать набор данных по identifier, title и link
    
    Returns:
        'plan' - планы проверок
        'inspection' - проверки
        None - неклассифицировано
    """
    identifier_lower = identifier.lower()
    title_lower = title.lower()
    link_lower = link.lower()
    
    # Проверяем на планы проверок
    if 'plan' in identifier_lower or 'план' in title_lower:
        return 'plan'
    
    # Проверяем на проверки
    if 'inspection' in identifier_lower or 'проверк' in title_lower:
        return 'inspection'
    
    return None


def classify_xml_file(file_path: Path) -> Optional[str]:
    """
    Классифицировать XML файл по содержимому
    
    Returns:
        'plan' - планы проверок
        'inspection' - проверки
        None - неклассифицировано
    """
    try:
        tree = etree.parse(str(file_path))
        root = tree.getroot()
        
        # Проверяем корневой элемент
        root_tag = root.tag.lower()
        
        if 'plan' in root_tag or 'план' in root_tag:
            return 'plan'
        
        if 'inspection' in root_tag or 'проверк' in root_tag:
            return 'inspection'
        
        # Проверяем namespace или другие признаки
        if root.nsmap:
            for ns in root.nsmap.values():
                if 'plan' in ns.lower():
                    return 'plan'
                if 'inspection' in ns.lower() or 'inspection' in ns.lower():
                    return 'inspection'
        
        # Проверяем наличие характерных элементов
        if root.xpath('.//PLAN') or root.xpath('.//plan'):
            return 'plan'
        
        if root.xpath('.//INSPECTION') or root.xpath('.//inspection'):
            return 'inspection'
        
    except Exception:
        pass
    
    return None








