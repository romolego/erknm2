"""Поиск уже скачанных файлов"""
from pathlib import Path
from erknm.config import DOWNLOAD_DIR

print("Поиск скачанных файлов...")
print("=" * 60)

# Ищем все XML файлы
xml_files = list(DOWNLOAD_DIR.rglob("*.xml"))
print(f"\nНайдено XML файлов: {len(xml_files)}")

if xml_files:
    print("\nXML файлы:")
    for f in xml_files[:20]:  # Показываем первые 20
        print(f"  {f.relative_to(DOWNLOAD_DIR)}")
    if len(xml_files) > 20:
        print(f"  ... и еще {len(xml_files) - 20} файлов")

# Ищем все ZIP файлы
zip_files = list(DOWNLOAD_DIR.rglob("*.zip"))
print(f"\nНайдено ZIP файлов: {len(zip_files)}")

if zip_files:
    print("\nZIP файлы:")
    for f in zip_files[:20]:  # Показываем первые 20
        print(f"  {f.relative_to(DOWNLOAD_DIR)}")
    if len(zip_files) > 20:
        print(f"  ... и еще {len(zip_files) - 20} файлов")

# Проверяем структуру папок
print("\nСтруктура папок:")
for item in sorted(DOWNLOAD_DIR.iterdir()):
    if item.is_dir():
        file_count = len(list(item.rglob("*")))
        print(f"  {item.name}/ ({file_count} элементов)")







