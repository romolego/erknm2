"""Утилита для безопасного удаления папок downloads/extracted"""
import sys
import shutil
from pathlib import Path
from erknm.config import DOWNLOAD_DIR

def cleanup_extracted(confirm=False):
    """
    Безопасно удалить папку downloads/extracted
    
    Args:
        confirm: Если False, требует подтверждение пользователя
    """
    extracted_dir = DOWNLOAD_DIR / "extracted"
    zips_dir = DOWNLOAD_DIR / "zips"
    
    if not extracted_dir.exists():
        print(f"Папка {extracted_dir} не существует. Нечего удалять.")
        return
    
    # Подсчитываем размер
    total_size = 0
    file_count = 0
    for item in extracted_dir.rglob('*'):
        if item.is_file():
            total_size += item.stat().st_size
            file_count += 1
    
    size_mb = total_size / (1024 * 1024)
    size_gb = total_size / (1024 * 1024 * 1024)
    
    print("=" * 60)
    print("Очистка папки downloads/extracted")
    print("=" * 60)
    print(f"Папка: {extracted_dir}")
    print(f"Файлов: {file_count}")
    if size_gb >= 1:
        print(f"Размер: {size_gb:.2f} GB ({size_mb:.2f} MB)")
    else:
        print(f"Размер: {size_mb:.2f} MB")
    print()
    print("ВНИМАНИЕ: Эта папка больше не используется системой.")
    print("ZIP архивы сохраняются в downloads/zips/ и обрабатываются напрямую.")
    print()
    
    if not confirm:
        response = input("Удалить папку downloads/extracted? (yes/no): ")
        if response.lower() not in ('yes', 'y', 'да', 'д'):
            print("Отменено.")
            return
    
    try:
        print(f"Удаление {extracted_dir}...")
        shutil.rmtree(extracted_dir)
        print(f"✓ Папка {extracted_dir} успешно удалена.")
        print(f"✓ Освобождено места: {size_mb:.2f} MB")
    except Exception as e:
        print(f"✗ Ошибка при удалении: {e}")
        sys.exit(1)
    
    # Проверяем, что папка zips на месте
    if zips_dir.exists():
        zip_count = len(list(zips_dir.glob('*.zip')))
        print(f"\n✓ Папка downloads/zips/ сохранена ({zip_count} ZIP файлов)")
    else:
        print("\n⚠ Папка downloads/zips/ не найдена")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Удалить папку downloads/extracted')
    parser.add_argument('--yes', '-y', action='store_true', 
                       help='Не требовать подтверждение')
    
    args = parser.parse_args()
    
    cleanup_extracted(confirm=args.yes)

