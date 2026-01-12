"""Модуль для запуска по расписанию"""
import schedule
import time
from erknm.sync.synchronizer import sync
from erknm.config import (
    SCHEDULE_ENABLED, SCHEDULE_MODE, SCHEDULE_TIME,
    SCHEDULE_DAY_OF_WEEK, SCHEDULE_DAY_OF_MONTH
)


def run_scheduler():
    """
    Запустить планировщик синхронизации с настройками из БД
    """
    if not SCHEDULE_ENABLED:
        print("Планировщик отключен в настройках.")
        return
    
    # Парсим время
    try:
        hour, minute = map(int, SCHEDULE_TIME.split(':'))
    except:
        hour, minute = 2, 0
    
    # Настраиваем расписание в зависимости от режима
    if SCHEDULE_MODE == 'daily':
        schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(sync, is_manual=False)
        print(f"Планировщик запущен. Синхронизация будет выполняться ежедневно в {SCHEDULE_TIME}.")
    elif SCHEDULE_MODE == 'weekly':
        day_map = {1: schedule.every().monday, 2: schedule.every().tuesday, 3: schedule.every().wednesday,
                   4: schedule.every().thursday, 5: schedule.every().friday, 6: schedule.every().saturday,
                   7: schedule.every().sunday}
        day = SCHEDULE_DAY_OF_WEEK if 1 <= SCHEDULE_DAY_OF_WEEK <= 7 else 1
        day_map[day].at(f"{hour:02d}:{minute:02d}").do(sync, is_manual=False)
        print(f"Планировщик запущен. Синхронизация будет выполняться еженедельно в {SCHEDULE_TIME} (день недели: {day}).")
    elif SCHEDULE_MODE == 'monthly':
        # Для ежемесячного режима используем день месяца
        day = SCHEDULE_DAY_OF_MONTH if 1 <= SCHEDULE_DAY_OF_MONTH <= 28 else 1
        # Schedule не поддерживает напрямую месячный режим, используем ежедневную проверку
        def monthly_job():
            if time.localtime().tm_mday == day:
                sync(is_manual=False)
        schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(monthly_job)
        print(f"Планировщик запущен. Синхронизация будет выполняться ежемесячно {day} числа в {SCHEDULE_TIME}.")
    else:
        # Дефолт - ежедневно
        schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(sync, is_manual=False)
        print(f"Планировщик запущен. Синхронизация будет выполняться ежедневно в {SCHEDULE_TIME}.")
    
    print("Нажмите Ctrl+C для остановки.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Проверяем каждую минуту
    except KeyboardInterrupt:
        print("\nПланировщик остановлен.")


if __name__ == "__main__":
    run_scheduler()



