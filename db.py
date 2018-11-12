import csv
import sqlite3
import sys
import time
import logging


def csv_reader(file_obj):
    reader = csv.reader(file_obj)
    return reader


def write_to_database(file):
    global note_number
    global error
    conn = sqlite3.connect("data_base.sqlite3")
    cursor = conn.cursor()
    cursor.execute("SELECT sql FROM sqlite_master WHERE name='calls'")
    if not cursor.fetchall():  # проверяем, есть ли таблица calls в данной базе данных, если нет - создём её
        cursor.execute("""CREATE TABLE calls
                          ('Crime Id' integer UNIQUE, 'Original Crime Type Name' text, 'Report Date' text, 
                            'Call Date' text,'Offense Date' text, 'Call Time' text, 'Call Date Time' text, 'Disposition' text, 
                            'Address' text,'City' text, 'State' text, 'Agency Id' integer, 'Address Type' text, 
                            'Common Location' text
                          )
                       """)
    print("Считывания данных из csv файла...")
    calls = list(file)
    note_number = len(calls)
    print("Запись в базу данных...")
    cursor.executemany("INSERT INTO calls VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", calls)
    conn.commit()


def write_log():
    print(f"Записей в базе {note_number}")
    print(f"Время выполнения {round(time.process_time(), 2)}")
    logging.info("Запись окончена:\n"
            f"{note_number} записей в базе данных\n"
            f"{round(time.process_time(), 2)} время выполнения")


if __name__ == "__main__":
    logging.basicConfig(filename="db_script.log", level=logging.INFO,
        format = f'{time.asctime()[4:]}: %(levelname)s: %(message)s')
    csv_path = "police-department-calls-for-service.csv"
    try:
        with open(csv_path, "r") as f_obj:
            write_to_database(csv_reader(f_obj))
    except IOError:
        logging.error("Ошибка при открытии csv файла")
        print("Ошибка при открытии csv файла. Нажмите любую клавишу ...")
        input()
        sys.exit()
    write_log()


