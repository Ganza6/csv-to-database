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
    if not cursor.fetchall(): #  проверяем, есть ли таблица calls в данной базе данных, если нет - создём её
        cursor.execute("""CREATE TABLE calls
                          ('Crime Id' integer UNIQUE, 'Original Crime Type Name' text, 'Report Date' text, 
                            'Call Date' text,'Offense Date' text, 'Call Time' text, 'Call Date Time' text, 'Disposition' text, 
                            'Address' text,'City' text, 'State' text, 'Agency Id' integer, 'Address Type' text, 
                            'Common Location' text
                          )
                       """)

    note_number = -1
    error = 0
    for row in file:
        print(row) # "Скрипт должен отображать процесс загрузки" , замедляет время его работы примерное в 2 раза
        if note_number == -1: # пропускаем первую строку, в который содержатся названия колонок
            note_number = 0
            continue
        new_row = []
        for column in row: # иногда, в строках встречаются двойные кавычки(обычно в каких-либо названиях)
            column = column.replace('"', "'") # для того, что бы избежать конфликтов с бд, меняем двойные кавычки на одинарные
            new_row.append(f'"{column}"')
        insert = f'INSERT INTO calls VALUES({", ".join(new_row)})'
        try:
            cursor.execute(insert)
            note_number += 1
        except(sqlite3.OperationalError):
            error += 1
    conn.commit()

def write_log():
    logging.info("Запись окончена:\n"
            f"{note_number} записей в базе данных\n"
            f"{error} ошибок\n"
            f"{round(time.process_time(),2)} время выполнения")


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


