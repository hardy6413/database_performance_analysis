import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk

import pandas as pd

from src.constants import POSTGRESQL, MONGODB, REDIS, \
    MEAN_AND_MEDIAN, WORD_COUNT, STATISTICS, \
    EXECUTION_TIME, SELECT, DELETE, MODIFY, EXECUTE
from src.repositories import redis_repository, mongo_repository, postgres_repository

pd.set_option('display.max_columns', 5)


class DatabaseOperationsApp:
    def __init__(self):
        self.window = tk.Tk()
        self.window.title("Wybór bazy danych")
        self.window.geometry("600x550")
        self.tab_control = ttk.Notebook(self.window)

        for db_name in [POSTGRESQL, MONGODB, REDIS]:
            tab = ttk.Frame(self.tab_control)
            self.tab_control.add(tab, text=db_name)

            self.inputtxt = tk.Text(tab, height=5)
            self.inputtxt.pack()

            if db_name == REDIS:
                self.execute = tk.Button(tab, text=SELECT, command=lambda: self.execute_query(db_name))
                self.execute.pack(pady=1, padx=5)

                self.btn_save = tk.Button(tab, text=SELECT, command=lambda: self.execute_save(db_name))
                self.btn_save.pack(pady=1, padx=5)

                self.btn_delete = tk.Button(tab, text=DELETE, command=lambda: self.execute_delete(db_name))
                self.btn_delete.pack(pady=1, padx=5)

                self.btn_modify = tk.Button(tab, text=MODIFY, command=lambda: self.execute_update(db_name))
                self.btn_modify.pack(pady=1, padx=5)
            else:
                self.execute = tk.Button(tab, text=EXECUTE, command=lambda: self.execute_query(db_name))
                self.execute.pack(pady=1, padx=5)

            self.lbl = ScrolledText(tab, wrap=tk.WORD, height=8)
            self.lbl.pack()

            self.execute_count_button = tk.Button(tab, text=STATISTICS, command=lambda: self.execute_count(db_name))
            self.execute_count_button.pack(pady=5)

            self.execute_word_button = tk.Button(tab, text=WORD_COUNT, command=lambda: self.execute_word(db_name))
            self.execute_word_button.pack(pady=5)

            self.execute_mean_button = tk.Button(tab, text=MEAN_AND_MEDIAN, command=lambda: self.execute_mean(db_name))
            self.execute_mean_button.pack(pady=5)

            self.duration = tk.Label(tab, text=EXECUTION_TIME)
            self.duration.pack()

        self.tab_control.bind("<<NotebookTabChanged>>", self.switch_tab)
        self.tab_control.pack(expand=1, fill='both')

        self.window.mainloop()

    def run(self):
        self.window.mainloop()

    def switch_tab(self, event):
        self.tab_control.index(self.tab_control.select())

    def execute_save(self, db_name):
        pass

    def execute_query(self, db_name):
        selected_db = db_name
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            res = postgres_repository.execute_query(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.selectDurations[-1]))
            print("query w PostgreSQL")
        elif selected_db == MONGODB:
            res = mongo_repository.execute_query(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.selectDurations[-1]))
            print("query danych w MongoDB")
        elif selected_db == REDIS:
            res = redis_repository.execute_query(inp)
            print("query danych w Redis")

        self.lbl.insert(tk.END, "\nQUERY RESULT --------------------------------------------------------\n"
                        + str(res.head()))

    def execute_delete(self, db_name):
        selected_db = db_name
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            res = postgres_repository.execute_delete(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.deleteDurations[-1]))
            print("delete w PostgreSQL")
        elif selected_db == MONGODB:
            res = mongo_repository.execute_delete(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.deleteDurations[-1]))
            print("delete danych w MongoDB")
        elif selected_db == REDIS:
            res = redis_repository.execute_query(inp)
            print("delete danych w Redis")

    def execute_update(self, db_name):
        selected_db = db_name
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            res = postgres_repository.execute_update(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.updateDurations[-1]))
            print("update w PostgreSQL")
        elif selected_db == MONGODB:
            res = mongo_repository.execute_update(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.updateDurations[-1]))
            print("update danych w MongoDB")
        elif selected_db == REDIS:
            res = redis_repository.execute_query(inp)
            print("update danych w Redis")

    def clear_data(self, db_name):
        selected_db = db_name
        if selected_db == POSTGRESQL:
            postgres_repository.delete_all()
            print("czyszczenie danych z Postgresql")
        elif selected_db == MONGODB:
            mongo_repository.delete_all()
            print("czyszczenie danych z MongoDB")
        elif selected_db == REDIS:
            redis_repository.delete_all()
            print("czyszczenie danych z Redis")

    def execute_count(self, db_name):
        selected_db = db_name
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            res = postgres_repository.execute_count(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.countDurations[-1]))
            self.lbl.insert(tk.END,
                            "\nQUERY RESULT --------------------------------------------------------\n"
                            + "liczba rekordow  "
                            + str(res))
            print("statystyki liczbowe w PostgreSQL")
        elif selected_db == MONGODB:
            res = mongo_repository.execute_count(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.countDurations[-1]))
            self.lbl.insert(tk.END,
                            "\nQUERY RESULT --------------------------------------------------------\n"
                            + "liczba rekordow  "
                            + str(res))
            print("update danych w MongoDB")
        elif selected_db == REDIS:
            res = redis_repository.execute_query(inp)
            print("update danych w Redis")

    def execute_word(self, db_name):
        selected_db = db_name
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            res = postgres_repository.execute_word(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.wordDurations[-1]))
            self.lbl.insert(tk.END,
                            "\nQUERY RESULT --------------------------------------------------------\n"
                            + "liczba wystąpien  "
                            + str(res))
            print("statystyki liczbowe w PostgreSQL")
        elif selected_db == MONGODB:
            res = mongo_repository.execute_word(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.wordDurations[-1]))
            self.lbl.insert(tk.END,
                            "\nQUERY RESULT --------------------------------------------------------\n"
                            + "liczba wystąpien  "
                            + str(res))
            print("update danych w MongoDB")
        elif selected_db == REDIS:
            res = redis_repository.execute_query(inp)
            print("update danych w Redis")

    def execute_mean(self, db_name):
        selected_db = db_name
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            mean, mode = postgres_repository.execute_mean(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.meanDurations[-1]))
            self.lbl.insert(tk.END,
                            "\nQUERY RESULT --------------------------------------------------------\n"
                            + " srednie  "
                            + str(mean)
                            + " \n mediany "
                            + str(mode))
            print("statystyki liczbowe w PostgreSQL")
        elif selected_db == MONGODB:
            mean, mode = mongo_repository.execute_mean(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.meanDurations[-1]))
            self.lbl.insert(tk.END,
                            "\nQUERY RESULT --------------------------------------------------------\n"
                            + " srednie  "
                            + str(mean)
                            + " \n mediany "
                            + str(mode))
        elif selected_db == REDIS:
            res = redis_repository.execute_query(inp)
            print("update danych w Redis")
