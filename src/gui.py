import tkinter as tk
from tkinter.scrolledtext import ScrolledText

import pandas as pd

from src.constants import POSTGRESQL, MONGODB, REDIS, \
    MEAN_AND_MEDIAN, WORD_COUNT, STATISTICS, \
    EXECUTION_TIME, SELECT, DELETE, MODIFY, INSERT
from src.repositories import redis_repository, mongo_repository, postgres_repository

pd.set_option('display.max_columns', 5)


class DatabaseOperationsApp:
    def __init__(self):
        self.window = tk.Tk()
        self.window.title("Wybór bazy danych")
        self.window.geometry("700x700")
        self.window.resizable(False, False)

        self.db_selection = tk.StringVar()
        self.db_selection.set(POSTGRESQL)

        self.db_dropdown = tk.OptionMenu(self.window, self.db_selection, POSTGRESQL, MONGODB, REDIS)
        self.db_dropdown.grid(row=0, column=2, sticky="ew", padx=20, pady=10)

        self.inputtxt = tk.Text(self.window, height=6)
        self.inputtxt.grid(row=1, column=1, columnspan=4, sticky="nsew", padx=20)

        self.execute = tk.Button(self.window, text=SELECT, command=self.execute_query, width=13)
        self.execute.grid(row=2, column=2, sticky="w", padx=70, pady=10)

        self.execute_delete_button = tk.Button(self.window, text=DELETE, command=self.execute_delete, width=13)
        self.execute_delete_button.grid(row=3, column=1, sticky="w", padx=50,  pady=5)

        self.execute_update_button = tk.Button(self.window, text=MODIFY, command=self.execute_update, width=13)
        self.execute_update_button.grid(row=3, column=2, sticky="w", padx=70, pady=5)

        self.execute_update_button = tk.Button(self.window, text=INSERT, command=self.execute_save, width=13)
        self.execute_update_button.grid(row=3, column=3, sticky="w", padx=70, pady=5)

        self.execute_count_button = tk.Button(self.window, text=STATISTICS, command=self.execute_count, width=13)
        self.execute_count_button.grid(row=4, column=1, padx=50, sticky="w",  pady=5)

        self.execute_word_button = tk.Button(self.window, text=WORD_COUNT, command=self.execute_word, width=13)
        self.execute_word_button.grid(row=4, column=2, sticky="w", padx=70,  pady=5)

        self.execute_mean_button = tk.Button(self.window, text=MEAN_AND_MEDIAN, command=self.execute_mean, width=13)
        self.execute_mean_button.grid(row=4, column=3, sticky="w", padx=70, pady=5)

        self.lbl = ScrolledText(self.window, wrap=tk.WORD)
        self.lbl.grid(row=5, column=1, columnspan=4, sticky="nsew", padx=20)

        self.duration = tk.Label(self.window, text=EXECUTION_TIME)
        self.duration.grid(row=6, column=1, sticky="w", padx=20, pady=5)

    def run(self):
        self.window.mainloop()

    def execute_query(self):
        selected_db = self.db_selection.get()
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            res = postgres_repository.execute_query(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.select_durations[-1]))
            self.lbl.insert(tk.END, str(res.head()))
        elif selected_db == MONGODB:
            res = mongo_repository.execute_query(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.select_durations[-1]))
            self.lbl.insert(tk.END, str(res.head()))
        elif selected_db == REDIS:
            res = redis_repository.execute_get_by_key(inp)
            self.duration.config(text="czas wykonania: " + str(redis_repository.select_durations[-1]))
            self.lbl.insert(tk.END, str(res))

    def execute_delete(self):
        selected_db = self.db_selection.get()
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            postgres_repository.execute_delete(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.delete_durations[-1]))
        elif selected_db == MONGODB:
            mongo_repository.execute_delete(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.delete_durations[-1]))
        elif selected_db == REDIS:
            redis_repository.execute_delete(keys=inp)
            self.duration.config(text="czas wykonania: " + str(redis_repository.delete_durations[-1]))

    def execute_save(self):
        selected_db = self.db_selection.get()
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            res = postgres_repository.execute_insert(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.insert_durations[-1]))
            self.lbl.insert(tk.END, str(res.head()))
        elif selected_db == MONGODB:
            res = mongo_repository.execute_insert(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.insert_durations[-1]))
            self.lbl.insert(tk.END, str(res.head()))
        elif selected_db == REDIS:
            redis_repository.execute_insert(inp)
            self.duration.config(text="czas wykonania: " + str(redis_repository.insert_durations[-1]))

    def execute_update(self):
        selected_db = self.db_selection.get()
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            postgres_repository.execute_update(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.update_durations[-1]))
        elif selected_db == MONGODB:
            mongo_repository.execute_update(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.update_durations[-1]))
        elif selected_db == REDIS:
            redis_repository.execute_update(inp)
            self.duration.config(text="czas wykonania: " + str(redis_repository.update_durations[-1]))

    def execute_count(self):
        selected_db = self.db_selection.get()
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            res = postgres_repository.execute_count(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.count_durations[-1]))
            self.print_count(self.lbl, res)
        elif selected_db == MONGODB:
            res = mongo_repository.execute_count(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.count_durations[-1]))
            self.print_count(self.lbl, res)
        elif selected_db == REDIS:
            # FIXME:
            res = redis_repository.execute_count(inp)
            self.duration.config(text="czas wykonania: " + str(redis_repository.count_durations[-1]))
            self.print_count(self.lbl, res)
            print("update danych w Redis")

    def execute_word(self):
        selected_db = self.db_selection.get()
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            res = postgres_repository.execute_word(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.word_durations[-1]))
            self.print_word(self.lbl, res)

            print("statystyki liczbowe w PostgreSQL")
        elif selected_db == MONGODB:
            res = mongo_repository.execute_word(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.word_durations[-1]))
            self.print_word(self.lbl, res)
            print("update danych w MongoDB")
        elif selected_db == REDIS:
            # FIXME:
            res = redis_repository.execute_word(inp)
            self.duration.config(text="czas wykonania: " + str(redis_repository.word_durations[-1]))
            self.print_word(self.lbl, res)
            print("update danych w Redis")

    def execute_mean(self):
        selected_db = self.db_selection.get()
        inp = self.inputtxt.get(1.0, "end-1c")
        if selected_db == POSTGRESQL:
            mean, mode = postgres_repository.execute_mean(inp)
            self.duration.config(text="czas wykonania: " + str(postgres_repository.mean_durations[-1]))
            self.print_mean(self.lbl, mean, mode)
            print("statystyki liczbowe w PostgreSQL")
        elif selected_db == MONGODB:
            mean, mode = mongo_repository.execute_mean(inp)
            self.duration.config(text="czas wykonania: " + str(mongo_repository.mean_durations[-1]))
            self.print_mean(self.lbl, mean, mode)
        elif selected_db == REDIS:
            # FIXME
            mean, mode = redis_repository.execute_mean(inp)
            self.duration.config(text="czas wykonania: " + str(redis_repository.mean_durations[-1]))
            self.print_mean(self.lbl, mean, mode)
            print("update danych w Redis")

    def print_word(self, label: ScrolledText, res):
        label.insert(tk.END, "liczba wystąpien " + str(res))

    def print_mean(self, label: ScrolledText, mean, mode):
        label.insert(tk.END, "srednie  " + str(mean) + "\nmediany " + str(mode))

    def print_count(self, label: ScrolledText, res):
        label.insert(tk.END, "liczba rekordow " + str(res))
