import json
import re
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk

import pandas as pd

from src.constants import POSTGRESQL, MONGODB, REDIS, \
    MEAN_AND_MEDIAN, WORD_COUNT, STATISTICS, \
    EXECUTION_TIME, SELECT, DELETE, MODIFY, EXECUTE, INSERT
from src.repositories import redis_repository, mongo_repository, postgres_repository

pd.set_option('display.max_columns', 5)


class DatabaseOperationsApp:
    def __init__(self):
        self.window = tk.Tk()
        self.window.title("Wybór bazy danych")
        self.window.geometry("600x550")
        self.tab_control = ttk.Notebook(self.window)

        tab_postgres = ttk.Frame(self.tab_control)
        self.tab_control.add(tab_postgres, text=POSTGRESQL)
        self.inputtxt_postgres = tk.Text(tab_postgres, height=5)
        self.inputtxt_postgres.pack()
        self.execute_postgres = tk.Button(tab_postgres, text=EXECUTE, command=self.execute_query)
        self.execute_postgres.pack(pady=1, padx=5)
        self.btn_save_postgres = tk.Button(tab_postgres, text=INSERT, command=self.execute_save)
        self.btn_save_postgres.pack(pady=1, padx=5)
        self.btn_delete_postgres = tk.Button(tab_postgres, text=DELETE, command=self.execute_delete)
        self.btn_delete_postgres.pack(pady=1, padx=5)
        self.btn_modify_postgres = tk.Button(tab_postgres, text=MODIFY, command=self.execute_update)
        self.btn_modify_postgres.pack(pady=1, padx=5)
        self.lbl_postgres = ScrolledText(tab_postgres, wrap=tk.WORD, height=8)
        self.lbl_postgres.pack()
        self.execute_count_button_postgres = tk.Button(tab_postgres, text=STATISTICS, command=self.execute_count)
        self.execute_count_button_postgres.pack(pady=5)
        self.execute_word_button_postgres = tk.Button(tab_postgres, text=WORD_COUNT, command=self.execute_word)
        self.execute_word_button_postgres.pack(pady=5)
        self.execute_mean_button_postgres = tk.Button(tab_postgres, text=MEAN_AND_MEDIAN, command=self.execute_mean)
        self.execute_mean_button_postgres.pack(pady=5)
        self.duration_postgres = tk.Label(tab_postgres, text=EXECUTION_TIME)
        self.duration_postgres.pack()

        tab_mongo = ttk.Frame(self.tab_control)
        self.tab_control.add(tab_mongo, text=MONGODB)
        self.inputtxt_mongo = tk.Text(tab_mongo, height=5)
        self.inputtxt_mongo.pack()
        self.execute_mongo = tk.Button(tab_mongo, text=EXECUTE, command=self.execute_query)
        self.execute_mongo.pack(pady=1, padx=5)
        self.btn_save_mongo = tk.Button(tab_mongo, text=INSERT, command=self.execute_save)
        self.btn_save_mongo.pack(pady=1, padx=5)
        self.btn_delete_mongo = tk.Button(tab_mongo, text=DELETE, command=self.execute_delete)
        self.btn_delete_mongo.pack(pady=1, padx=5)
        self.btn_modify_mongo = tk.Button(tab_mongo, text=MODIFY, command=self.execute_update)
        self.btn_modify_mongo.pack(pady=1, padx=5)
        self.lbl_mongo = ScrolledText(tab_mongo, wrap=tk.WORD, height=8)
        self.lbl_mongo.pack()
        self.execute_count_button_mongo = tk.Button(tab_mongo, text=STATISTICS, command=self.execute_count)
        self.execute_count_button_mongo.pack(pady=5)
        self.execute_word_button_mongo = tk.Button(tab_mongo, text=WORD_COUNT, command=self.execute_word)
        self.execute_word_button_mongo.pack(pady=5)
        self.execute_mean_button_mongo = tk.Button(tab_mongo, text=MEAN_AND_MEDIAN, command=self.execute_mean)
        self.execute_mean_button_mongo.pack(pady=5)
        self.duration_mongo = tk.Label(tab_mongo, text=EXECUTION_TIME)
        self.duration_mongo.pack()

        tab_redis = ttk.Frame(self.tab_control)
        self.tab_control.add(tab_redis, text=REDIS)
        self.inputtxt_redis = tk.Text(tab_redis, height=5)
        self.inputtxt_redis.pack()
        self.execute_redis = tk.Button(tab_redis, text=SELECT, command=self.execute_query)
        self.execute_redis.pack(pady=1, padx=5)
        self.btn_save_redis = tk.Button(tab_redis, text=INSERT, command=self.execute_save)
        self.btn_save_redis.pack(pady=1, padx=5)
        self.btn_delete_redis = tk.Button(tab_redis, text=DELETE, command=self.execute_delete)
        self.btn_delete_redis.pack(pady=1, padx=5)
        self.btn_modify_redis = tk.Button(tab_redis, text=MODIFY, command=self.execute_update)
        self.btn_modify_redis.pack(pady=1, padx=5)
        self.lbl_redis = ScrolledText(tab_redis, wrap=tk.WORD, height=8)
        self.lbl_redis.pack()
        self.execute_count_button_redis = tk.Button(tab_redis, text=STATISTICS, command=self.execute_count)
        self.execute_count_button_redis.pack(pady=5)
        self.execute_word_button_redis = tk.Button(tab_redis, text=WORD_COUNT, command=self.execute_word)
        self.execute_word_button_redis.pack(pady=5)
        self.execute_mean_button_redis = tk.Button(tab_redis, text=MEAN_AND_MEDIAN, command=self.execute_mean)
        self.execute_mean_button_redis.pack(pady=5)
        self.duration_redis = tk.Label(tab_redis, text=EXECUTION_TIME)
        self.duration_redis.pack()

        self.tab_control.bind("<<NotebookTabChanged>>", self.switch_tab)
        self.tab_control.pack(expand=1, fill='both')

        self.window.mainloop()

    def run(self):
        self.window.mainloop()

    def switch_tab(self, event):
        self.tab_control.index(self.tab_control.select())

    def get_current_tab_name(self):
        current_tab_index = self.tab_control.select()
        return self.tab_control.tab(current_tab_index, option="text")

    def execute_query(self):
        selected_db = self.get_current_tab_name()
        if selected_db == POSTGRESQL:
            inp = self.inputtxt_postgres.get(1.0, "end-1c")
            res = postgres_repository.execute_query(inp)
            self.duration_postgres.config(text="czas wykonania: " + str(postgres_repository.select_durations[-1]))
            self.lbl_postgres.insert(tk.END, str(res.head()))
        elif selected_db == MONGODB:
            inp = self.inputtxt_mongo.get(1.0, "end-1c")
            res = mongo_repository.execute_query(inp)
            self.duration_mongo.config(text="czas wykonania: " + str(mongo_repository.select_durations[-1]))
            self.lbl_mongo.insert(tk.END, str(res.head()))
        elif selected_db == REDIS:
            inp = self.inputtxt_redis.get(1.0, "end-1c")
            res = redis_repository.execute_get_by_key(inp)
            self.duration_mongo.config(text="czas wykonania: " + str(redis_repository.select_durations[-1]))
            self.lbl_redis.insert(tk.END, str(res))

    def execute_delete(self):
        selected_db = self.get_current_tab_name()
        if selected_db == POSTGRESQL:
            inp = self.inputtxt_postgres.get(1.0, "end-1c")
            postgres_repository.execute_delete(inp)
            self.duration_mongo.config(text="czas wykonania: " + str(postgres_repository.delete_durations[-1]))
        elif selected_db == MONGODB:
            inp = self.inputtxt_mongo.get(1.0, "end-1c")
            mongo_repository.execute_delete(inp)
            self.duration_mongo.config(text="czas wykonania: " + str(mongo_repository.delete_durations[-1]))
        elif selected_db == REDIS:
            inp = self.inputtxt_redis.get(1.0, "end-1c")
            redis_repository.execute_delete(keys=inp)
            self.duration_mongo.config(text="czas wykonania: " + str(redis_repository.delete_durations[-1]))

    def execute_save(self):
        selected_db = self.get_current_tab_name()
        if selected_db == POSTGRESQL:
            inp = self.inputtxt_postgres.get(1.0, "end-1c")
            res = postgres_repository.execute_insert(inp)
            self.duration_postgres.config(text="czas wykonania: " + str(postgres_repository.insert_durations[-1]))
            self.lbl_postgres.insert(tk.END, str(res.head()))
        elif selected_db == MONGODB:
            inp = self.inputtxt_mongo.get(1.0, "end-1c")
            res = mongo_repository.execute_insert(inp)
            self.duration_mongo.config(text="czas wykonania: " + str(mongo_repository.insert_durations[-1]))
            self.lbl_mongo.insert(tk.END, str(res.head()))
        elif selected_db == REDIS:
            inp = self.inputtxt_redis.get(1.0, "end-1c")
            redis_repository.execute_insert(inp)
            self.duration_redis.config(text="czas wykonania: " + str(redis_repository.insert_durations[-1]))

    def execute_update(self):
        selected_db = self.get_current_tab_name()
        if selected_db == POSTGRESQL:
            inp = self.inputtxt_postgres.get(1.0, "end-1c")
            postgres_repository.execute_update(inp)
            self.duration_mongo.config(text="czas wykonania: " + str(postgres_repository.update_durations[-1]))
        elif selected_db == MONGODB:
            inp = self.inputtxt_mongo.get(1.0, "end-1c")
            mongo_repository.execute_update(inp)
            self.duration_postgres.config(text="czas wykonania: " + str(mongo_repository.update_durations[-1]))
        elif selected_db == REDIS:
            inp = self.inputtxt_redis.get(1.0, "end-1c")
            redis_repository.execute_update(inp)
            self.duration_redis.config(text="czas wykonania: " + str(redis_repository.update_durations[-1]))

    def execute_count(self):
        selected_db = self.get_current_tab_name()
        if selected_db == POSTGRESQL:
            inp = self.inputtxt_postgres.get(1.0, "end-1c")
            res = postgres_repository.execute_count(inp)
            self.duration_postgres.config(text="czas wykonania: " + str(postgres_repository.count_durations[-1]))
            self.print_count(self.lbl_postgres, res)
        elif selected_db == MONGODB:
            inp = self.inputtxt_mongo.get(1.0, "end-1c")
            res = mongo_repository.execute_count(inp)
            self.duration_mongo.config(text="czas wykonania: " + str(mongo_repository.count_durations[-1]))
            self.print_count(self.lbl_mongo, res)
        elif selected_db == REDIS:
            # FIXME:
            inp = self.inputtxt_redis.get(1.0, "end-1c")
            res = redis_repository.execute_count(inp)
            self.duration_redis.config(text="czas wykonania: " + str(redis_repository.count_durations[-1]))
            self.print_count(self.lbl_redis, res)
            print("update danych w Redis")

    def execute_word(self):
        selected_db = self.get_current_tab_name()
        if selected_db == POSTGRESQL:
            inp = self.inputtxt_postgres.get(1.0, "end-1c")
            res = postgres_repository.execute_word(inp)
            self.duration_postgres.config(text="czas wykonania: " + str(postgres_repository.word_durations[-1]))
            self.print_word(self.lbl_postgres, res)

            print("statystyki liczbowe w PostgreSQL")
        elif selected_db == MONGODB:
            inp = self.inputtxt_mongo.get(1.0, "end-1c")
            res = mongo_repository.execute_word(inp)
            self.duration_mongo.config(text="czas wykonania: " + str(mongo_repository.word_durations[-1]))
            self.print_word(self.lbl_mongo, res)
            print("update danych w MongoDB")
        elif selected_db == REDIS:
            # FIXME:
            inp = self.inputtxt_redis.get(1.0, "end-1c")
            res = redis_repository.execute_word(inp)
            self.duration_redis.config(text="czas wykonania: " + str(redis_repository.word_durations[-1]))
            self.print_word(self.lbl_redis, res)
            print("update danych w Redis")

    def execute_mean(self):
        selected_db = self.get_current_tab_name()
        if selected_db == POSTGRESQL:
            inp = self.inputtxt_postgres.get(1.0, "end-1c")
            mean, mode = postgres_repository.execute_mean(inp)
            self.duration_postgres.config(text="czas wykonania: " + str(postgres_repository.mean_durations[-1]))
            self.print_mean(self.lbl_postgres, mean, mode)
            print("statystyki liczbowe w PostgreSQL")
        elif selected_db == MONGODB:
            inp = self.inputtxt_mongo.get(1.0, "end-1c")
            mean, mode = mongo_repository.execute_mean(inp)
            self.duration_mongo.config(text="czas wykonania: " + str(mongo_repository.mean_durations[-1]))
            self.print_mean(self.lbl_mongo, mean, mode)
        elif selected_db == REDIS:
            # FIXME
            inp = self.inputtxt_redis.get(1.0, "end-1c")
            mean, mode = redis_repository.execute_mean(inp)
            self.duration_redis.config(text="czas wykonania: " + str(redis_repository.mean_durations[-1]))
            self.print_mean(self.lbl_mongo, mean, mode)
            print("update danych w Redis")

    def print_word(self, label: ScrolledText, res):
        label.insert(tk.END, "liczba wystąpien " + str(res))

    def print_mean(self, label: ScrolledText, mean, mode):
        label.insert(tk.END, "srednie  " + str(mean) + "\nmediany " + str(mode))

    def print_count(self, label: ScrolledText, res):
        label.insert(tk.END, "liczba rekordow " + str(res))
