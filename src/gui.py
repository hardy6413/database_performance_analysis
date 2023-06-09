import tkinter as tk
from tkinter.scrolledtext import ScrolledText

import pandas as pd

from src.constants import POSTGRESQL, MONGODB, REDIS, FILEPATH
from src.repositories import redis_repository, mongo_repository, postgres_repository


pd.set_option('display.max_columns', 5)


class DatabaseOperationsApp:
    def __init__(self):
        self.window = tk.Tk()
        self.window.title("Database Operations Analysis")
        self.window.geometry("450x500")

        self.db_selection_label = tk.Label(self.window, text="Wybierz bazę danych:")
        self.db_selection_label.pack()

        self.db_selection = tk.StringVar()
        self.db_selection.set(POSTGRESQL)

        self.db_dropdown = tk.OptionMenu(self.window, self.db_selection, POSTGRESQL, MONGODB, REDIS)
        self.db_dropdown.pack(pady=10)

        # self.btn_search = tk.Button(self.window, text="Wyszukaj", command=self.search_data)
        # self.btn_search.pack(pady=5)

        self.btn_search = tk.Button(self.window, text="Wyczyść baze", command=self.clear_data)
        self.btn_search.pack(pady=5)

        self.btn_read = tk.Button(self.window, text="Załaduj dane", command=self.read_data)
        self.btn_read.pack(pady=5)

        # self.btn_save = tk.Button(self.window, text="Zapisz", command=self.save_data)
        # self.btn_save.pack(pady=5)

        # self.btn_delete = tk.Button(self.window, text="Usuń", command=self.delete_data)
        # self.btn_delete.pack(pady=5)

        # self.btn_modify = tk.Button(self.window, text="Modyfikuj", command=self.modify_data)
        # self.btn_modify.pack(pady=5)

        self.inputtxt = tk.Text(self.window, height=5, width=20)
        self.inputtxt.pack()

        self.execute = tk.Button(self.window, text="wykonaj query", command=self.execute_query)
        self.execute.pack(pady=5)

        self.execute_delete_button = tk.Button(self.window, text="wykonaj usunięcie", command=self.execute_delete)
        self.execute_delete_button.pack(pady=5)

        self.execute_update_button = tk.Button(self.window, text="wykonaj update", command=self.execute_update)
        self.execute_update_button.pack(pady=5)

        self.execute_count_button = tk.Button(self.window, text="statystyki liczbowe", command=self.execute_count)
        self.execute_count_button.pack(pady=5)

        self.execute_word_button = tk.Button(self.window, text="liczba wystąpień słów", command=self.execute_word)
        self.execute_word_button.pack(pady=5)

        self.execute_mean_button = tk.Button(self.window, text="średnia i mediana", command=self.execute_mean)
        self.execute_mean_button.pack(pady=5)

        self.duration = tk.Label(self.window, text="czas wykonania:")
        self.duration.pack()

        self.lbl = ScrolledText(self.window, wrap=tk.WORD)
        self.lbl.pack()


    def run(self):
        self.window.mainloop()

    def search_data(self):
        selected_db = self.db_selection.get()

        if selected_db == POSTGRESQL:
            print("Wyszukiwanie danych w PostgreSQL")
        elif selected_db == MONGODB:
            print("Wyszukiwanie danych w MongoDB")
        elif selected_db == REDIS:
            print("Wyszukiwanie danych w Redis")

    def read_data(self):
        selected_db = self.db_selection.get()
        data = pd.read_csv(FILEPATH, low_memory=False)
        if selected_db == POSTGRESQL:
            postgres_repository.initialize_postgres(data)
            print("Odczyt danych z PostgreSQL")
        elif selected_db == MONGODB:
            mongo_repository.initialize_mongo(data)
            print("Odczyt danych z MongoDB")
        elif selected_db == REDIS:
            redis_repository.initialize_redis(data)
            print("Odczyt danych z Redis")

    def execute_query(self):
        selected_db = self.db_selection.get()
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

    def execute_delete(self):
        selected_db = self.db_selection.get()
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

    def execute_update(self):
        selected_db = self.db_selection.get()
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

    def clear_data(self):
        selected_db = self.db_selection.get()
        if selected_db == POSTGRESQL:
            postgres_repository.delete_all()
            print("czyszczenie danych z Postgresql")
        elif selected_db == MONGODB:
            mongo_repository.delete_all()
            print("czyszczenie danych z MongoDB")
        elif selected_db == REDIS:
            redis_repository.delete_all()
            print("czyszczenie danych z Redis")



    def execute_count(self):
        selected_db = self.db_selection.get()
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


    def execute_word(self):
        selected_db = self.db_selection.get()
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


    def execute_mean(self):
        selected_db = self.db_selection.get()
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
