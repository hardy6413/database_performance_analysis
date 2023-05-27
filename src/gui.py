import tkinter as tk
from src.constants import POSTGRESQL, MONGODB, REDIS, FILEPATH
import pandas as pd

from src.repositories import redis_repository, mongo_repository, postgres_repository


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

        self.btn_search = tk.Button(self.window, text="Wyszukaj", command=self.search_data)
        self.btn_search.pack(pady=5)

        self.btn_search = tk.Button(self.window, text="Wyczyść baze", command=self.clear_data)
        self.btn_search.pack(pady=5)

        self.btn_read = tk.Button(self.window, text="Załaduj dane", command=self.read_data)
        self.btn_read.pack(pady=5)

        self.btn_save = tk.Button(self.window, text="Zapisz", command=self.save_data)
        self.btn_save.pack(pady=5)

        self.btn_delete = tk.Button(self.window, text="Usuń", command=self.delete_data)
        self.btn_delete.pack(pady=5)

        self.btn_modify = tk.Button(self.window, text="Modyfikuj", command=self.modify_data)
        self.btn_modify.pack(pady=5)

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


    def save_data(self):
        selected_db = self.db_selection.get()

        if selected_db == POSTGRESQL:
            print("Zapisywanie danych do PostgreSQL")
        elif selected_db == MONGODB:
            print("Zapisywanie danych do MongoDB")
        elif selected_db == REDIS:
            print("Zapisywanie danych do Redis")

    def delete_data(self):
        selected_db = self.db_selection.get()

        if selected_db == POSTGRESQL:
            print("Usuwanie danych z PostgreSQL")
        elif selected_db == MONGODB:
            print("Usuwanie danych z MongoDB")
        elif selected_db == REDIS:
            print("Usuwanie danych z Redis")

    def modify_data(self):
        selected_db = self.db_selection.get()

        if selected_db == POSTGRESQL:
            print("Modyfikacja danych w PostgreSQL")
        elif selected_db == MONGODB:
            print("Modyfikacja danych w MongoDB")
        elif selected_db == REDIS:
            print("Modyfikacja danych w Redis")

