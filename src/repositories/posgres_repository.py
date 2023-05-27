from src.database_connection import get_alchemy_engine, get_postgres_connection

postgres_conn = get_postgres_connection()
alchemy_conn = get_alchemy_engine()


def initialize_postgres(data):
    data.to_sql('footballers', if_exists='append', index=False, con=alchemy_conn)

