import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

engine = None  # Variable global para el motor de la base de datos

def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()

        print("Conexión exitosa a la base de datos!")
        return engine
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def run_sql_file(engine, filepath):
    try:
        with open(filepath, "r") as file:
            sql_commands = file.read()

        with engine.connect() as conn:
            conn.execute(text(sql_commands))

        print(f"Ejecutado exitosamente: {filepath}")
    except Exception as e:
        print(f"Error al ejecutar {filepath}: {e}")

if __name__ == "__main__":
    # 1) Conectar
    engine = connect()

    if engine:
        # 2) Crear tablas
        run_sql_file(engine, "./src/sql/create.sql")

        # 3) Insertar datos
        run_sql_file(engine, "./src/sql/insert.sql")

        # 4) Mostrar datos con Pandas
        try:
            df = pd.read_sql("SELECT * FROM authors LIMIT 5;", engine)
            print(df)
        except Exception as e:
            print(f"Error al mostrar datos: {e}")