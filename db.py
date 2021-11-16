import os
import psycopg2


pg_creds = {
    'host': '192.168.0.107'
    , 'port': '5432'
    , 'database': 'dshop'
    , 'user': 'pguser'
    , 'password': 'secret'
}

def read_from_db():

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        cursor.execute("select table_name from information_schema.tables where table_schema='public'")
        result = cursor.fetchall()
        tables = [row[0] for row in result]

    for i in tables:
        with psycopg2.connect(**pg_creds) as pg_connection:
            cursor = pg_connection.cursor()
            with open(file=os.path.join(os.getcwd(), f'{i}.csv'), mode='w',encoding='utf-8') as csv_file:
                cursor.copy_expert(f'COPY public.{str(i)} TO STDOUT WITH HEADER CSV', csv_file)


if __name__ == '__main__':
    read_from_db()