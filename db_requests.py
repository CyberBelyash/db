import psycopg2
from psycopg2 import OperationalError

DB_CONFIG = {
    "database": "postgres",
    "user": "postgres",
    "password": "123456789",
    "host": "localhost",
    "port": "5432"
}

def create_tables():
    """Создание таблиц в базе данных"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Создание таблицы worker
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.worker (
                workercode BIGINT PRIMARY KEY,
                name VARCHAR NOT NULL,
                middlename VARCHAR NOT NULL,
                lastname VARCHAR NOT NULL,
                department VARCHAR NOT NULL
            );
        """)
        
        # Создание таблицы payslip
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.payslip (
                slip_id BIGINT PRIMARY KEY,
                workercode BIGINT REFERENCES worker(workercode),
                slip_date DATE NOT NULL
            );
        """)
        
        # Создание таблицы payslip_item
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.payslip_item (
                item_id SERIAL PRIMARY KEY,
                slip_id BIGINT REFERENCES payslip(slip_id),
                nomination VARCHAR NOT NULL,
                amount NUMERIC NOT NULL,
                quantity BIGINT DEFAULT 1,
                description VARCHAR NOT NULL
            );
        """)
        
        conn.commit()
        print("Таблицы успешно созданы!")
        
    except OperationalError as e:
        print(f"Ошибка подключения: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

def insert_test_data():
    """Добавление тестовых данных"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        
        cur.execute("""
            INSERT INTO worker (workercode, name, middlename, lastname, department) 
            VALUES 
                (444, 'Николай', 'Николаевич', 'Николаев', 'Отдел продаж'),
                (1, 'Иван', 'Иванович', 'Иванов', 'IT');
        """)
        
        
        cur.execute("""
            INSERT INTO payslip (slip_id, workercode, slip_date)
            VALUES (678, 444, '2024-12-25');
        """)
        
        
        cur.executemany("""
            INSERT INTO payslip_item (slip_id, nomination, amount, quantity, description)
            VALUES (%s, %s, %s, %s, %s)
        """, [
            (678, 'Оклад', 60000.00, 1, 'Основная зарплата'),
            (678, 'Премия', 15000.00, 1, 'Премия за выполнение плана'),
            (678, 'НДФЛ', -9750.00, 1, 'Налог на доходы физических лиц'),
            (678, 'Итого', 65250.00, 1, 'Итоговая сумма к выплате')
        ])
        
        conn.commit()
        print("Тестовые данные добавлены!")
        
    except Exception as e:
        print(f"Ошибка вставки данных: {e}")
        conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def get_payslip(slip_id: int):
    """Получение расчетного листка по ID"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Получение заголовка
        cur.execute("""
            SELECT w.*, p.slip_date 
            FROM payslip p
            JOIN worker w ON p.workercode = w.workercode
            WHERE p.slip_id = %s
        """, (slip_id,))
        
        header = cur.fetchone()
        
        if not header:
            print(f"Листок №{slip_id} не найден")
            return

        
        cur.execute("""
            SELECT nomination, amount, quantity, description 
            FROM payslip_item 
            WHERE slip_id = %s
            ORDER BY item_id
        """, (slip_id,))
        
        items = cur.fetchall()

        
        print(f"\nРасчетный листок № {slip_id}")
        print(f"Сотрудник: {header[3]} {header[1]} {header[2]}")
        print(f"Отдел: {header[4]}")
        print(f"Дата: {header[5]}")
        
        print("\n| Наименование | Сумма | Количество | Описание |")
        print("|--------------|-------|------------|----------|")
        for item in items:
            print(f"| {item[0]:12} | {item[1]:>7} | {item[2]:>10} | {item[3]:30} |")
        
    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    create_tables()
    insert_test_data()
    get_payslip(678)