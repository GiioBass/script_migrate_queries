from tracemalloc import start
import pymysql
import re
from datetime import date, datetime

# Configuración de la BD
DB_CONFIG = {
    "host": "grownet-db.cdy2i6aa4vms.eu-west-2.rds.amazonaws.com",
    "user": "admin",       # cambia por tu usuario
    "password": "CYiHJfYzeLwa8oJwgSeA",   # cambia por tu password
    "database": "copy"  # cambia por tu base de datos
}

# Nombre de la tabla destino
TABLE_NAME = "polog_wholesalers"
# Archivo de log
ERROR_LOG = "errores.txt"

def extract_rows(block: str):
    """Extrae registros ( ... ) respetando paréntesis dentro de strings"""
    rows = []
    current = []
    paren_count = 0
    in_string = False

    for ch in block:
        if ch == "'" and (not current or current[-1] != "\\"):  
            # toggle estado de string
            in_string = not in_string

        if not in_string:
            if ch == "(":
                if paren_count == 0:
                    current = []
                paren_count += 1
            elif ch == ")":
                paren_count -= 1
                if paren_count == 0:
                    current.append(ch)
                    rows.append("".join(current)[1:-1])  # quita paréntesis externos
                    continue

        if paren_count > 0:
            current.append(ch)

    return rows

def parse_sql_file(file_path):
    """Parse the SQL-like file and extract tuples of data"""
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Dividir por los bloques "@"
    blocks = content.split("@")

    records = []
    for block in blocks:
        # Buscar todas las ocurrencias de "( ... )"
        matches = extract_rows(block)
        for match in matches:
            values = parse_values(match)
            records.append(values)
    return records


def parse_values(row_str):
    """Parse a row string into Python values"""
    parts = re.split(r",(?![^']*'\s*,)", row_str)

    parsed = []
    for p in parts:
        p = p.strip()
        if p.upper() == "NULL":
            parsed.append(None)
        elif p.startswith("'") and p.endswith("'"):
            parsed.append(p[1:-1])  # quitar comillas
        elif p.isdigit():
            parsed.append(int(p))
        else:
            try:
                parsed.append(float(p))
            except ValueError:
                parsed.append(p)
    return tuple(parsed)


def log_error(record_number, record, error):
    """Save error details in a log file"""
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] Error en registro #{record_number}\n")
        f.write(f"   Registro: {record}\n")
        f.write(f"   Error: {error}\n\n")


def insert_records(records):
    """Insert records one by one and log errors"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    skipIndex = []  # 👉 ejemplo: omitir siempre el valor en la posición 5 (0-based)

    # Ajusta la cantidad de placeholders según tus columnas reales
    # Escribe aquí las columnas en el mismo orden que vienen en el archivo
    COLUMNS = [
        "id", "ordered", "note", "cost_unit", "conversion_unit_id", "units", "fluid", "weight", "created_at", "updated_at", "purchase_order_id", "products_supplier_id", "emailed", "status_polog_id", "user_reception_id", "quantity_packing", "evidence_image", "final_cost", "tax", "evidence_note", "master_product_id", "conversion_fact_id", "date_reception"
    ]

    placeholders = ", ".join(["%s"] * len(COLUMNS))
    columns_str = ", ".join(COLUMNS)
    query = f"INSERT INTO {TABLE_NAME} ({columns_str}) VALUES ({placeholders})"

    success, errors, duplicates = 0, 0, 0

    for i, record in enumerate(records, start=1):
        print(f"Registro {i} → {len(record)} valores")
        record = tuple(v for j, v in enumerate(record) if j not in skipIndex)
        try:
            cursor.execute(query, record)
            conn.commit()
            success += 1
        except pymysql.err.IntegrityError as e:
            if e.args[0] == 1062:  # Duplicate entry
                print(f"⚠️ Registro duplicado ignorado #{i}: {record[0]}")
                print(f"   → {e}")
                duplicates += 1
                conn.rollback()
                continue
            else:
                print(f"❌ Error en registro #{i}: {record}")
                print(f"   → {e}")
                log_error(i, record, e)
                
                with open("failed_queries.sql", "a", encoding="utf-8") as f:
                    raw_query = query % tuple([repr(v) for v in record])
                    f.write(raw_query + ";\n")
                
                errors += 1
                conn.rollback()
        except Exception as e:
            print(f"❌ Error en registro #{i}: {record}")
            print(f"   → {e}")
            log_error(i, record, e)
            
              # Guardar consulta fallida en archivo .sql
            with open("failed_queries.sql", "a", encoding="utf-8") as f:
                raw_query = query % tuple([repr(v) for v in record])
                f.write(raw_query + ";\n")
            
            errors += 1
            conn.rollback()
            
    print(f"✅ {success} registros insertados correctamente.")
    print(f"⚠️ {errors} registros fallaron. Revisa el archivo {ERROR_LOG}")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    file_path = "values_clean.sql"  # Cambia por la ruta real
    records = parse_sql_file(file_path)

    print(f"Se encontraron {len(records)} registros.")
    insert_records(records)
