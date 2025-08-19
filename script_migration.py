import pymysql
import re
from datetime import datetime

# Configuración de la BD
DB_CONFIG = {
    "host": "grownet-db.cdy2i6aa4vms.eu-west-2.rds.amazonaws.com",
    "user": "admin",       # cambia por tu usuario
    "password": "CYiHJfYzeLwa8oJwgSeA",   # cambia por tu password
    "database": "copy"  # cambia por tu base de datos
}

# Nombre de la tabla destino
TABLE_NAME = "products_suppliers"
# Archivo de log
ERROR_LOG = "errores.txt"


def parse_sql_file(file_path):
    """Parse the SQL-like file and extract tuples of data"""
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Dividir por los bloques "@"
    blocks = content.split("@")

    records = []
    for block in blocks:
        # Buscar todas las ocurrencias de "( ... )"
        matches = re.findall(r"\((.*?)\)", block, re.DOTALL)
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

    # Ajusta la cantidad de placeholders según tus columnas reales
    # Escribe aquí las columnas en el mismo orden que vienen en el archivo
    COLUMNS = [
        "id","name_wh","code","units","fluids","weight","is_parent","short_flag","state","is_BOM","type_id","master_product_id","is_hidden_purchase","units_of_sale_id","type_detail_id","location_warehouse_id","product_note","show_in_sales","is_preloaded"
    ]

    placeholders = ", ".join(["%s"] * len(COLUMNS))
    columns_str = ", ".join(COLUMNS)
    query = f"INSERT INTO {TABLE_NAME} ({columns_str}) VALUES ({placeholders})"

    success, errors, duplicates = 0, 0, 0

    for i, record in enumerate(records, start=1):
        print(f"Registro {i} → {len(record)} valores")
        try:
            cursor.execute(query, record)
            conn.commit()
            success += 1
        except pymysql.err.IntegrityError as e:
            if e.args[0] == 1062:  # Duplicate entry
                print(f"⚠️ Registro duplicado ignorado #{i}: {record[0]}")
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
    file_path = "queries.sql"  # Cambia por la ruta real
    records = parse_sql_file(file_path)

    print(f"Se encontraron {len(records)} registros.")
    insert_records(records)
