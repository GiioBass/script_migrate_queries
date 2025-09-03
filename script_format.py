import re

INPUT_FILE = "details_po.sql"     # cambia por tu archivo real
OUTPUT_FILE = "values_clean.sql"

def extract_values():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        content = f.read()

    # Buscar bloques que comienzan con VALUES y terminan en ;
    matches = re.findall(r"VALUES\s*(.*?)\s*;", content, re.DOTALL | re.IGNORECASE)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for block in matches:
            block = block.strip()
            if block:
                # Agregar @ al inicio del bloque
                out.write(f"@{block};\n\n")

    print(f"✅ Valores extraídos en {OUTPUT_FILE}")

if __name__ == "__main__":
    extract_values()
