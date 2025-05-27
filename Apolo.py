# steamlit.py
import streamlit as st
import os
import zipfile
import re
import tempfile
import shutil
import subprocess
from pathlib import Path # Importamos Path para manejar rutas
import sys # Importar sys para sys.executable

# Configurar la página, incluyendo el favicon y el nuevo título.
# ¡Esta debe ser la PRIMERA llamada a una función de Streamlit!
st.set_page_config(page_title="Apolo", page_icon="logo.png")

# --- Configuración (copiada de revisar_archivos_v2.py) ---
VALID_EXTS = {'.sql', '.pks', '.pkb', '.prc', '.fnc', '.vw', '.trg', '.seq'}
# REPORT_DIR = 'reports' # No necesitamos un directorio fijo de reportes por ahora, lo mostraremos en la UI

# --- Funciones de utilidad (adaptadas de revisar_archivos_v2.py) ---

def extract_archive(archive_path, dest_dir):
    """Extrae archivos .zip a un directorio de destino."""
    ext = os.path.splitext(archive_path)[1].lower()
    if ext == '.zip':
        try:
            with zipfile.ZipFile(archive_path, 'r') as z:
                z.extractall(dest_dir)
        except zipfile.BadZipFile as e:
             raise ValueError(f"Archivo ZIP corrupto o no válido: {e}")
        except Exception as e:
             raise Exception(f"Ocurrió un error inesperado al procesar el archivo ZIP: {e}")

    else:
        raise ValueError(f"Tipo de archivo no soportado para extracción: se espera .zip")

def numeric_key(s):
    """Extrae el número inicial de una cadena para ordenamiento numérico."""
    m = re.match(r"(\d+)", s)
    return int(m.group(1)) if m else float('inf')

def collect_and_order_files(root_dir):
    """Recopila y ordena los archivos válidos (SOLO PARA ANÁLISIS) dentro de un directorio raíz."""
    folder_map = {}
    try:
        for dirpath, _, files in os.walk(root_dir):
            # Ignorar carpetas 'rollback'
            if "rollback" in os.path.basename(dirpath).lower():
                 continue

            rel_folder = os.path.relpath(dirpath, root_dir)
            # USAR VALID_EXTS AQUÍ SOLO PARA FILTRAR ARCHIVOS PARA EL ANÁLISIS
            valid = [f for f in files if os.path.splitext(f)[1].lower() in VALID_EXTS]
            if valid:
                folder_map[rel_folder] = sorted(valid, key=numeric_key)
        # Ordenar carpetas basándose en el primer número que aparezca en el nombre de la carpeta
        ordered_folders = sorted(folder_map.keys(), key=lambda x: numeric_key(os.path.basename(x)))

        # Crear una lista aplanada de archivos en orden de procesamiento (para el manifiesto, aunque aquí solo para el análisis)
        # Esto asegura que el orden del reporte coincida con el orden de ejecución/manifiesto
        ordered_files_list = [] # Esta lista solo contendrá archivos elegibles para VALID_EXTS
        for folder in ordered_folders:
            for filename in folder_map[folder]:
                ordered_files_list.append(os.path.join(folder, filename))

        return ordered_folders, folder_map, ordered_files_list # Retornamos la lista de archivos solo para análisis
    except Exception as e:
         st.error(f"Error inesperado al recopilar y ordenar archivos del directorio temporal para análisis: {e}")
         return [], {}, [] # Retornar listas vacías en caso de error


def check_slash_terminators(lines, ext):
    """Verifica la presencia de '/' después del *último* bloque PL/SQL END;."""
    slash_issues = []
    # Solo aplicar esta verificación a tipos de archivos que usan END; y requieren /
    # Excluimos .sql (puede tener múltiples sentencias sin /), .seq y .vw
    if ext not in ('.pks', '.pkb', '.prc', '.fnc', '.trg'):
        return slash_issues

    # Patrón para encontrar líneas que terminan con END; o END <palabra>;
    # Usamos '\s*$' para coincidir con cero o más espacios antes del final de la línea
    end_pattern = re.compile(r'END(\s+\w+)?;\s*$', re.IGNORECASE)

    last_end_index = -1
    # Buscar el último END; o END <palabra>; desde el final del archivo
    for i in range(len(lines) - 1, -1, -1):
        if end_pattern.search(lines[i]):
            last_end_index = i
            break # Encontramos el último, salimos del bucle

    # Si no se encontró ningún END; que cumpla el patrón, no aplicamos la validación del slash.
    if last_end_index == -1:
        return slash_issues

    # Si se encontró el último END;, verificamos lo que sigue
    j = last_end_index + 1 # Empezar a buscar desde la siguiente línea después del último END;

    # Saltar líneas en blanco y comentarios
    while j < len(lines) and (lines[j].strip() == "" or lines[j].strip().startswith('--') or lines[j].strip().startswith('/*')):
        j += 1

    # j ahora es el índice de la primera línea no en blanco y no comentario después del último END;
    # o j es len(lines) si solo había líneas en blanco/comentarios hasta el final del archivo.

    if j == len(lines):
        # Llegamos al final del archivo sin encontrar '/' o texto significativo
        slash_issues.append(f"Línea {last_end_index+1}: Falta '/' al final después del bloque END;.")
    elif lines[j].strip() != '/':
        # Encontramos una línea no en blanco y no comentario, pero no es '/'
        slash_issues.append(f"Línea {last_end_index+1}: Falta '/' al final después del bloque END;.")
    # Si lines[j].strip() == '/', significa que el '/' fue encontrado correctamente, no añadimos issue.

    return slash_issues


def analyze_file(path, ext):
    """Lee un archivo y verifica únicamente la presencia del slash final después del *último* END;."""
    # Solo analizamos extensiones relevantes que requieren este chequeo.
    if ext.lower() not in VALID_EXTS:
         return [] # No issues for non-DB script files

    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except Exception as e:
         return [f"Error al leer el archivo '{os.path.basename(path)}': {e}"] # Mensaje más específico

    # Realizar únicamente la verificación específica del slash después del último END;
    issues = check_slash_terminators(lines, ext)

    return issues


# --- Funciones para Generación de Manifiesto (Adaptadas de genera_manifest.py) ---

# Constantes para las extensiones y carpetas especiales
# ALLOWED_EXTENSIONS_MANIFEST ahora incluye .fmb y .rdf para la recolección general
ALLOWED_EXTENSIONS_MANIFEST = {".sql", ".pks", ".pkb", ".prc", ".fnc", ".trg", ".vw", ".fmb", ".rdf"}
# Carpetas consideradas "script-like" que irán a la sección de scripts con formato por carpeta
SQL_SPECIFIC_FOLDERS = {"scripts", "grants", "opciones", "indices", "tabla", "sequence"}


# Categorías para el manifiesto, definiendo encabezado y si usan formato por carpeta
# Las claves de este diccionario (scripts, packages, etc.) se usarán para determinar la 'carpeta_por_tipo_de_archivo' en la ruta del manifiesto.
# NOTA: .fmb y .rep NO se añaden aquí porque NO van en el manifest.txt principal.
MANIFEST_CATEGORIES = {
    # La categoría 'scripts' maneja archivos en carpetas específicas con cualquier extensión elegible Y archivos .sql no en carpetas específicas
    "scripts": {
        "header": "-- Ejecucion de scripts sql",
        "extensions": {".sql"}, # .sql archivos van a la sección scripts
        "specific_folders": SQL_SPECIFIC_FOLDERS, # Carpetas que definen esta categoría y el formato por carpeta
        "format_per_folder": True # Usa formato por carpeta
    },
    # Las siguientes categorías son para objetos PL/SQL, categorizados por extensión, NO en carpetas script-like
    "packages": {
        "header": "-- Ejecucion de script creacion de packages",
        "extensions": {".pks"},
        "specific_folders": set(),
        "format_per_folder": False # No usa formato por carpeta
    },
    "packagesbodies": { # Usamos 'packagesbodies' para coincidir con el ejemplo probable de manifiesto si hay .pkb
        "header": "-- Ejecucion de script creacion de packagesBodies",
        "extensions": {".pkb"},
        "specific_folders": set(),
        "format_per_folder": False
    },
    "procedures": {
        "header": "-- Ejecucion de script creacion de procedures",
        "extensions": {".prc"},
        "specific_folders": set(),
        "format_per_folder": False
    },
    "functions": {
        "header": "-- Ejecucion de script creacion de funciones",
        "extensions": {".fnc"},
        "specific_folders": set(),
        "format_per_folder": False
    },
    "views": {
        "header": "-- Ejecucion de script creacion de views",
        "extensions": {".vw"},
        "specific_folders": set(),
        "format_per_folder": False
    },
    "triggers": {
        "header": "-- Ejecucion de script creacion de triggers",
        "extensions": {".trg"},
        "specific_folders": set(),
        "format_per_folder": False
    }
    # NOTA: Si un archivo con extensión .pks, .pkb, etc. está en una carpeta SQL_SPECIFIC_FOLDERS,
    # get_manifest_category lo asignará a su categoría por extensión si no es .sql.
    # El formato por carpeta en generate_manifest_content manejará la ruta correcta.
}

def extract_prefix_number(filename_str):
    """Extrae el número inicial de un nombre de archivo."""
    match = re.match(r"(\d+)", filename_str)
    return int(match.group(1)) if match else float('inf')

def collect_files_for_manifest(root_dir: Path):
    """
    Navega recursivamente por el directorio raíz, filtra y ordena los archivos
    para la generación del manifiesto Y LA COPIA. Usa ALLOWED_EXTENSIONS_MANIFEST.
    Retorna una lista de diccionarios con datos de los archivos.
    """
    collected_files_data = []
    try:
        for dirpath, dirnames, filenames in os.walk(root_dir):
            current_path = Path(dirpath)

            # Ignorar carpetas 'rollback'
            if "rollback" in current_path.name.lower():
                dirnames[:] = [] # No descender en subdirectorios de 'rollback'
                continue

            for filename_str in filenames:
                file_path = current_path / filename_str
                file_ext = file_path.suffix.lower()

                # Usar ALLOWED_EXTENSIONS_MANIFEST para incluir todos los archivos relevantes
                if file_ext in ALLOWED_EXTENSIONS_MANIFEST:
                    # Usamos relative_to(root_dir) para obtener la ruta relativa desde la carpeta extraída
                    relative_path = file_path.relative_to(root_dir)
                    prefix_num = extract_prefix_number(filename_str)

                    collected_files_data.append({
                        "absolute_path": str(file_path), # Necesitamos la ruta absoluta para copiar el archivo después
                        "relative_path_from_extracted": str(relative_path.as_posix()), # Ruta relativa dentro del zip
                        "parent_folder_name": current_path.name, # Nombre de la carpeta inmediata del archivo extraído
                        "prefix_num": prefix_num,
                        "extension": file_ext,
                        "filename_str": filename_str
                    })

        # Ordenar la lista aplanada de archivos. Esto es crucial para el orden general del manifiesto Y LA VISUALIZACIÓN.
        # El ordenamiento se basa primero en la ruta relativa (para agrupar archivos de la misma subcarpeta del zip),
        # luego por número de prefijo, y finalmente por nombre de archivo.
        # ESTE ORDENAMIENTO ES IMPORTANTE PARA RESPETAR EL ORDEN DE LAS CARPETAS DEL ZIP.
        collected_files_data.sort(key=lambda x: (x["relative_path_from_extracted"], x.get("prefix_num", float('inf')), x["filename_str"]))

        return collected_files_data
    except Exception as e:
         st.error(f"Error inesperado al recopilar archivos para el manifiesto y copiado desde '{root_dir}': {e}")
         return []


def get_manifest_category(file_data, manifest_categories, script_like_folders):
    """
    Determina la clave de categoría del manifiesto para un archivo dado.
    La categorización influye en el formato del manifiesto (por carpeta o no)
    y la construcción de la ruta en el manifiesto.
    Archivos como .fmb y .rep, que no están en las categorías de scripts DB,
    retornarán None y no serán incluidos en el manifest.txt principal.
    """
    file_ext = file_data["extension"].lower()
    parent_folder_name_lower = file_data["parent_folder_name"].lower()

    # Determinar si la carpeta inmediata o cualquier carpeta padre contiene una palabra clave de carpeta "script-like"
    # Iteramos sobre las partes de la ruta relativa al zip para ser más robustos
    relative_path_parts = Path(file_data["relative_path_from_extracted"]).parts
    is_in_script_like_folder = any(keyword.lower() in part.lower() for part in relative_path_parts for keyword in script_like_folders)


    # Regla de Categorización:
    # Iterar a través de las categorías definidas en MANIFEST_CATEGORIES.
    for category_key, details in manifest_categories.items():
        # Si la extensión del archivo coincide con las extensiones de esta categoría
        if file_ext in details["extensions"]:
            # Si la categoría es 'scripts' (solo .sql), verificamos si está en carpeta script-like
            if category_key == "scripts":
                # Un archivo .sql va a la sección scripts si está en una carpeta script-like O si no está en una carpeta script-like.
                # La diferencia es solo en el formato (por carpeta vs lista plana) manejada en generate_manifest_content.
                # Aquí solo determinamos la *categoría*.
                return "scripts"
            else:
                 # Para otras categorías (paquetes, procedures, etc.), el archivo debe *no* estar en una carpeta script-like
                 # O si está en una carpeta script-like, debe ser una extensión que se categoriza por tipo (pks, pkb, etc.).
                 # La lógica original parece compleja. Simplificamos: Si la extensión coincide con la categoría,
                 # y no es una extensión que DEBE ir a 'scripts' (actualmente solo .sql) si está en una carpeta script-like.
                 # Revertimos a una lógica más simple: si la extensión coincide con la categoría, asignarla.
                 # La función generate_manifest_content se encarga de la estructura de directorios.
                 # El único caso especial es .sql en carpetas script-like vs .sql fuera.
                 # La definición de MANIFEST_CATEGORIES y el manejo en generate_manifest_content
                 # ya deberían dirigir correctamente .sql en carpetas script-like a la sección 'scripts'
                 # y .sql fuera a la sección 'scripts' también, pero sin formato por carpeta.
                 # Mantengamos la lógica original que busca la extensión en las categorías.
                 return category_key

    # Si no se categoriza por las reglas anteriores (extensión no permitida en MANIFEST_CATEGORIES,
    # como .fmb, .rep, o cualquier otra no listada)
    return None


def generate_manifest_content(schema_name: str, branch_name: str, all_files_data: list, script_like_folders: set, manifest_categories: dict):
    """
    Genera el contenido del archivo manifest.txt (solo para scripts DB) respetando el orden de las carpetas originales del zip.
    Añade salto de línea y encabezado antes de listar archivos de cada carpeta.
    Construye rutas con la base 'database/plsql/{esquema_en_minusculas}/{carpeta_por_tipo_de_archivo}/'.
    Asegura que .pks va antes que .pkb dentro de cada carpeta original.
    Este manifiesto solo incluye archivos categorizados en MANIFEST_CATEGORIES (scripts DB).
    """
    content_lines = []
    schema_name_lower = schema_name.lower() # Esquema en minúsculas para la ruta
    branch_name_upper = branch_name.upper() # La rama se usa en la ruta base de la sección de scripts

    content_lines.append(f"SCHEMA={schema_name.upper()}") # SCHEMA= debe seguir usando mayúsculas según ejemplo
    content_lines.append("") # Línea en blanco después del encabezado

    # Agrupar archivos por su carpeta original relativa al zip Y por categoría de manifiesto
    # Solo incluiremos archivos que tienen una categoría de manifiesto válida (no None)
    files_by_original_folder_and_category = {}
    for file_data in all_files_data:
        category_key = get_manifest_category(file_data, manifest_categories, script_like_folders)
        if category_key: # Solo procesar archivos que fueron categorizados para el manifiesto de scripts DB
            original_folder_relative_to_zip = Path(file_data["relative_path_from_extracted"]).parent.as_posix()
            if original_folder_relative_to_zip not in files_by_original_folder_and_category:
                files_by_original_folder_and_category[original_folder_relative_to_zip] = {}
            if category_key not in files_by_original_folder_and_category[original_folder_relative_to_zip]:
                 files_by_original_folder_and_category[original_folder_relative_to_zip][category_key] = []
            files_by_original_folder_and_category[original_folder_relative_to_zip][category_key].append(file_data)


    # Ordenar las carpetas originales basadas en sus nombres (usando numeric_key)
    sorted_original_folders = sorted(files_by_original_folder_and_category.keys(), key=lambda x: numeric_key(Path(x).name))

    is_first_folder_block = True # Flag para blank lines entre bloques de carpeta

    # Iterar a través de las carpetas originales ordenadas
    for original_folder_relative_to_zip in sorted_original_folders:
        files_by_manifest_category_in_folder = files_by_original_folder_and_category[original_folder_relative_to_zip]


        # Escribir el contenido para esta carpeta original, categoría por categoría, en el orden definido
        # Solo añadiremos un salto de línea antes del *primer* bloque de categoría dentro de esta carpeta,
        # si no es la primera carpeta general.
        added_first_category_header_in_folder = False

        # Iterar a través de las categorías del manifiesto en su orden definedo
        for category_key, details in manifest_categories.items():
            files_in_this_category_and_folder = files_by_manifest_category_in_folder.get(category_key, [])

            if files_in_this_category_and_folder: # Si hay archivos para esta categoría en esta carpeta
                # Add blank line before this category block IF it's not the very first category block overall
                # AND it's the first category block within this specific original folder
                if is_first_folder_block:
                    pass # No blank line before the very first block overall
                elif not added_first_category_header_in_folder:
                    content_lines.append("") # Add a blank line before the first category block in this folder

                # Add header for this category (repeated for each folder block where this category has files)
                content_lines.append(details["header"])
                added_first_category_header_in_folder = True # Marcar que ya añadimos al menos un encabezado en esta carpeta

                # Sort files within this category and folder. Special handling for packages/package bodies.
                if category_key == "packages" or category_key == "packagesbodies":
                    # Sort .pks before .pkb for these categories.
                    # Key: True for .pkb, False for .pks. False sorts before True.
                    # Then by prefix number, then filename.
                    sorted_files_in_category_and_folder = sorted(files_in_this_category_and_folder,
                                                                 key=lambda x: (x["extension"].lower() != ".pks", x.get("prefix_num", float('inf')), x["filename_str"]))
                else:
                    # Standard sorting by prefix number and filename
                    sorted_files_in_category_and_folder = sorted(files_in_this_category_and_folder,
                                                                 key=lambda x: (x.get("prefix_num", float('inf')), x["filename_str"]))

                # Escribir cada línea de archivo para esta categoría y carpeta
                for file_data in sorted_files_in_category_and_folder:
                    filename = file_data["filename_str"]
                    file_ext = file_data["extension"].lower()

                    # Determine the type folder name in the manifest path based on category key
                    # This matches the folder structure used when copying files for DB scripts
                    type_folder_name_in_manifest = category_key.lower() # Use the category key lowercase as the type folder name

                    # Path construction: database/plsql/{schema_lower}/{type_folder_name_in_manifest}/{filename}
                    manifest_file_path = Path("database", "plsql", schema_name_lower, type_folder_name_in_manifest, filename).as_posix()
                    manifest_line = f"{manifest_file_path}"
                    content_lines.append(manifest_line)


        is_first_folder_block = False # Marcar que al menos un bloque de carpeta ha sido procesado

    return "\n".join(content_lines) # Usar el carácter de salto de línea real


# --- Funciones para Operaciones Git (Adaptadas de create_branch.py) ---

def run_git_command(repo_path, command, cwd=None, suppress_errors=False):
    """Ejecuta un comando Git usando subprocess. Muestra errores a menos que suppress_errors=True."""
    # Usamos shell=True para que funcione en Windows si git no está en el PATH del entorno de Streamlit
    # Pero shell=True con listas de comandos puede ser peligroso si los inputs no son confiables.
    # Dado que el repo_path y branch_name vienen de text_inputs, debemos ser cuidadosos.
    # Es más seguro pasar el comando como una cadena única con shell=True.
    # Asegurarse de que 'git' está en el PATH del sistema donde se ejecuta Streamlit.
    full_command = ["git"] + command # Construir la lista de comandos primero
    try:
        # Si cwd is None, subprocess se ejecuta en el directorio actual del script (no deseado)
        # Si cwd está especificado, se ejecuta en ese directorio (deseado)
        result = subprocess.run(full_command, check=True, capture_output=True, text=True, cwd=repo_path, shell=False) # shell=False es más seguro con listas
        st.text(result.stdout.strip()) # Mostrar stdout sin excesivos espacios
        if result.stderr:
             st.text(result.stderr.strip()) # Mostrar stderr sin excesivos espacios
        return True
    except FileNotFoundError:
        st.error(f"Error: El comando 'git' no fue encontrado. Asegúrate de que Git está instalado y en el PATH.")
        return False
    except subprocess.CalledProcessError as e:
        if not suppress_errors:
            st.error(f"Error ejecutando comando Git: {' '.join(full_command)}")
            st.error(f"Código de retorno: {e.returncode}")
            st.error(f"Salida estándar:\n{e.stdout.strip()}")
            st.error(f"Salida de error:\n{e.stderr.strip()}")
        return False
    except Exception as e:
         if not suppress_errors:
              st.error(f"Ocurrió un error inesperado al ejecutar un comando Git: {e}")
         return False

def check_branch_exists(repo_path, branch_name):
    """Verifica si una rama existe en el repositorio local sin imprimir errores si no existe."""
    # run_git_command(..., suppress_errors=True) para no mostrar el error si rev-parse falla (rama no existe)
    # git rev-parse --verify HEAD produce 0 si existe, 1 si no
    try:
         subprocess.run(["git", "rev-parse", "--verify", branch_name], check=True, capture_output=True, text=True, cwd=repo_path, shell=False)
         return True # Si check=True no lanza excepción, la rama existe
    except subprocess.CalledProcessError:
         return False # Si check=True lanza CalledProcessError, la rama NO existe
    except FileNotFoundError:
         st.error("Error: El comando 'git' no fue encontrado al verificar la existencia de la rama.")
         return False
    except Exception as e:
         st.error(f"Ocurrió un error inesperado al verificar la existencia de la rama '{branch_name}': {e}")
         return False


def create_and_checkout_branch(repo_path, branch_name):
    """Crea y cambia a una nueva rama en el repositorio local."""
    st.info(f"🔄 Cambiando a la rama 'main' y haciendo pull...")
    if not run_git_command(repo_path, ["checkout", "main"]):
        st.error("Fallo al cambiar a la rama 'main'.")
        return False
    if not run_git_command(repo_path, ["pull"]):
         st.error("Fallo al hacer pull en la rama 'main'.")
         return False

    st.info("🧹 Limpiando archivos no rastreados...")
    # Agregar el comando git clean -fdx
    if not run_git_command(repo_path, ["clean", "-fdx"]):
        st.warning("Falló la limpieza de archivos no rastreados. Esto podría deberse a permisos o archivos en uso, pero intentaremos continuar.")
        # No retornamos False aquí para permitir que el proceso continúe incluso si la limpieza falla

    # Usar la nueva función check_branch_exists para verificar si la rama ya existe
    if check_branch_exists(repo_path, branch_name):
        st.warning(f"La rama '{branch_name}' ya existe. Cambiando a ella en lugar de crearla.")
        # Si la rama existe, simplemente hacemos checkout
        if not run_git_command(repo_path, ["checkout", branch_name]):
             st.error(f"Fallo al cambiar a la rama existente '{branch_name}'.")
             return False
    else:
        st.info(f"🌿 Creando y cambiando a la nueva rama '{branch_name}'...")
        # Si la rama no existe, la creamos y hacemos checkout
        if not run_git_command(repo_path, ["checkout", "-b", branch_name]):
             st.error(f"Fallo al crear y cambiar a la nueva rama '{branch_name}'.")
             return False

    st.success(f"✅ Rama '{branch_name}' seleccionada exitosamente.")
    return True


# --- Funciones para Copiar Archivos y Generar Manifiesto ---

def copy_extracted_files_to_repo(temp_dir: str, repo_path: str, schema_name: str, files_data: list):
    """
    Copia los archivos extraídos del directorio temporal al repositorio local
    siguiendo la estructura de carpetas definida para los diferentes tipos de archivos.
    """
    st.info(f"📋 Copiando archivos al repositorio local en: {repo_path}")
    schema_lower = schema_name.lower()
    copied_count = 0
    try:
        for file_data in files_data:
            src_path = Path(file_data["absolute_path"])
            file_ext = file_data["extension"].lower() # Obtener la extensión

            # Determinar la ruta de destino basada en la extensión
            dest_base_dir = Path(repo_path) # Base del repositorio
            dest_relative_path = None # Ruta relativa dentro del repositorio

            if file_ext in {'.sql', '.pks', '.pkb', '.prc', '.fnc', '.trg', '.vw'}:
                 # Scripts DB van a la estructura database/plsql
                 # Determinar la carpeta de tipo de archivo DENTRO de database/plsql
                 dest_type_folder = ""
                 if file_ext == ".sql":
                     dest_type_folder = "scripts"
                 elif file_ext == ".pks":
                     dest_type_folder = "packages"
                 elif file_ext == ".pkb":
                     dest_type_folder = "packagesbodies"
                 elif file_ext == ".prc":
                     dest_type_folder = "procedures"
                 elif file_ext == ".fnc":
                     dest_type_folder = "functions"
                 elif file_ext == ".trg":
                     dest_type_folder = "triggers"
                 elif file_ext == ".vw":
                     dest_type_folder = "views"

                 if dest_type_folder:
                      dest_relative_path = Path("database", "plsql", schema_lower, dest_type_folder, src_path.name)

            # Manejar extensiones .fmb y .rdf (antes .rep)
            elif file_ext == '.fmb':
                 dest_relative_path = Path("fuentes", "forma", src_path.name)
            elif file_ext == '.rdf':
                 dest_relative_path = Path("fuentes", "reporte", src_path.name)


            # Si se determinó una ruta de destino
            if dest_relative_path:
                 dest_full_path = dest_base_dir / dest_relative_path
                 dest_dir = dest_full_path.parent

                 # Crear el directorio de destino si no existe
                 dest_dir.mkdir(parents=True, exist_ok=True)

                 # Copiar el archivo
                 shutil.copy2(src_path, dest_full_path) # copy2 intenta preservar metadatos
                 copied_count += 1
            else:
                 # Este caso no debería ocurrir si collect_files_for_manifest filtra correctamente,
                 # o si hemos definido la lógica de copiado para todas las extensiones permitidas.
                 # Sin embargo, es una buena práctica defensiva.
                 st.warning(f"Archivo '{file_data['relative_path_from_extracted']}' con extensión '{file_ext}' no tiene una carpeta de destino definida en la lógica de copiado, no será copiado.")


        st.success(f"✅ {copied_count} archivos copiados exitosamente al repositorio local.")
        return True
    except Exception as e:
        st.error(f"❌ Error inesperado al copiar archivos al repositorio: {e}") # Mensaje más específico
        return False


def generate_and_write_manifest(repo_path: str, branch_name: str, schema_name: str, files_data: list):
    """
    Genera el contenido del manifest.txt (solo para scripts DB) y lo escribe en la ubicación correcta dentro del branch.
    """
    try:
        # La ruta donde se guarda el manifest.txt (siempre bajo database/data/<schema>/<branch>)
        # Usamos schema_name (puede ser mayúsculas si vino así del selectbox, aunque en la ruta se usa minúsculas)
        # Usamos branch_name_upper (convertido a mayúsculas)
        manifest_dir = Path(repo_path) / "database" / "data" / schema_name.upper() / branch_name.upper() # Usar mayúsculas aquí según ejemplo de ruta del manifest

        # ** NEW: Clean up the target manifest directory before writing **
        # Limpiamos solo el directorio específico del branch para el manifiesto DB.
        if manifest_dir.exists():
            st.info(f"Limpiando directorio manifiesto existente para '{branch_name.upper()}' en la ruta DB data: {manifest_dir.relative_to(repo_path).as_posix()}")
            try:
                shutil.rmtree(manifest_dir)
            except Exception as e:
                st.warning(f"No se pudo limpiar el directorio manifiesto existente '{manifest_dir.relative_to(repo_path).as_posix()}' en la ruta DB data. Detalle: {e}")


        # Crear directorios si no existen
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = manifest_dir / "manifest.txt"

        # Generar el contenido del manifiesto, pasando todos los datos y configuraciones necesarias.
        # generate_manifest_content filtrará automáticamente los archivos que no corresponden a categorías DB.
        manifest_content = generate_manifest_content(
            schema_name=schema_name, # Pasar el nombre del esquema como está seleccionado (puede ser mayúsculas/minúsculas)
            branch_name=branch_name, # Pasar el nombre de la rama (puede ser mayúsculas/minúsculas)
            all_files_data=files_data, # Pasar *todos* los datos recolectados
            script_like_folders=SQL_SPECIFIC_FOLDERS,
            manifest_categories=MANIFEST_CATEGORIES # Pasar las categorías de scripts DB
        )

        # Solo escribir el archivo manifest.txt si hay contenido generado
        if manifest_content.strip(): # Verificar si hay algo más que espacios en blanco después de strip
             with open(manifest_path, "w", encoding="utf-8") as f:
                 f.write(manifest_content)

             st.success(f"✅ Manifiesto generado en: `{manifest_path.relative_to(repo_path).as_posix()}`") # Mostrar ruta relativa al repo
        else:
             st.info(f"ℹ️ No se generó contenido para el manifiesto de scripts DB. No se creó el archivo `{manifest_path.relative_to(repo_path).as_posix()}`.")


        return True
    except Exception as e:
        st.error(f"❌ Error al generar o escribir el archivo manifest.txt: {e}")
        return False

def get_schema_directories(repo_path: str):
    """Lista los nombres de los directorios dentro de repo_path/database/plsql."""
    schema_list = []
    if not repo_path:
        return []
    schema_base_path = Path(repo_path) / "database" / "plsql"
    if schema_base_path.is_dir():
        try:
            # Listar solo directorios, excluir archivos y subdirectorios que empiecen con '.'
            schema_list = [d.name for d in schema_base_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
            schema_list.sort() # Opcional: ordenar alfabéticamente
        except Exception as e:
            st.warning(f"No se pudieron listar los directorios de esquema en '{schema_base_path}'. Verifica la ruta del repositorio y permisos. Detalle: {e}") # Mensaje más útil
            schema_list = []
    return schema_list


# --- Interfaz Streamlit ---
# Eliminamos el título principal y la descripción inicial
# st.title("🚀 Herramienta de Análisis y Preparación de Scripts DB para Azure DevOps 📦")
# st.write("Sube un archivo ZIP, analiza los scripts de base de datos, y automatiza la creación de rama y manifiesto.")

# Inicializar estados en session_state si no existen y definir el nivel actual
if 'level' not in st.session_state:
    st.session_state.level = 1
if 'temp_dir' not in st.session_state:
    st.session_state.temp_dir = None
if 'archive_extracted' not in st.session_state:
    st.session_state.archive_extracted = False
if 'analysis_done' not in st.session_state:
     st.session_state.analysis_done = False
if 'findings' not in st.session_state:
     st.session_state.findings = {}
if 'ordered_files_list' not in st.session_state:
     st.session_state.ordered_files_list = [] # Esta lista seguirá conteniendo solo archivos para análisis (VALID_EXTS)
if 'temp_extracted_files_data' not in st.session_state:
     st.session_state.temp_extracted_files_data = [] # Esta lista contendrá todos los archivos recopilados (ALLOWED_EXTENSIONS_MANIFEST)
if 'last_uploaded_filename' not in st.session_state:
    st.session_state.last_uploaded_filename = None
if 'repo_path_input' not in st.session_state:
     st.session_state.repo_path_input = ""
if 'schema_directories' not in st.session_state:
     st.session_state.schema_directories = []
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = None
if 'branch_name_input' not in st.session_state:
     st.session_state.branch_name_input = ""


# --- Nivel 1: Subir Archivo y Analizar ---
# Este bloque solo se ejecuta si el nivel actual es 1
if st.session_state.get('level', 1) == 1:
    # Eliminamos el encabezado de nivel y el texto descriptivo
    # st.header(f"🎮 Nivel {st.session_state.get('level', 1)}: Análisis de Scripts")
    # st.write("Sube un archivo ZIP para analizar su contenido. ¡Supera este nivel corrigiendo todos los fallos para avanzar! 👇")

    uploaded_file = st.file_uploader("Elige un archivo ZIP de scripts", type=["zip"], key="uploader_lvl1")

    if uploaded_file is not None:
        if st.session_state.temp_dir is None or not os.path.exists(st.session_state.temp_dir) or (st.session_state.last_uploaded_filename and st.session_state.last_uploaded_filename != uploaded_file.name):
             if st.session_state.temp_dir and os.path.exists(st.session_state.temp_dir):
                  try:
                       shutil.rmtree(st.session_state.temp_dir)
                  except Exception as e:
                       st.warning(f"No se pudo limpiar el directorio temporal anterior {st.session_state.temp_dir}. Detalle: {e}")

             st.session_state.temp_dir = tempfile.mkdtemp(prefix='automation_streamlit_')
             st.session_state.archive_extracted = False
             st.session_state.analysis_done = False
             st.session_state.findings = {}
             st.session_state.ordered_files_list = [] # Resetear lista de archivos para análisis
             st.session_state.temp_extracted_files_data = [] # Resetear lista de archivos recopilados para copiado/manifiesto
             st.session_state.last_uploaded_filename = uploaded_file.name

        archive_path = os.path.join(st.session_state.temp_dir, uploaded_file.name)

        if not st.session_state.archive_extracted:
            try:
                if not os.path.exists(archive_path) or (st.session_state.last_uploaded_filename and st.session_state.last_uploaded_filename == uploaded_file.name and os.path.getsize(archive_path) != uploaded_file.size):
                     with open(archive_path, "wb") as f:
                         f.write(uploaded_file.getbuffer())
                     st.info(f"📦 Archivo '{uploaded_file.name}' subido exitosamente a directorio temporal.")

                st.info("🧩 Extrayendo archivos...")
                extract_archive(archive_path, st.session_state.temp_dir)
                st.session_state.archive_extracted = True
                st.success("✅ Archivo extraído correctamente.")

                st.info("🔍 Recopilando archivos para procesamiento y análisis...")
                # Recopilar TODOS los archivos relevantes (para copiado y manifiesto)
                st.session_state.temp_extracted_files_data = collect_files_for_manifest(Path(st.session_state.temp_dir))

                # Recopilar SOLO los archivos elegibles para el análisis interno (VALID_EXTS)
                # Aunque ordered_folders y folder_map no se usan directamente en el reporte,
                # esta llamada sigue siendo útil para obtener ordered_files_list para el análisis.
                ordered_folders, folder_map, ordered_files_list_for_analysis = collect_and_order_files(st.session_state.temp_dir)
                st.session_state.ordered_files_list = ordered_files_list_for_analysis # Guardar la lista solo para análisis


                if not st.session_state.temp_extracted_files_data: # Verificar si se encontró ALGUN archivo elegible para procesar
                    # Modificar mensaje para reflejar que no se encontraron archivos para *procesamiento*
                    allowed_exts_str = ', '.join(sorted(list(ALLOWED_EXTENSIONS_MANIFEST)))
                    st.warning(f"⚠️ No se encontraron archivos con extensiones permitidas ({allowed_exts_str}) en el archivo subido para procesar.")
                    st.session_state.analysis_done = True # Marcar como hecho para mostrar el reporte vacío
                    st.session_state.findings = {}
                else:
                    # Realizar análisis solo en los archivos elegibles para análisis (VALID_EXTS)
                    findings = {}
                    # Iterar sobre la lista de archivos para análisis (ordered_files_list)
                    for f_rel_path in st.session_state.ordered_files_list:
                        full_path = os.path.join(st.session_state.temp_dir, f_rel_path)
                        ext = os.path.splitext(f_rel_path)[1].lower()
                        # analyze_file internamente verifica si la extensión es elegible
                        findings[f_rel_path] = analyze_file(full_path, ext)

                    st.session_state.findings = findings
                    st.session_state.analysis_done = True
                    st.success("✅ Recopilación y análisis de archivos completado.")

            except EnvironmentError as e:
                st.error(f"❌ Error de entorno durante la extracción o análisis: {e}")
                st.session_state.analysis_done = False
            except ValueError as e:
                st.error(f"❌ Error de valor durante la extracción o análisis: {e}")
                st.session_state.analysis_done = False
            except Exception as e:
                st.error(f"❌ Ocurrió un error inesperado durante la fase de subida, extracción o análisis inicial: {e}")
                st.session_state.analysis_done = False

        # Mostrar reporte de análisis si el análisis se completó
        if st.session_state.get('analysis_done', False):
            findings = st.session_state.get('findings', {})
            # Usar la lista completa de archivos recopilados para la SECCIÓN 1
            all_collected_files_data = st.session_state.get('temp_extracted_files_data', [])

            # Calcular el total de fallos solo de los scripts DB analizados
            total_issues = sum(len(issues) for issues in findings.values())

            st.subheader("Reporte de Análisis") # Mantener el subencabezado del reporte

            # Modificar encabezado para reflejar que se muestran todos los archivos para procesamiento
            st.markdown("#### SECCIÓN 1: Archivos identificados y orden (Para procesamiento)") # Mantener encabezado de sección
            if all_collected_files_data:
                # all_collected_files_data ya está ordenado por collect_files_for_manifest
                for file_data in all_collected_files_data:
                    st.write(f"- `{file_data['relative_path_from_extracted']}`")
            else:
                 # Este mensaje ahora se muestra si temp_extracted_files_data está vacío
                 allowed_exts_str = ', '.join(sorted(list(ALLOWED_EXTENSIONS_MANIFEST)))
                 st.info(f"ℹ️ No se identificaron archivos con extensiones permitidas ({allowed_exts_str}) en el archivo subido.")

            st.markdown("#### SECCIÓN 2: Análisis detallado por archivo (Terminadores '/')") # Mantener encabezado de sección
            # files_with_slash_issues solo contendrá los scripts DB con problemas
            files_with_slash_issues = {f_rel_path: issues for f_rel_path, issues in findings.items() if issues}

            if files_with_slash_issues:
                 # Iterar sobre la lista ordenada de archivos para análisis para mostrar los fallos en ese orden
                 # ordered_files_list contiene solo los archivos que fueron analizados (VALID_EXTS)
                 for f_rel_path in st.session_state.ordered_files_list:
                    if f_rel_path in files_with_slash_issues:
                         issues = files_with_slash_issues[f_rel_path]
                         st.markdown(f"##### Archivo: `{os.path.basename(f_rel_path)}`") # Mostrar solo el nombre del archivo en el reporte detallado
                         for issue in issues:
                             st.warning(f"⚠️ - {issue}") # Emoji para warning
            else:
                st.info("🎉 No se encontraron fallos de terminación (/).") # Emoji para éxito en la sección

            # La transición de nivel se basa en si hay fallos en el análisis (solo scripts DB)
            if total_issues == 0 and st.session_state.get('temp_extracted_files_data'): # Solo pasar si no hay fallos Y se encontró ALGUN archivo para procesar
                st.success("✅ ¡Análisis completado! No se encontraron fallos en los scripts DB. ¡Nivel 1 Superado!") # Emoji y texto de éxito
                st.session_state.level = 2 # Pasar al Nivel 2 si no hay fallos
            elif total_issues > 0:
                st.error(f"❌ Análisis completado. Se encontraron {total_issues} fallos. Por favor, corrige los fallos antes de continuar.") # Emoji para error
                st.session_state.level = 1 # Permanecer en el Nivel 1
            elif not st.session_state.get('temp_extracted_files_data'):
                 # Si no se encontró ningún archivo elegible para procesar, quedarse en nivel 1
                 st.warning("⚠️ No se encontraron archivos elegibles para procesar en el archivo subido. Por favor, sube un archivo con las extensiones permitidas.")
                 st.session_state.level = 1


# --- Nivel 2 & 3: Preparación para Azure DevOps (Inputs y Acción) ---
if st.session_state.get('level', 1) >= 2:
    st.markdown("---") # Mantener el separador
    # Eliminamos el encabezado de nivel y el texto descriptivo
    # st.header(f"🎯 Nivel {st.session_state.level}: Preparación para Azure DevOps")
    # if st.session_state.level == 2:
    #      st.write("Ingresa la ruta de tu repositorio local, selecciona el esquema y define el nombre del nuevo branch. ¡Completa correctamente estos campos para pasar al Nivel 3! 👇")
    # elif st.session_state.level == 3:
    #      st.write("¡Inputs validados! Estás listo para ejecutar el proceso en Azure DevOps. Presiona el botón para crear la rama, copiar archivos y generar el manifiesto. 💪")


    # 1. Campo de texto para la ruta del repositorio
    st.session_state.repo_path_input = st.text_input(
        "Ruta del Directorio del Repositorio Local:", # Simplificar label
        value=st.session_state.repo_path_input,
        # Eliminamos el help text
        # help="Ingresa la ruta absoluta o relativa al directorio raíz de tu repositorio Git local.",
        key="repo_path_text_input"
    )

    # 2. Dropdown para seleccionar el esquema
    repo_path = st.session_state.repo_path_input.strip()
    current_schema_dirs = []
    if repo_path and os.path.isdir(repo_path):
         current_schema_dirs = get_schema_directories(repo_path)

    # Solo actualizar el estado si la lista ha cambiado para evitar re-render innecesario
    if current_schema_dirs != st.session_state.schema_directories:
         st.session_state.schema_directories = current_schema_dirs
         # Si el esquema seleccionado previamente ya no está en la lista, o si no hay un esquema seleccionado,
         # intentar establecer "DBAPER" como predeterminado si está disponible.
         if st.session_state.selected_schema not in st.session_state.schema_directories:
             if "DBAPER" in st.session_state.schema_directories:
                 st.session_state.selected_schema = "DBAPER"
             else:
                 st.session_state.selected_schema = None
         st.rerun() # Corregido: Usar st.rerun()


    schema_options = st.session_state.schema_directories
    schema_display_options = ["-- Selecciona un esquema --"] + schema_options

    # Determinar el esquema a preseleccionar.
    # Si un esquema válido ya está seleccionado en session_state, usar ese.
    # De lo contrario, si estamos en el Nivel 2 y "DBAPER" está disponible en la lista de opciones, preseleccionar "DBAPER".
    # De lo contrario, seleccionar el placeholder (índice 0).
    schema_to_preselect = None
    if st.session_state.selected_schema in schema_options:
        schema_to_preselect = st.session_state.selected_schema
    # Aplicar la preselección de "DBAPER" solo al entrar al Nivel 2, si "DBAPER" está disponible y no hay otro esquema válido ya seleccionado.
    elif st.session_state.get('level', 1) == 2 and "DBAPER" in schema_options and st.session_state.selected_schema not in schema_options:
        schema_to_preselect = "DBAPER"

    # Encontrar el índice del esquema a preseleccionar en las opciones de visualización
    index_of_selection = 0 # Por defecto, seleccionar el placeholder
    if schema_to_preselect:
        try:
            index_of_selection = schema_display_options.index(schema_to_preselect)
        except ValueError:
            # Esto no debería ocurrir si schema_to_preselect se obtuvo de schema_options o es "DBAPER" (verificado si está en options),
            # pero como salvaguarda.
            index_of_selection = 0


    selected_schema_index = st.selectbox(\
        "Seleccione el Esquema:", # Simplificar label
        options=range(len(schema_display_options)),
        format_func=lambda x: schema_display_options[x].upper() if x > 0 else schema_display_options[x],
        index=index_of_selection, # Usar el índice calculado para la preselección
        key="schema_select_box",
        disabled=not bool(schema_options) # Deshabilitar si no hay opciones de esquema disponibles
    )
    # Actualizar el esquema seleccionado en session_state basado en el valor del selectbox
    st.session_state.selected_schema = schema_display_options[selected_schema_index] if selected_schema_index > 0 else None

    # 3. Campo de texto para el nombre del branch
    st.session_state.branch_name_input = st.text_input(
        "Nombre del Nuevo Branch:",
        value=st.session_state.branch_name_input,
        # Eliminamos el help text
        # help="El nombre del branch debe comenzar con 'F_' y no contener espacios. Se convertirá a mayúsculas.",
        key="branch_name_text_input"
    )

    # Validar inputs del Nivel 2
    repo_path_valid = repo_path and os.path.isdir(repo_path)
    schema_selected_valid = st.session_state.selected_schema is not None
    branch_name_clean = st.session_state.branch_name_input.strip()
    # Validation: Must start with "F_", no spaces, and have characters after "F_"
    branch_name_valid_format = branch_name_clean.upper().startswith("F_") and " " not in branch_name_clean and len(branch_name_clean) > 2

    # Check if Level 2 inputs are valid to potentially move to Level 3
    level_2_inputs_valid = repo_path_valid and schema_selected_valid and branch_name_valid_format

    # Transición entre Nivel 2 y 3
    # La transición a Nivel 3 solo ocurre si no hay fallos en Nivel 1 Y los inputs de Nivel 2 son válidos
    level_1_no_issues = st.session_state.get('analysis_done', False) and sum(len(issues) for issues in st.session_state.get('findings', {}).values()) == 0
    # También se requiere que se hayan encontrado archivos para procesar para pasar al Nivel 2/3
    files_for_processing_found = bool(st.session_state.get('temp_extracted_files_data'))


    if level_1_no_issues and files_for_processing_found and level_2_inputs_valid and st.session_state.level < 3:
        st.session_state.level = 3
        st.rerun() # Corregido: Usar st.rerun()

    # La transición de vuelta a Nivel 2 ocurre si deja de haber input válidos en Nivel 2, O si aparecen fallos en Nivel 1 (aunque el análisis ya debería haber puesto el nivel en 1)
    # Simplificamos: si no hay inputs de Nivel 2 válidos y el nivel es 3, regresar a 2.
    # O si la validación de archivos encontrados para procesamiento falla (esto puede pasar si se borra el temp_dir después de pasar Nivel 1)
    if (not level_2_inputs_valid or not files_for_processing_found) and st.session_state.level == 3:
        st.session_state.level = 2
        st.rerun() # Corregido: Usar st.rerun()


    # --- Nivel 3: Botón de Acción Principal ---
    # El botón está habilitado solo si se pasaron los Niveles 1 y 2 (es decir, si el nivel actual es 3)
    disable_button = not (st.session_state.get('level', 1) == 3)


    if st.button("🚀 Ejecutar Proceso Azure DevOps", disabled=disable_button): # Simplificar texto del botón
        st.info("🛠️ Iniciando proceso...") # Simplificar mensaje de inicio

        repo_path = st.session_state.repo_path_input.strip()
        branch_name = st.session_state.branch_name_input.strip().upper() # Usar mayúsculas para el nombre de la rama en Git
        schema_name = st.session_state.selected_schema # Usar el esquema seleccionado del dropdown
        temp_dir = st.session_state.temp_dir
        # Usar la lista completa de archivos recopilados para el copiado y la generación del manifiesto (que filtra internamente)
        files_data_for_processing = st.session_state.temp_extracted_files_data

        # Asegurarse de que los inputs son válidos justo antes de ejecutar (doble verificación)
        # Añadir verificación de si hay archivos para procesar
        if not (repo_path and os.path.isdir(repo_path) and schema_name and branch_name.startswith("F_") and " " not in branch_name and files_data_for_processing):
             st.error("❌ Error de validación interna antes de ejecutar el proceso. Por favor, revisa los inputs del Nivel 2 y asegura que se encontraron archivos para procesar.")
             st.session_state.level = 2 # Regresar al Nivel 2 si la validación falla aquí
             st.rerun() # Forzar rerun para actualizar la UI
        else:
             # Ejecutar los pasos del Nivel 3
             # 1. Crear y cambiar a la nueva rama
             if create_and_checkout_branch(repo_path, branch_name):
                 # 2. Copiar archivos extraídos al repositorio
                 # Pasar el nombre del esquema (del dropdown) y los datos de *todos* los archivos recopilados
                 if copy_extracted_files_to_repo(temp_dir, repo_path, schema_name, files_data_for_processing):
                      # 3. Generar y escribir el manifest.txt (solo para scripts DB)
                      # Pasar el nombre del esquema (del dropdown), el nombre de la rama (en mayúsculas para la ruta del manifest),
                      # y los datos de *todos* los archivos, además de las configuraciones necesarias.
                      # generate_and_write_manifest filtrará internamente los archivos no DB.
                      if generate_and_write_manifest(repo_path, branch_name, schema_name, files_data_for_processing):
                           st.success("🥳🎉 ¡Proceso de Azure DevOps completado exitosamente! ¡Nivel 3 Superado!")
                           # st.balloons() # Comentamos o eliminamos esta línea para quitar la animación de globos


                           # Opcional: Añadir, commit y push
                           # st.info("Adding, committing, and pushing changes...")
                           # if run_git_command(repo_path, ["add", "."]):
                           #      commit_message = f"feat: Add DB scripts for branch {branch_name}"
                           #      if run_git_command(repo_path, ["commit", "-m", commit_message]):
                           #           remote_name = "origin"
                           #           if run_git_command(repo_path, ["push", "-u", remote_name, branch_name]):
                           #                st.success(f"✅ Changes pushed to branch '{branch_name}'.")
                           #           else:
                           #                st.error("❌ Failed to push changes.")
                           #      else:
                           #           st.error("❌ Failed to create commit.")
                           # else:
                           #      st.error("❌ Failed to add files to staging area.")

                      else:
                           st.error("❌ Proceso fallido en la etapa de generación/escritura del manifest.txt.")
                 else:
                      st.error("❌ Proceso fallido en la etapa de copia de archivos.")
             else:
                  st.error("❌ Proceso fallido en las operaciones Git iniciales.")


# --- Limpieza del directorio temporal ---
# Este bloque ahora solo se activa cuando el botón de limpieza es clickeado, no en cada rerun.
# La lógica para verificar si el botón fue clickeado y activar la limpieza está en la sección del botón.
if st.session_state.get('temp_dir') and os.path.exists(st.session_state.temp_dir):
     col1, col2 = st.columns([0.4, 0.6])
     with col1:
          if st.button("🧹 Limpiar Temporales y Reiniciar", key="cleanup_button"): # Simplificar texto del botón
               pass # La lógica se activa en el siguiente rerun


     with col2:
          # Eliminamos el texto descriptivo, manteniendo solo mensajes de estado si ocurren.
          # st.info("Borra los archivos temporales extraídos y reinicia la aplicación a su estado inicial.")
          pass # No mostrar nada en la segunda columna a menos que haya mensajes de estado del borrado


     # La lógica de limpieza ahora se activa cuando el estado 'cleanup_button_clicked' es True
     # Inicializar el estado del botón clickeado si no existe
     if 'cleanup_button_clicked' not in st.session_state:
          st.session_state.cleanup_button_clicked = False

     # Si el botón de limpieza fue clickeado en el último rerun, proceder con la limpieza
     if st.session_state.cleanup_button_clicked:
          try:
               if st.session_state.get('temp_dir') and os.path.exists(st.session_state.temp_dir):
                    st.info(f"Borrando directorio temporal: {st.session_state.temp_dir}")
                    shutil.rmtree(st.session_state.temp_dir)
                    st.success("✨ Directorio temporal limpiado.")

               # Limpiar solo los estados relevantes para reiniciar la aplicación
               keys_to_clear = ['temp_dir', 'archive_extracted', 'analysis_done', 'findings',
                                'ordered_files_list', 'temp_extracted_files_data', 'last_uploaded_filename',
                                'level'] # No reiniciar inputs de Nivel 2 como repo_path, branch_name, selected_schema
               for key in keys_to_clear:
                    if key in st.session_state:
                         del st.session_state[key]

               st.session_state.cleanup_button_clicked = False # Reset the button state AFTER cleanup
               st.success("✨ Estado de análisis y temporal reiniciado.")
               st.rerun() # Forzar un rerun después de la limpieza
          except Exception as e:
               st.error(f"❌ Error al limpiar el directorio temporal: {e}")
               st.session_state.cleanup_button_clicked = False # Reset the button state even on error

     # Actualizar el estado del botón clickeado
     if st.session_state.get('cleanup_button'): # st.button retorna True si fue clickeado en este rerun
          st.session_state.cleanup_button_clicked = True
          st.rerun() # Forzar un rerun para que la lógica de limpieza se ejecute en el siguiente ciclo (donde cleanup_button_clicked será True)