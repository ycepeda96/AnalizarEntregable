# apolo_mejorado.py
import streamlit as st
import os
import zipfile
import re
import tempfile
import shutil
import subprocess
from pathlib import Path
import sys
import logging

# Configura el registro
logging.basicConfig(filename='app.log', level=logging.DEBUG)

# --- Configuración Global ---
# ¡Esta debe ser la PRIMERA llamada a una función de Streamlit!
st.set_page_config(page_title="Apolo - Automatización Azure DevOps", page_icon="🚀", layout="wide")

# Constantes para extensiones y carpetas
VALID_DB_EXTS = {'.sql', '.pks', '.pkb', '.prc', '.fnc', '.vw', '.trg', '.seq'}
ALLOWED_EXTENSIONS_MANIFEST = VALID_DB_EXTS.union({".fmb", ".rdf"})
SQL_SPECIFIC_FOLDERS = {"scripts", "grants", "opciones", "indices", "tabla", "sequence"}

# Categorías para el manifiesto
MANIFEST_CATEGORIES = {
    "scripts": {
        "header": "-- Ejecucion de scripts sql",
        "extensions": {".sql"},
        "specific_folders": SQL_SPECIFIC_FOLDERS,
        "format_per_folder": True
    },
    "packages": {
        "header": "-- Ejecucion de script creacion de packages",
        "extensions": {".pks"},
        "specific_folders": set(),
        "format_per_folder": False
    },
    "packagesbodies": {
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
}

# --- Funciones de Utilidad (globales si son genéricas y no dependen del estado de la app) ---

def numeric_key(s: str) -> int:
    """Extrae el número inicial de una cadena para ordenamiento numérico."""
    m = re.match(r"(\d+)", s)
    return int(m.group(1)) if m else float('inf')

def run_git_command(repo_path: str, command: list, suppress_errors: bool = False) -> bool:
    """Ejecuta un comando Git usando subprocess. Muestra errores a menos que suppress_errors=True."""
    full_command = ["git"] + command
    try:
        result = subprocess.run(full_command, check=True, capture_output=True, text=True, cwd=repo_path, shell=False)
        st.text(result.stdout.strip())
        if result.stderr:
            st.text(result.stderr.strip())
        return True
    except FileNotFoundError:
        st.error("Error: El comando 'git' no fue encontrado. Asegúrate de que Git está instalado y en el PATH.")
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

def check_git_repo(repo_path: str) -> bool:
    """Verifica si la ruta especificada es un repositorio Git válido."""
    if not os.path.isdir(repo_path):
        return False
    git_folder = Path(repo_path) / ".git"
    return git_folder.is_dir()

def get_schema_directories(repo_path: str) -> list[str]:
    """Lista los nombres de los directorios dentro de repo_path/database/plsql."""
    schema_list = []
    if not repo_path:
        return []
    schema_base_path = Path(repo_path) / "database" / "plsql"
    if schema_base_path.is_dir():
        try:
            schema_list = [d.name for d in schema_base_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
            schema_list.sort()
        except Exception as e:
            st.warning(f"No se pudieron listar los directorios de esquema en '{schema_base_path}'. Verifica la ruta del repositorio y permisos. Detalle: {e}")
            schema_list = []
    return schema_list

def check_git_status(repo_path: str):
    """Verifica el estado del repositorio Git y devuelve el resultado."""
    try:
        result = subprocess.run(["git", "status"], check=True, capture_output=True, text=True, cwd=repo_path)
        return result.stdout.strip()
    except Exception as e:
        return f"Error al verificar el estado del repositorio: {e}"

class ApoloApp:
    def __init__(self):
        self._initialize_session_state()

    def _initialize_session_state(self):
        """Inicializa las variables de estado de la sesión."""
        default_state = {
            'level': 1,
            'temp_dir': None,
            'archive_extracted': False,
            'analysis_done': False,
            'findings': {},
            'ordered_db_files_for_analysis': [], # Lista de archivos DB para el reporte de análisis
            'all_extracted_files_data': [], # Lista de todos los archivos para copia/manifiesto
            'last_uploaded_filename': None,
            'repo_path_input': "",
            'schema_directories': [],
            'selected_schema': None,
            'branch_name_input': "",
            'commit_message_input': "",
            'cleanup_triggered': False # Nuevo estado para controlar la limpieza
        }
        for key, value in default_state.items():
            if key not in st.session_state:
                st.session_state[key] = value

    def _reset_state_for_new_upload(self):
        """Reinicia el estado para una nueva carga de archivo ZIP."""
        if st.session_state.temp_dir and os.path.exists(st.session_state.temp_dir):
            try:
                shutil.rmtree(st.session_state.temp_dir)
            except Exception as e:
                st.warning(f"No se pudo limpiar el directorio temporal anterior {st.session_state.temp_dir}. Detalle: {e}")

        st.session_state.temp_dir = tempfile.mkdtemp(prefix='apolo_')
        st.session_state.archive_extracted = False
        st.session_state.analysis_done = False
        st.session_state.findings = {}
        st.session_state.ordered_db_files_for_analysis = []
        st.session_state.all_extracted_files_data = []
        st.session_state.last_uploaded_filename = None # Se actualiza después de la carga
        st.session_state.level = 1 # Asegura que se reinicie al nivel 1

    def _extract_archive(self, archive_path: str, dest_dir: str):
        """Extrae archivos .zip a un directorio de destino."""
        if not zipfile.is_zipfile(archive_path):
            raise ValueError(f"El archivo '{Path(archive_path).name}' no es un archivo ZIP válido o está corrupto.")
        with zipfile.ZipFile(archive_path, 'r') as z:
            z.extractall(dest_dir)

    def _validate_file_naming_and_ext(self, file_path: Path) -> list[str]:
        """
        Valida el archivo para extensiones en minúsculas y caracteres especiales.
        Retorna una lista de cadenas de error/advertencia.
        """
        errors = []
        if file_path.suffix != file_path.suffix.lower():
            errors.append(f"❌ La extensión del archivo '{file_path.name}' debe estar en minúsculas para evitar problemas de compatibilidad.")
        
        special_chars_pattern = re.compile(r'[/\*# ]') # Caracteres especiales prohibidos
        if special_chars_pattern.search(file_path.name):
            errors.append(f"⚠️ El archivo '{file_path.name}' contiene caracteres especiales (/, *, #, espacio) que podrían causar errores al compilar en Azure. Se recomienda evitarlos.")
        
        return errors

    def _check_slash_terminators(self, lines: list[str], ext: str, file_name: str) -> list[str]:
        """Verifica la presencia de '/' después del *último* bloque PL/SQL END;."""
        slash_issues = []
        if ext.lower() not in ('.pks', '.pkb', '.prc', '.fnc', '.trg'):
            return slash_issues

        end_pattern = re.compile(r'END(\s+\w+)?;\s*$', re.IGNORECASE)
        last_end_index = -1
        for i in range(len(lines) - 1, -1, -1):
            if end_pattern.search(lines[i]):
                last_end_index = i
                break

        if last_end_index == -1:
            return slash_issues

        j = last_end_index + 1
        while j < len(lines) and (lines[j].strip() == "" or lines[j].strip().startswith('--') or lines[j].strip().startswith('/*')):
            j += 1

        if j == len(lines) or lines[j].strip() != '/':
            slash_issues.append(f"Línea {last_end_index+1}: Falta '/' al final después del último bloque END;.")
        return slash_issues

    def _analyze_db_file(self, full_path: Path) -> list[str]:
        """Realiza el análisis completo de un archivo de script de base de datos."""
        issues = []
        file_ext = full_path.suffix.lower()

        # Validaciones de nombrado y extensión
        issues.extend(self._validate_file_naming_and_ext(full_path))

        # Si el archivo no es un script DB válido para el análisis de terminadores, salimos
        if file_ext not in VALID_DB_EXTS:
            return issues

        try:
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        except Exception as e:
            return issues + [f"Error al leer el archivo '{full_path.name}': {e}"]

        # Verificación específica del slash
        issues.extend(self._check_slash_terminators(lines, file_ext, full_path.name))
        return issues

    def _collect_files_for_processing(self, root_dir: Path) -> list[dict]:
        """
        Navega recursivamente, filtra y ordena todos los archivos relevantes
        para la generación del manifiesto y la copia (todas las extensiones en ALLOWED_EXTENSIONS_MANIFEST).
        """
        collected_files_data = []
        for dirpath, dirnames, filenames in os.walk(root_dir):
            current_path = Path(dirpath)

            # Ignorar carpetas 'rollback' y sus subdirectorios
            if "rollback" in current_path.name.lower():
                dirnames[:] = [] # No descender
                continue

            for filename_str in filenames:
                file_path = current_path / filename_str
                file_ext = file_path.suffix.lower()

                if file_ext in ALLOWED_EXTENSIONS_MANIFEST:
                    relative_path = file_path.relative_to(root_dir)
                    prefix_num = numeric_key(filename_str)

                    collected_files_data.append({
                        "absolute_path": str(file_path),
                        "relative_path_from_extracted": str(relative_path.as_posix()),
                        "parent_folder_name": current_path.name,
                        "prefix_num": prefix_num,
                        "extension": file_ext,
                        "filename_str": filename_str
                    })

        # Ordenar la lista aplanada para consistencia
        collected_files_data.sort(key=lambda x: (x["relative_path_from_extracted"], x.get("prefix_num", float('inf')), x["filename_str"]))
        return collected_files_data

    def _get_manifest_category(self, file_data: dict) -> str | None:
        """Determina la clave de categoría del manifiesto para un archivo dado."""
        file_ext = file_data["extension"].lower()
        
        # Iterar a través de las categorías definidas para encontrar una coincidencia de extensión
        for category_key, details in MANIFEST_CATEGORIES.items():
            if file_ext in details["extensions"]:
                return category_key
        return None # Si no coincide con ninguna categoría de manifiesto DB

    def _generate_manifest_content(self, schema_name: str, branch_name: str, all_files_data: list[dict]) -> str:
        """
        Genera el contenido del archivo manifest.txt.
        Incluye solo archivos categorizados en MANIFEST_CATEGORIES.
        """
        content_lines = []
        schema_name_upper = schema_name.upper()
        branch_name_upper = branch_name.upper()

        content_lines.append(f"SCHEMA={schema_name_upper}")
        content_lines.append("")

        files_by_original_folder_and_category = {}
        for file_data in all_files_data:
            category_key = self._get_manifest_category(file_data)
            if category_key: # Solo procesar archivos que fueron categorizados para el manifiesto DB
                original_folder_relative_to_zip = Path(file_data["relative_path_from_extracted"]).parent.as_posix()
                if original_folder_relative_to_zip not in files_by_original_folder_and_category:
                    files_by_original_folder_and_category[original_folder_relative_to_zip] = {}
                if category_key not in files_by_original_folder_and_category[original_folder_relative_to_zip]:
                    files_by_original_folder_and_category[original_folder_relative_to_zip][category_key] = []
                files_by_original_folder_and_category[original_folder_relative_to_zip][category_key].append(file_data)

        sorted_original_folders = sorted(files_by_original_folder_and_category.keys(), key=lambda x: numeric_key(Path(x).name))

        is_first_block_overall = True
        for original_folder_relative_to_zip in sorted_original_folders:
            files_by_manifest_category_in_folder = files_by_original_folder_and_category[original_folder_relative_to_zip]

            added_first_category_header_in_folder = False

            for category_key, details in MANIFEST_CATEGORIES.items():
                files_in_this_category_and_folder = files_by_manifest_category_in_folder.get(category_key, [])

                if files_in_this_category_and_folder:
                    if not is_first_block_overall and not added_first_category_header_in_folder:
                        content_lines.append("")

                    content_lines.append(details["header"])
                    added_first_category_header_in_folder = True
                    is_first_block_overall = False

                    if category_key in ("packages", "packagesbodies"):
                        sorted_files_in_category_and_folder = sorted(files_in_this_category_and_folder,
                                                                     key=lambda x: (x["extension"].lower() != ".pks", x.get("prefix_num", float('inf')), x["filename_str"]))
                    else:
                        sorted_files_in_category_and_folder = sorted(files_in_this_category_and_folder,
                                                                     key=lambda x: (x.get("prefix_num", float('inf')), x["filename_str"]))

                    for file_data in sorted_files_in_category_and_folder:
                        filename = file_data["filename_str"]
                        
                        type_folder_name_in_manifest = category_key.lower() # Nombre de la carpeta en el manifiesto

                        # Construcción de la ruta: database/plsql/{schema_lower}/{type_folder_name_in_manifest}/{filename}
                        # Para el manifiesto, la carpeta del esquema va en mayúsculas, pero la carpeta del tipo de archivo en minúsculas.
                        manifest_file_path = Path("database", "plsql", schema_name.lower(), type_folder_name_in_manifest, filename).as_posix()
                        content_lines.append(manifest_file_path)

        return "\n".join(content_lines)

    def _create_and_checkout_branch(self, repo_path: str, branch_name: str) -> bool:
        """Crea y cambia a una nueva rama en el repositorio local."""
        with st.spinner("Cambiando a la rama 'main' y haciendo pull..."):
            if not run_git_command(repo_path, ["checkout", "main"]):
                st.error("Falló al cambiar a la rama 'main'.")
                return False
            if not run_git_command(repo_path, ["pull"]):
                st.error("Falló al hacer pull en la rama 'main'.")
                return False

        with st.spinner("Limpiando archivos no rastreados..."):
            if not run_git_command(repo_path, ["clean", "-fdx"], suppress_errors=True):
                st.warning("Falló la limpieza de archivos no rastreados. Esto podría deberse a permisos o archivos en uso, pero el proceso continuará.")

        with st.spinner(f"Verificando y creando/cambiando a la rama '{branch_name}'..."):
            branch_exists = False
            try:
                # Usamos check_output para capturar la salida y determinar si la rama existe localmente o remotamente
                # git branch --list <branch_name> para ramas locales
                local_branches_output = subprocess.run(["git", "branch", "--list", branch_name], check=True, capture_output=True, text=True, cwd=repo_path, shell=False).stdout.strip()
                if local_branches_output: # Si hay alguna salida, significa que la rama existe localmente
                    branch_exists = True
                else:
                    # git branch -r --list origin/<branch_name> para ramas remotas
                    remote_branches_output = subprocess.run(["git", "branch", "-r", "--list", f"origin/{branch_name}"], check=True, capture_output=True, text=True, cwd=repo_path, shell=False).stdout.strip()
                    if remote_branches_output:
                        branch_exists = True
            except subprocess.CalledProcessError:
                branch_exists = False # No existe local ni remotamente

            if branch_exists:
                st.warning(f"La rama '{branch_name}' ya existe. Cambiando a ella en lugar de crearla.")
                if not run_git_command(repo_path, ["checkout", branch_name]):
                    st.error(f"Falló al cambiar a la rama existente '{branch_name}'.")
                    return False
            else:
                st.info(f"Creando y cambiando a la nueva rama '{branch_name}'...")
                if not run_git_command(repo_path, ["checkout", "-b", branch_name]):
                    st.error(f"Falló al crear y cambiar a la nueva rama '{branch_name}'.")
                    return False

        st.success(f"Rama '{branch_name}' seleccionada exitosamente.")
        return True

    def _copy_extracted_files_to_repo(self, repo_path: str, schema_name: str, files_data: list[dict]) -> bool:
        """Copia los archivos extraídos al repositorio local siguiendo la estructura definida."""
        st.info(f"Copiando archivos al repositorio local en: {repo_path}")
        schema_lower = schema_name.lower() # Para la ruta de copia de archivos

        copied_count = 0
        try:
            for file_data in files_data:
                src_path = Path(file_data["absolute_path"])
                file_ext = file_data["extension"].lower()
                
                dest_base_dir = Path(repo_path)
                dest_relative_path = None

                # Lógica de copia basada en la extensión
                if file_ext in VALID_DB_EXTS:
                    # Determinar la carpeta de tipo de archivo DENTRO de database/plsql
                    # Mapeo de extensión a la carpeta de destino en el repositorio (minúsculas)
                    type_folder_mapping = {
                        ".sql": "scripts",
                        ".pks": "packages",
                        ".pkb": "packagesbodies",
                        ".prc": "procedures",
                        ".fnc": "functions",
                        ".trg": "triggers",
                        ".vw": "views"
                    }
                    dest_type_folder = type_folder_mapping.get(file_ext)
                    if dest_type_folder:
                        dest_relative_path = Path("database", "plsql", schema_lower, dest_type_folder, src_path.name)
                elif file_ext == '.fmb':
                    dest_relative_path = Path("fuentes", "forma", src_path.name)
                elif file_ext == '.rdf':
                    dest_relative_path = Path("fuentes", "reporte", src_path.name)

                if dest_relative_path:
                    dest_full_path = dest_base_dir / dest_relative_path
                    dest_dir = dest_full_path.parent
                    dest_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_path, dest_full_path)
                    copied_count += 1
                else:
                    st.warning(f"Archivo '{file_data['relative_path_from_extracted']}' con extensión '{file_ext}' no tiene una carpeta de destino definida en la lógica de copiado, no será copiado.")

            st.success(f"{copied_count} archivos copiados exitosamente al repositorio local.")
            return True
        except Exception as e:
            st.error(f"Error inesperado al copiar archivos al repositorio: {e}")
            return False

    def _generate_and_write_manifest(self, repo_path: str, branch_name: str, schema_name: str, files_data: list[dict]) -> bool:
        """Genera el contenido del manifest.txt y lo escribe en la ubicación correcta."""
        try:
            manifest_dir = Path(repo_path) / "database" / "data" / schema_name.upper() / branch_name.upper()

            # Limpiar el directorio del manifiesto antes de escribir
            if manifest_dir.exists():
                st.info(f"Limpiando directorio manifiesto existente para '{branch_name.upper()}' en la ruta DB data: {manifest_dir.relative_to(repo_path).as_posix()}")
                try:
                    shutil.rmtree(manifest_dir)
                except Exception as e:
                    st.warning(f"No se pudo limpiar el directorio manifiesto existente '{manifest_dir.relative_to(repo_path).as_posix()}' en la ruta DB data. Detalle: {e}")

            manifest_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = manifest_dir / "manifest.txt"

            manifest_content = self._generate_manifest_content(
                schema_name=schema_name,
                branch_name=branch_name,
                all_files_data=files_data
            )

            if manifest_content.strip():
                with open(manifest_path, "w", encoding="utf-8") as f:
                    f.write(manifest_content)
                st.success(f"Manifiesto generado en: `{manifest_path.relative_to(repo_path).as_posix()}`")
            else:
                st.info(f"No se generó contenido para el manifiesto de scripts DB. No se creó el archivo `{manifest_path.relative_to(repo_path).as_posix()}`.")

            return True
        except Exception as e:
            st.error(f"Error al generar o escribir el archivo manifest.txt: {e}")
            return False

    def display_progress_stepper(self):
        """Muestra un stepper visual para el progreso de la aplicación."""
        current_level = st.session_state.level
        steps = {
            1: "Análisis de Scripts",
            2: "Configuración y Validación",
            3: "Ejecución y Git"
        }
        
        cols = st.columns(len(steps))
        for i, (level, description) in enumerate(steps.items()):
            with cols[i]:
                if level == current_level:
                    st.markdown(f"**<div style='text-align: center; color: #28a745;'>{description}</div>**", unsafe_allow_html=True)
                elif level < current_level:
                    st.markdown(f"<div style='text-align: center; color: #6c757d;'>~~{description}~~</div>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<div style='text-align: center; color: #adb5bd;'>{description}</div>", unsafe_allow_html=True)
            if i < len(steps) - 1:
                with cols[i]:
                    st.markdown("<div style='text-align: center; color: #adb5bd;'>—</div>", unsafe_allow_html=True) # Separador

        st.markdown("<br>", unsafe_allow_html=True) # Espacio después del stepper


    def run(self):
        st.title("🚀 Apolo: Automatización para Azure DevOps 📦")
        st.write("Sube un archivo ZIP, analiza los scripts de base de datos, y automatiza la creación de rama, copia de archivos y redacción de manifiesto.")

        self.display_progress_stepper()

        # --- Nivel 1: Subir Archivo y Analizar ---
        if st.session_state.level == 1:
            st.header("1. Análisis de Scripts")
            st.info("Sube un archivo ZIP para analizar su contenido y verificar la preparación para Azure DevOps.")

            uploaded_file = st.file_uploader("Elige un archivo ZIP que contenga el contenido de la rama", type=["zip"], key="uploader_lvl1")

            if uploaded_file:
                # Comprobar si es un archivo nuevo o el mismo cargado con cambios
                if st.session_state.last_uploaded_filename != uploaded_file.name or \
                   (st.session_state.last_uploaded_filename == uploaded_file.name and 
                    st.session_state.get('last_uploaded_file_size') != uploaded_file.size):
                    
                    self._reset_state_for_new_upload()
                    st.session_state.last_uploaded_filename = uploaded_file.name
                    st.session_state.last_uploaded_file_size = uploaded_file.size # Guardar tamaño para detectar cambios
                    
                    archive_path = os.path.join(st.session_state.temp_dir, uploaded_file.name)
                    with open(archive_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    st.info(f"Archivo '{uploaded_file.name}' subido exitosamente a directorio temporal.")

                    try:
                        with st.spinner("Extrayendo archivos..."):
                            self._extract_archive(archive_path, st.session_state.temp_dir)
                        st.session_state.archive_extracted = True
                        st.success("Archivo extraído correctamente.")

                        with st.spinner("Recopilando y analizando archivos..."):
                            # Recopilar TODOS los archivos relevantes para copiado y manifiesto
                            st.session_state.all_extracted_files_data = self._collect_files_for_processing(Path(st.session_state.temp_dir))
                            
                            # Realizar análisis solo en los archivos de base de datos válidos
                            findings = {}
                            db_files_for_analysis_paths = []
                            for file_data in st.session_state.all_extracted_files_data:
                                if file_data["extension"] in VALID_DB_EXTS: # Solo analizamos extensiones DB
                                    full_path = Path(file_data["absolute_path"])
                                    db_files_for_analysis_paths.append(file_data["relative_path_from_extracted"]) # Para orden del reporte
                                    issues = self._analyze_db_file(full_path)
                                    if issues:
                                        findings[file_data["relative_path_from_extracted"]] = issues
                            
                            # Ordenar la lista de paths de archivos DB para el reporte
                            db_files_for_analysis_paths.sort(key=lambda x: numeric_key(Path(x).name))
                            st.session_state.ordered_db_files_for_analysis = db_files_for_analysis_paths
                            st.session_state.findings = findings
                            st.session_state.analysis_done = True
                        
                        st.success("Recopilación y análisis de archivos completado.")

                    except (ValueError, zipfile.BadZipFile) as e:
                        st.error(f"Error al procesar el archivo ZIP: {e}")
                        st.session_state.analysis_done = False
                        st.session_state.archive_extracted = False
                    except Exception as e:
                        st.error(f"Ocurrió un error inesperado durante la extracción o análisis: {e}")
                        st.session_state.analysis_done = False
                        st.session_state.archive_extracted = False
                    st.rerun() # Forzar rerun para mostrar el estado actualizado


            if st.session_state.get('analysis_done', False):
                findings = st.session_state.get('findings', {})
                all_collected_files_data = st.session_state.get('all_extracted_files_data', [])
                
                total_db_issues = sum(len(issues) for issues in findings.values())

                st.subheader("📝 Reporte de Análisis")

                st.markdown("##### 1. Archivos Identificados para Procesamiento")
                if all_collected_files_data:
                    st.info(f"Se identificaron {len(all_collected_files_data)} archivos con extensiones permitidas ({', '.join(sorted(list(ALLOWED_EXTENSIONS_MANIFEST)))}) para copiar y/o incluir en el manifiesto.")
                    with st.expander("Ver lista de archivos identificados"):
                        for file_data in all_collected_files_data:
                            st.text(f"- {file_data['relative_path_from_extracted']}")
                else:
                    st.info(f"No se identificaron archivos con extensiones permitidas ({', '.join(sorted(list(ALLOWED_EXTENSIONS_MANIFEST)))}) en el archivo subido.")

                st.markdown("##### 2. Análisis Detallado de Scripts de Base de Datos")
                if findings:
                    st.warning(f"Se encontraron {total_db_issues} fallo(s) en los scripts de base de datos. Por favor, revisa y corrige los siguientes:")
                    for f_rel_path in st.session_state.ordered_db_files_for_analysis: # Iterar en orden
                        if f_rel_path in findings:
                            issues = findings[f_rel_path]
                            st.markdown(f"**Archivo: `{Path(f_rel_path).name}`** (Ruta: `{f_rel_path}`)")
                            for issue in issues:
                                if "❌" in issue:
                                    st.error(issue)
                                else:
                                    st.warning(issue)
                    
                    report_content = ""
                    report_content += "REPORTE DE ANÁLISIS DE APOLO\n\n"
                    report_content += "1. Archivos Identificados para Procesamiento:\n"
                    if all_collected_files_data:
                        for file_data in all_collected_files_data:
                            report_content += f"- {file_data['relative_path_from_extracted']}\n"
                    else:
                        report_content += "No se identificaron archivos con extensiones permitidas.\n"

                    report_content += "\n2. Análisis Detallado de Scripts de Base de Datos:\n"
                    if findings:
                        for f_rel_path in st.session_state.ordered_db_files_for_analysis:
                            if f_rel_path in findings:
                                report_content += f"\nArchivo: {Path(f_rel_path).name} (Ruta: {f_rel_path})\n"
                                for issue in findings[f_rel_path]:
                                    report_content += f"  - {issue}\n"
                    else:
                        report_content += "No se encontraron fallos en los scripts de base de datos.\n"
                    
                    st.download_button(
                        label="Descargar Reporte de Análisis",
                        data=report_content,
                        file_name="apolo_analysis_report.txt",
                        mime="text/plain"
                    )

                else:
                    if any(f["extension"] in VALID_DB_EXTS for f in all_collected_files_data):
                         st.success("🎉 No se encontraron fallos en los scripts de base de datos. ¡Excelente trabajo!")
                    else:
                         st.info("No se encontraron scripts de base de datos para analizar.")

                if total_db_issues == 0 and bool(st.session_state.get('all_extracted_files_data')):
                    st.success("¡Análisis completado! Nivel 1 Superado.")
                    if st.button("Continuar"):
                        st.session_state.level = 2
                        st.rerun()  # Forzar rerun para mostrar el estado actualizado
                elif total_db_issues > 0:
                    st.error("Análisis completado. Se encontraron fallos. Por favor, corrige los fallos antes de continuar.")
                    st.session_state.level = 1
                elif not bool(st.session_state.get('all_extracted_files_data')):
                    st.warning("No se encontraron archivos elegibles para procesar. Por favor, sube un archivo con las extensiones permitidas.")
                    st.session_state.level = 1

        # --- Nivel 2: Configuración y Validación ---
        if st.session_state.level >= 2:
            st.markdown("---")
            st.header("2. Configuración y Validación")
            st.info("Ingresa la ruta de tu repositorio local y selecciona el esquema de base de datos.")

            # 1. Campo de texto para la ruta del repositorio
            st.session_state.repo_path_input = st.text_input(
                "Ruta del Directorio del Repositorio Local:",
                value=st.session_state.repo_path_input,
                placeholder=r"Ej: C:\Users\TuUsuario\MiRepoGit",
                help="Ingresa la ruta absoluta a tu repositorio Git local. Asegúrate de que Git esté instalado y configurado en tu sistema."
            )

            repo_path = st.session_state.repo_path_input.strip()
            repo_path_valid = False
            if repo_path:
                if os.path.isdir(repo_path):
                    if check_git_repo(repo_path):
                        st.success("Ruta del repositorio válida y es un repositorio Git.")
                        repo_path_valid = True
                    else:
                        st.error("El directorio no es un repositorio Git válido. Asegúrate de que contiene la carpeta '.git'.")
                else:
                    st.error("La ruta del directorio no existe.")
            else:
                st.info("Introduce la ruta de tu repositorio local.")

            # 2. Dropdown para seleccionar el esquema
            current_schema_dirs = []
            if repo_path_valid:
                current_schema_dirs = get_schema_directories(repo_path)
            
            # Actualizar st.session_state.schema_directories si la lista cambia
            if current_schema_dirs != st.session_state.schema_directories:
                st.session_state.schema_directories = current_schema_dirs
                # Si el esquema previamente seleccionado no está en la nueva lista, o si no hay selección,
                # intentar preseleccionar "DBAPER" si existe, de lo contrario None.
                if st.session_state.selected_schema not in st.session_state.schema_directories:
                    st.session_state.selected_schema = "DBAPER" if "DBAPER" in st.session_state.schema_directories else None
                # No se necesita rerun aquí, ya que el selectbox se re-renderiza con el nuevo estado al final del script.

            schema_options = st.session_state.schema_directories
            schema_display_options = ["-- Selecciona un esquema --"] + schema_options

            index_of_selection = 0
            if st.session_state.selected_schema in schema_options:
                index_of_selection = schema_display_options.index(st.session_state.selected_schema)

            selected_schema_index = st.selectbox(
                "Seleccione el Esquema:",
                options=range(len(schema_display_options)),
                format_func=lambda x: schema_display_options[x],
                index=index_of_selection,
                key="schema_select_box",
                disabled=not bool(schema_options),
                help="Selecciona el esquema de base de datos al que pertenecen los scripts."
            )
            st.session_state.selected_schema = schema_display_options[selected_schema_index] if selected_schema_index > 0 else None

            schema_selected_valid = st.session_state.selected_schema is not None
            if not schema_selected_valid and bool(schema_options):
                st.warning("Por favor, selecciona un esquema válido.")
            elif not bool(schema_options) and repo_path_valid:
                st.info("No se encontraron esquemas en la ruta 'database/plsql' del repositorio. Asegúrate de que la estructura es correcta.")

            # 3. Campo de texto para el nombre del branch
            st.session_state.branch_name_input = st.text_input(
                "Nombre del Nuevo Branch:",
                value=st.session_state.branch_name_input,
                placeholder="Ej: F_MEJORA_INFORME",
                help="El nombre del branch debe comenzar con 'F_' (mayúsculas), no contener espacios o caracteres especiales (excepto guiones bajos). Se convertirá a mayúsculas."
            )
            branch_name_clean = st.session_state.branch_name_input.strip()
            branch_name_valid_format = False
            if branch_name_clean:
                # Regex: ^F_[A-Z0-9_]+$ -> Empieza con F_, seguido de 1 o más letras, números o guiones bajos
                if re.fullmatch(r"F_[A-Z0-9_]+", branch_name_clean.upper()):
                    st.success("Formato del nombre del branch válido.")
                    branch_name_valid_format = True
                else:
                    st.error("El nombre del branch debe comenzar con 'F_' (mayúsculas) y contener solo letras, números o guiones bajos después. Ejemplo: 'F_MI_NUEVA_FUNCIONALIDAD'")
            else:
                st.info("Introduce el nombre del nuevo branch.")

            # 4. Campo de texto para el mensaje de commit
            st.session_state.commit_message_input = st.text_input(
                "Mensaje para el Commit (opcional):",
                value=st.session_state.commit_message_input,
                placeholder="feat: Añadir nueva funcionalidad de informe X",
                help="Este mensaje se usará para el commit de los cambios en la rama."
            )
            
            # Condición para pasar a Nivel 3
            level_1_ok = st.session_state.get('analysis_done', False) and not st.session_state.get('findings') # No fallos en análisis
            files_for_processing_found = bool(st.session_state.get('all_extracted_files_data'))

            level_2_inputs_valid = repo_path_valid and schema_selected_valid and branch_name_valid_format

            if level_1_ok and files_for_processing_found and level_2_inputs_valid and st.session_state.level < 3:
                if st.button("Continuar"):
                    st.session_state.level = 3
                    st.success("¡Configuración y validación completada! Nivel 2 Superado.")
                    st.rerun()
            elif (not level_2_inputs_valid or not files_for_processing_found) and st.session_state.level == 3:
                st.session_state.level = 2
                st.warning("Se detectaron cambios en la configuración o validación. Regresando al Nivel 2.")
                st.rerun()

# --- Nivel 3: Ejecución y Git ---
        if st.session_state.level >= 3:
            st.markdown("---")
            st.header("3. Ejecución del Proceso")
            st.info("¡Todos los requisitos cumplidos! Revisa los detalles y haz clic en el botón para ejecutar el proceso en Azure DevOps.")

            # Mostrar resumen antes de la ejecución
            st.markdown("##### Resumen de la Operación:")
            st.text(f"- Repositorio: {st.session_state.repo_path_input.strip()}")
            st.text(f"- Esquema seleccionado: {st.session_state.selected_schema}")
            st.text(f"- Nuevo Branch: {st.session_state.branch_name_input.strip().upper()}")
            st.text(f"- Archivos a procesar: {len(st.session_state.all_extracted_files_data)}")

            # Previsualización del manifest.txt
            manifest_preview_content = self._generate_manifest_content(
                schema_name=st.session_state.selected_schema,
                branch_name=st.session_state.branch_name_input.strip(),
                all_files_data=st.session_state.all_extracted_files_data
            )
            if manifest_preview_content.strip():
                with st.expander("Previsualizar contenido de manifest.txt"):
                    st.code(manifest_preview_content, language='text')
            else:
                st.info("No se generará un archivo manifest.txt ya que no se encontraron scripts de base de datos para incluir.")

            # Inicializar estado para la ejecución del proceso principal
            if 'main_process_executed' not in st.session_state:
                st.session_state.main_process_executed = False
                st.session_state.main_process_success = False

            # Botón para el proceso principal (crear rama, copiar archivos, generar manifiesto)
            execute_button_label = "🚀 Ejecutar Proceso Azure DevOps"
            disable_execute_button = st.session_state.main_process_executed or not (st.session_state.level == 3)
            
            if st.button(execute_button_label, disabled=disable_execute_button, key="execute_main_process"):
                st.info("Iniciando proceso de automatización...")
                repo_path = st.session_state.repo_path_input.strip()
                branch_name = st.session_state.branch_name_input.strip().upper()
                schema_name = st.session_state.selected_schema
                files_data_for_processing = st.session_state.all_extracted_files_data

                # Doble verificación final
                if not (repo_path and os.path.isdir(repo_path) and check_git_repo(repo_path) and schema_name and branch_name and files_data_for_processing):
                    st.error("Error de validación interna. Algunos inputs necesarios no son válidos.")
                    st.session_state.level = 2 # Regresar al Nivel 2
                    st.session_state.main_process_executed = False
                    st.session_state.main_process_success = False
                    st.rerun()
                else:
                    success = True
                    with st.spinner("Realizando operaciones Git y copiando archivos..."):
                        if not self._create_and_checkout_branch(repo_path, branch_name):
                            success = False
                        
                        if success and not self._copy_extracted_files_to_repo(repo_path, schema_name, files_data_for_processing):
                            success = False
                        
                        if success and not self._generate_and_write_manifest(repo_path, branch_name, schema_name, files_data_for_processing):
                            success = False
                    
                    st.session_state.main_process_executed = True
                    st.session_state.main_process_success = success
                    st.rerun() # Forzar un rerun para mostrar los resultados del proceso principal

            # Mostrar resultados del proceso principal
            if st.session_state.main_process_executed:
                if st.session_state.main_process_success:
                    st.success("🥳🎉 Proceso de Azure DevOps completado exitosamente!")
                    st.balloons() # Animación de globos

                    # Opcional: Añadir, commit y push
                    st.markdown("##### Opcional: Subir cambios al repositorio")
                    st.info("Si deseas que Apolo añada, haga commit y empuje los cambios a la rama remota, presiona 'Confirmar y Subir Cambios'.")

                    # Inicializar estado para la subida de Git
                    if 'git_push_initiated' not in st.session_state:
                        st.session_state.git_push_initiated = False
                        st.session_state.git_push_success = False
                        st.session_state.git_push_message = "" # Para almacenar el mensaje de resultado

                    # Botón para la subida de Git
                    if st.button("Confirmar y Subir Cambios", disabled=st.session_state.git_push_initiated, key="confirm_and_push"):
                        st.session_state.git_push_initiated = True # Indicar que se inició el proceso
                        st.info("Iniciando subida de cambios a Git...")
                        
                        repo_path = st.session_state.repo_path_input.strip()
                        branch_name = st.session_state.branch_name_input.strip().upper()
                        commit_message = st.session_state.commit_message_input.strip()
                        if not commit_message: # Mensaje por defecto si no se proporciona
                            commit_message = f"feat: Add DB scripts for branch {branch_name}"

                        push_success = True
                        push_messages = []

                        with st.spinner("Añadiendo archivos al staging area..."):
                            if not run_git_command(repo_path, ["add", "."]):
                                push_messages.append("❌ Falló al añadir archivos al área de staging.")
                                push_success = False
                        
                        if push_success:
                            with st.spinner(f"Creando commit: '{commit_message}'..."):
                                if not run_git_command(repo_path, ["commit", "-m", commit_message]):
                                    push_messages.append("❌ Falló al crear el commit.")
                                    push_success = False

                        if push_success:
                            with st.spinner(f"Empujando cambios a la rama '{branch_name}' en 'origin'..."):
                                if not run_git_command(repo_path, ["push", "-u", "origin", branch_name]):
                                    push_messages.append("❌ Falló al empujar los cambios a la rama remota. Asegúrate de tener permisos y credenciales configuradas.")
                                    push_success = False

                        st.session_state.git_push_success = push_success
                        st.session_state.git_push_message = "\n".join(push_messages)
                        st.rerun() # Forzar rerun para mostrar el resultado de la subida

                    # Mostrar resultado de la subida de Git
                    if st.session_state.git_push_initiated:
                        if st.session_state.git_push_success:
                            st.success(f"✅ Cambios empujados exitosamente a la rama '{st.session_state.branch_name_input.strip().upper()}'.")
                        else:
                            st.error(st.session_state.git_push_message if st.session_state.git_push_message else "❌ No se pudieron subir los cambios al repositorio remoto. Consulta los mensajes de error anteriores.")
                else:
                    st.error("❌ El proceso de automatización principal falló en una de las etapas. Revisa los mensajes de error anteriores.")


        # --- Limpieza de Directorio Temporal y Reinicio ---
        st.markdown("---")
        col1, col2 = st.columns([0.3, 0.7])
        with col1:
            if st.button("🧹 Limpiar Temporales y Reiniciar Aplicación", key="cleanup_button"):
                st.session_state.cleanup_triggered = True # Activar el flag
                st.rerun() # Forzar un rerun para que la lógica de limpieza se ejecute

        # Lógica de limpieza que se ejecuta en el rerun siguiente al click del botón
        if st.session_state.cleanup_triggered:
            with col2:
                st.info("Iniciando limpieza y reinicio...")
            try:
                if st.session_state.get('temp_dir') and os.path.exists(st.session_state.temp_dir):
                    with col2:
                        st.info(f"Borrando directorio temporal: {st.session_state.temp_dir}")
                    shutil.rmtree(st.session_state.temp_dir)
                    with col2:
                        st.success("Directorio temporal limpiado.")

                # Limpiar solo los estados relevantes para reiniciar la aplicación completamente
                # y establecer el nivel inicial.
                for key in list(st.session_state.keys()): # Iterar sobre una copia de las claves
                    del st.session_state[key]
                
                # Vuelve a inicializar el estado para que la aplicación se cargue fresca
                self._initialize_session_state() 

                with col2:
                    st.success("Estado de la aplicación reiniciado completamente.")
                st.session_state.cleanup_triggered = False # Resetear el flag
                st.rerun() # Forzar un rerun final para mostrar el estado inicial
            except Exception as e:
                with col2:
                    st.error(f"Error al limpiar el directorio temporal: {e}")
                st.session_state.cleanup_triggered = False


if __name__ == "__main__":
    app = ApoloApp()
    app.run()