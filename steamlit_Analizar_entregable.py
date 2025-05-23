# streamlit_optimized.py
import streamlit as st
import os
import zipfile
import re
import tempfile
import shutil
import rarfile
import subprocess
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set
from enum import Enum
import logging
from contextlib import contextmanager
import functools

# === CONFIGURACIÓN Y CONSTANTES ===
class FileExtension(Enum):
    SQL = '.sql'
    PKS = '.pks'
    PKB = '.pkb'
    PRC = '.prc'
    FNC = '.fnc'
    VW = '.vw'
    TRG = '.trg'
    SEQ = '.seq'

VALID_EXTS = {ext.value for ext in FileExtension}
SQL_SPECIFIC_FOLDERS = {"scripts", "grants", "opciones", "indices", "tabla", "sequence"}

@dataclass
class FileData:
    absolute_path: str
    relative_path_from_extracted: str
    parent_folder_name: str
    prefix_num: float
    extension: str
    filename: str

@dataclass
class AnalysisResult:
    findings: Dict[str, List[str]] = field(default_factory=dict)
    ordered_files: List[str] = field(default_factory=list)
    files_data: List[FileData] = field(default_factory=list)
    total_issues: int = 0

@dataclass
class ManifestCategory:
    header: str
    extensions: Set[str]
    destination_folder: str
    specific_folders: Set[str] = field(default_factory=set)
    format_per_folder: bool = False

# === CONFIGURACIÓN DE MANIFIESTO ===
MANIFEST_CATEGORIES = {
    "scripts": ManifestCategory(
        header="-- Ejecucion de scripts sql",
        extensions={FileExtension.SQL.value},
        destination_folder="scripts",
        specific_folders=SQL_SPECIFIC_FOLDERS,
        format_per_folder=True
    ),
    "packages": ManifestCategory(
        header="-- Ejecucion de script creacion de packages",
        extensions={FileExtension.PKS.value},
        destination_folder="packages"
    ),
    "packagesbodies": ManifestCategory(
        header="-- Ejecucion de script creacion de packagesBodies",
        extensions={FileExtension.PKB.value},
        destination_folder="packagesbodies"
    ),
    "procedures": ManifestCategory(
        header="-- Ejecucion de script creacion de procedures",
        extensions={FileExtension.PRC.value},
        destination_folder="procedures"
    ),
    "functions": ManifestCategory(
        header="-- Ejecucion de script creacion de funciones",
        extensions={FileExtension.FNC.value},
        destination_folder="functions"
    ),
    "views": ManifestCategory(
        header="-- Ejecucion de script creacion de views",
        extensions={FileExtension.VW.value},
        destination_folder="views"
    ),
    "triggers": ManifestCategory(
        header="-- Ejecucion de script creacion de triggers",
        extensions={FileExtension.TRG.value},
        destination_folder="triggers"
    )
}

# === CONFIGURACIÓN DE LOGGING ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === DECORADORES Y UTILIDADES ===
def error_handler(func):
    """Decorador para manejo centralizado de errores"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error en {func.__name__}: {e}")
            st.error(f"❌ Error en {func.__name__}: {e}")
            return None
    return wrapper

@contextmanager
def temp_directory():
    """Context manager para manejo seguro de directorios temporales"""
    temp_dir = tempfile.mkdtemp(prefix='streamlit_db_')
    try:
        yield temp_dir
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

# === CLASES PRINCIPALES ===
class SessionManager:
    """Gestor centralizado del estado de la sesión"""
    
    @staticmethod
    def initialize_session():
        """Inicializa todos los estados necesarios"""
        defaults = {
            'level': 1,
            'temp_dir': None,
            'archive_extracted': False,
            'analysis_done': False,
            'analysis_result': AnalysisResult(),
            'last_uploaded_filename': None,
            'repo_path_input': "",
            'schema_directories': [],
            'selected_schema': None,
            'branch_name_input': ""
        }
        
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value
    
    @staticmethod
    def reset_analysis():
        """Resetea solo los datos de análisis"""
        keys_to_reset = [
            'archive_extracted', 'analysis_done', 'analysis_result',
            'last_uploaded_filename'
        ]
        for key in keys_to_reset:
            if key in st.session_state:
                if key == 'analysis_result':
                    st.session_state[key] = AnalysisResult()
                else:
                    st.session_state[key] = None if 'filename' in key else False

class ArchiveExtractor:
    """Clase especializada en extracción de archivos"""
    
    @staticmethod
    @error_handler
    def is_unrar_available() -> bool:
        """Verifica disponibilidad de unrar de forma más eficiente"""
        try:
            result = subprocess.run(
                ["unrar"], 
                stdout=subprocess.DEVNULL, 
                stderr=subprocess.DEVNULL, 
                timeout=5,
                check=False
            )
            return result.returncode != 127  # 127 = command not found
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False
    
    @staticmethod
    @error_handler
    def extract_archive(archive_path: str, dest_dir: str) -> bool:
        """Extrae archivos con mejor manejo de errores"""
        ext = Path(archive_path).suffix.lower()
        
        try:
            if ext == '.zip':
                with zipfile.ZipFile(archive_path, 'r') as z:
                    z.extractall(dest_dir)
            elif ext == '.rar':
                if not ArchiveExtractor.is_unrar_available():
                    raise EnvironmentError("unrar no disponible")
                with rarfile.RarFile(archive_path) as r:
                    r.extractall(dest_dir)
            else:
                raise ValueError(f"Formato no soportado: {ext}")
            
            return True
        except (zipfile.BadZipFile, rarfile.BadRarFile) as e:
            st.error(f"❌ Error: Archivo corrupto o no válido. Detalle: {e}")
            return False
        except EnvironmentError as e:
            st.error(f"❌ Error de entorno: {e}")
            st.error("Por favor, asegúrate de que 'unrar' esté instalado y accesible en tu sistema si intentas extraer archivos .rar.")
            return False
        except Exception as e:
            st.error(f"❌ Ocurrió un error inesperado durante la extracción: {e}")
            logger.error(f"Error inesperado en extract_archive: {e}", exc_info=True)
            return False

class FileAnalyzer:
    """Clase especializada en análisis de archivos"""
    
    @staticmethod
    def numeric_key(s: str) -> float:
        """Extrae número inicial para ordenamiento"""
        match = re.match(r"(\d+)", s)
        return int(match.group(1)) if match else float('inf')
    
    @staticmethod
    @error_handler
    def collect_files(root_dir: str) -> Tuple[List[str], List[FileData]]:
        """Recolecta y ordena archivos de forma más eficiente"""
        ordered_files = []
        files_data = []
        root_path = Path(root_dir)
        
        for file_path in root_path.rglob('*'):
            if not file_path.is_file():
                continue
                
            # Ignorar carpetas rollback
            if any('rollback' in part.lower() for part in file_path.parts):
                continue
                
            ext = file_path.suffix.lower()
            if ext not in VALID_EXTS:
                continue
            
            relative_path = file_path.relative_to(root_path)
            prefix_num = FileAnalyzer.numeric_key(file_path.name)
            
            # Para lista ordenada
            ordered_files.append(str(relative_path.as_posix()))
            
            # Para datos detallados
            file_data = FileData(
                absolute_path=str(file_path),
                relative_path_from_extracted=str(relative_path.as_posix()),
                parent_folder_name=file_path.parent.name,
                prefix_num=prefix_num,
                extension=ext,
                filename=file_path.name
            )
            files_data.append(file_data)
        
        # Ordenar ambas listas
        ordered_files.sort(key=lambda x: (Path(x).parent.as_posix(), 
                                        FileAnalyzer.numeric_key(Path(x).name)))
        files_data.sort(key=lambda x: (Path(x.relative_path_from_extracted).parent.as_posix(),
                                     x.prefix_num, x.filename))
        
        return ordered_files, files_data
    
    @staticmethod
    def check_slash_terminators(lines: List[str], ext: str) -> List[str]:
        """Verifica terminadores '/' de forma más eficiente"""
        if ext not in {'.pks', '.pkb', '.prc', '.fnc', '.trg'}:
            return []
        
        # Compilar patrón una sola vez
        end_pattern = re.compile(r'END(\s+\w+)?;\s*$', re.IGNORECASE)
        
        # Buscar último END desde el final
        last_end_index = -1
        for i in range(len(lines) - 1, -1, -1):
            if end_pattern.search(lines[i]):
                last_end_index = i
                break
        
        if last_end_index == -1:
            return []
        
        # Verificar líneas siguientes
        j = last_end_index + 1
        while j < len(lines):
            line = lines[j].strip()
            if not line or line.startswith(('--', '/*')):
                j += 1
                continue
            
            if line != '/':
                return [f"Línea {last_end_index+1}: Falta '/' después del bloque END;"]
            break
        
        if j == len(lines):
            return [f"Línea {last_end_index+1}: Falta '/' después del bloque END;"]
        
        return []
    
    @staticmethod
    @error_handler
    def analyze_file(file_path: str, ext: str) -> List[str]:
        """Analiza un archivo individual"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            return FileAnalyzer.check_slash_terminators(lines, ext)
        except Exception as e:
            return [f"Error leyendo archivo: {e}"]

class GitManager:
    """Clase para operaciones Git optimizadas"""
    
    @staticmethod
    @error_handler
    def run_git_command(repo_path: str, command: List[str], 
                       suppress_errors: bool = False) -> bool:
        """Ejecuta comandos Git de forma más segura"""
        try:
            result = subprocess.run(
                ["git"] + command,
                cwd=repo_path,
                check=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.stdout.strip():
                st.text(result.stdout.strip())
            if result.stderr.strip():
                st.text(result.stderr.strip())
                
            return True
            
        except subprocess.TimeoutExpired:
            if not suppress_errors:
                st.error("❌ Comando Git expiró (timeout)")
            return False
        except subprocess.CalledProcessError as e:
            if not suppress_errors:
                st.error(f"❌ Error Git: {' '.join(command)}")
                st.error(f"Salida: {e.stderr.strip()}")
            return False
        except FileNotFoundError:
            st.error("❌ Git no encontrado en PATH")
            return False
    
    @staticmethod
    def branch_exists(repo_path: str, branch_name: str) -> bool:
        """Verifica existencia de rama de forma más eficiente"""
        try:
            subprocess.run(
                ["git", "rev-parse", "--verify", branch_name],
                cwd=repo_path,
                check=True,
                capture_output=True,
                timeout=10
            )
            return True
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return False
    
    @staticmethod
    def create_and_checkout_branch(repo_path: str, branch_name: str) -> bool:
        """Crea y cambia a rama con mejor flujo"""
        steps = [
            ("Cambiando a main", ["checkout", "main"]),
            ("Actualizando main", ["pull"]),
            ("Limpiando archivos", ["clean", "-fdx"])
        ]
        
        for description, cmd in steps:
            st.info(f"🔄 {description}...")
            if not GitManager.run_git_command(repo_path, cmd, 
                                            suppress_errors=(description == "Limpiando archivos")):
                if description != "Limpiando archivos":
                    return False
        
        # Manejar creación/checkout de rama
        if GitManager.branch_exists(repo_path, branch_name):
            st.warning(f"Rama '{branch_name}' existe. Cambiando a ella...")
            success = GitManager.run_git_command(repo_path, ["checkout", branch_name])
        else:
            st.info(f"🌿 Creando rama '{branch_name}'...")
            success = GitManager.run_git_command(repo_path, ["checkout", "-b", branch_name])
        
        if success:
            st.success(f"✅ Rama '{branch_name}' lista")
        
        return success

class ManifestGenerator:
    """Clase para generación optimizada de manifiestos"""
    
    @staticmethod
    def get_manifest_category(file_data: FileData) -> Optional[str]:
        """Determina categoría de manifiesto de forma más eficiente"""
        ext = file_data.extension.lower()
        path_parts = Path(file_data.relative_path_from_extracted).parts
        
        # Verificar si está en carpeta script-like
        is_script_like = any(
            keyword.lower() in part.lower() 
            for part in path_parts 
            for keyword in SQL_SPECIFIC_FOLDERS
        )
        
        if is_script_like and ext in VALID_EXTS:
            return "scripts"
        
        # Categorizar por extensión
        for category_key, details in MANIFEST_CATEGORIES.items():
            if ext in details.extensions:
                return category_key
        
        return None
    
    @staticmethod
    def generate_manifest_content(schema_name: str, branch_name: str, 
                                files_data: List[FileData]) -> str:
        """Genera contenido de manifiesto optimizado"""
        lines = [f"SCHEMA={schema_name.upper()}", ""]
        schema_lower = schema_name.lower()
        
        # Agrupar por carpeta original
        files_by_folder = {}
        for file_data in files_data:
            folder = Path(file_data.relative_path_from_extracted).parent.as_posix()
            if folder not in files_by_folder:
                files_by_folder[folder] = []
            files_by_folder[folder].append(file_data)
        
        # Ordenar carpetas
        sorted_folders = sorted(files_by_folder.keys(), 
                              key=lambda x: FileAnalyzer.numeric_key(Path(x).name))
        
        first_block = True
        
        for folder in sorted_folders:
            folder_files = files_by_folder[folder]
            
            # Agrupar por categoría dentro de carpeta
            files_by_category = {cat: [] for cat in MANIFEST_CATEGORIES.keys()}
            for file_data in folder_files:
                category = ManifestGenerator.get_manifest_category(file_data)
                if category:
                    files_by_category[category].append(file_data)
            
            added_header = False
            
            for category_key, details in MANIFEST_CATEGORIES.items():
                category_files = files_by_category[category_key]
                if not category_files:
                    continue
                
                # Agregar línea en blanco antes del primer bloque de la carpeta
                if not first_block and not added_header:
                    lines.append("")
                
                lines.append(details.header)
                added_header = True
                
                # Ordenar archivos en categoría
                if category_key in ["packages", "packagesbodies"]:
                    category_files.sort(key=lambda x: (
                        x.extension.lower() != ".pks",
                        x.prefix_num,
                        x.filename
                    ))
                else:
                    category_files.sort(key=lambda x: (x.prefix_num, x.filename))
                
                # Generar líneas de manifiesto
                for file_data in category_files:
                    filename = file_data.filename
                    category_details = MANIFEST_CATEGORIES.get(category_key)
                    if category_details:
                        type_folder_name_in_manifest = category_details.destination_folder
                    else:
                        type_folder_name_in_manifest = "unknown"
                    manifest_file_path = Path("database", "plsql", schema_lower, type_folder_name_in_manifest, filename).as_posix()
                    manifest_line = f"{manifest_file_path}"
                    lines.append(manifest_line)
            
            first_block = False
        
        return "\n".join(lines)

class FileManager:
    """Clase para operaciones de archivos optimizadas"""
    
    @staticmethod
    @error_handler
    def copy_files_to_repo(files_data: List[FileData], repo_path: str, 
                          schema_name: str) -> bool:
        """Copia archivos al repositorio de forma más eficiente"""
        schema_lower = schema_name.lower()
        copied_count = 0
        base_path = Path(repo_path) / "database" / "plsql" / schema_lower
        
        for file_data in files_data:
            ext = file_data.extension.lower()
            
            # Buscar la categoría por extensión para obtener la carpeta de destino
            folder_name = None
            # Iterar sobre las categorías para encontrar una que coincida con la extensión
            for category_key, details in MANIFEST_CATEGORIES.items():
                if ext in details.extensions:
                     folder_name = details.destination_folder
                     break # Encontramos la carpeta, salimos del bucle

            if not folder_name:
                # Esto no debería pasar si VALID_EXTS y ALLOWED_EXTENSIONS_MANIFEST
                # están sincronizados y todos tienen una categoría asignada.
                st.warning(f"Archivo '{file_data.filename}' con extensión '{ext}' no tiene una carpeta de destino definida, no será copiado.")
                continue

            dest_dir = base_path / folder_name
            dest_dir.mkdir(parents=True, exist_ok=True)
            
            src_path = Path(file_data.absolute_path)
            dest_path = dest_dir / src_path.name
            
            shutil.copy2(src_path, dest_path)
            copied_count += 1
        
        st.success(f"✅ {copied_count} archivos copiados")
        return True
    
    @staticmethod
    @error_handler
    def write_manifest(repo_path: str, branch_name: str, schema_name: str, 
                      content: str) -> bool:
        """Escribe manifiesto de forma más robusta"""
        manifest_dir = Path(repo_path) / "database" / "data" / schema_name.upper() / branch_name.upper()
        
        # Limpiar directorio existente
        if manifest_dir.exists():
            shutil.rmtree(manifest_dir)
        
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = manifest_dir / "manifest.txt"
        
        with open(manifest_path, "w", encoding="utf-8") as f:
            f.write(content)
        
        st.success(f"✅ Manifiesto generado: {manifest_path.relative_to(repo_path)}")
        return True

# === FUNCIONES DE UTILIDAD ===
@error_handler
def get_schema_directories(repo_path: str) -> List[str]:
    """Obtiene directorios de esquema de forma más eficiente"""
    if not repo_path:
        return []
    
    schema_path = Path(repo_path) / "database" / "plsql"
    if not schema_path.is_dir():
        return []
    
    try:
        return sorted([
            d.name for d in schema_path.iterdir() 
            if d.is_dir() and not d.name.startswith('.')
        ])
    except Exception:
        return []

def validate_inputs(repo_path: str, schema: str, branch_name: str) -> Tuple[bool, bool, bool, bool]:
    """Valida todos los inputs de forma centralizada"""
    repo_valid = repo_path and Path(repo_path).is_dir()
    schema_valid = schema is not None
    branch_clean = branch_name.strip()
    branch_valid = (branch_clean.upper().startswith("F_") and 
                   " " not in branch_clean and len(branch_clean) > 2)
    level_2_valid = repo_valid and schema_valid and branch_valid
    
    return repo_valid, schema_valid, branch_valid, level_2_valid

# === INTERFAZ STREAMLIT OPTIMIZADA ===
def render_level_1():
    """Renderiza el nivel 1 de forma optimizada"""
    st.header("🎮 Nivel 1: Análisis de Scripts")
    st.write("Sube un archivo ZIP o RAR para analizar. ¡Supera este nivel corrigiendo todos los fallos!")
    
    uploaded_file = st.file_uploader("Archivo ZIP o RAR", type=["zip", "rar"], key="uploader_lvl1")
    
    if not uploaded_file:
        return
    
    # Verificar si es nuevo archivo
    if (st.session_state.last_uploaded_filename != uploaded_file.name or 
        not st.session_state.archive_extracted):
        
        SessionManager.reset_analysis()
        st.session_state.last_uploaded_filename = uploaded_file.name
        
        # Crear directorio temporal
        if st.session_state.temp_dir:
            shutil.rmtree(st.session_state.temp_dir, ignore_errors=True)
        st.session_state.temp_dir = tempfile.mkdtemp(prefix='streamlit_db_')
        
        # Procesar archivo
        archive_path = Path(st.session_state.temp_dir) / uploaded_file.name
        with open(archive_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.info("📦 Archivo subido, extrayendo...")
        
        if ArchiveExtractor.extract_archive(str(archive_path), st.session_state.temp_dir):
            st.session_state.archive_extracted = True
            st.success("✅ Archivo extraído")
            
            st.info("🔍 Analizando archivos...")
            ordered_files, files_data = FileAnalyzer.collect_files(st.session_state.temp_dir)
            
            if not ordered_files:
                st.warning("⚠️ No se encontraron archivos válidos")
                st.session_state.analysis_done = True
                return
            
            # Realizar análisis
            findings = {}
            for file_rel_path in ordered_files:
                full_path = Path(st.session_state.temp_dir) / file_rel_path
                ext = full_path.suffix.lower()
                findings[file_rel_path] = FileAnalyzer.analyze_file(str(full_path), ext)
            
            # Actualizar resultado
            result = AnalysisResult(
                findings=findings,
                ordered_files=ordered_files,
                files_data=files_data,
                total_issues=sum(len(issues) for issues in findings.values())
            )
            
            st.session_state.analysis_result = result
            st.session_state.analysis_done = True
            st.success("✅ Análisis completado")
        else:
            st.error("❌ Error extrayendo archivo")
    
    # Mostrar resultados si están disponibles
    if st.session_state.analysis_done:
        result = st.session_state.analysis_result
        
        st.subheader("Reporte de Análisis")
        
        # Sección 1: Archivos identificados
        st.markdown("#### SECCIÓN 1: Archivos identificados")
        if result.ordered_files:
            for file_path in result.ordered_files[:10]:  # Mostrar solo primeros 10
                st.write(f"- `{file_path}`")
            if len(result.ordered_files) > 10:
                st.write(f"... y {len(result.ordered_files) - 10} archivos más")
        else:
            st.info("ℹ️ No se encontraron archivos válidos")
        
        # Sección 2: Análisis detallado
        st.markdown("#### SECCIÓN 2: Análisis de terminadores '/'")
        files_with_issues = {k: v for k, v in result.findings.items() if v}
        
        if files_with_issues:
            for file_path, issues in files_with_issues.items():
                st.markdown(f"##### `{Path(file_path).name}`")
                for issue in issues:
                    st.warning(f"⚠️ {issue}")
        else:
            st.info("🎉 No se encontraron fallos de terminación")
        
        # Resultado final
        if result.total_issues == 0:
            st.success("✅ ¡Nivel 1 Superado! No hay fallos.")
            st.session_state.level = 2
        else:
            st.error(f"❌ Se encontraron {result.total_issues} fallos. Corrígelos antes de continuar.")

def render_level_2_and_3():
    """Renderiza los niveles 2 y 3 de forma optimizada"""
    current_level = st.session_state.level
    st.markdown("---")
    st.header(f"🎯 Nivel {current_level}: Preparación para Azure DevOps")
    
    if current_level == 2:
        st.write("Configura repositorio, esquema y rama. ¡Completa correctamente para pasar al Nivel 3!")
    else:
        st.write("¡Inputs validados! Ejecuta el proceso completo. 💪")
    
    # Input 1: Ruta del repositorio
    repo_path = st.text_input(
        "Ruta del Repositorio Local:",
        value=st.session_state.repo_path_input,
        help="Ruta absoluta al directorio del repositorio Git",
        key="repo_path_input"
    )
    st.session_state.repo_path_input = repo_path
    
    # Input 2: Selección de esquema
    schema_dirs = get_schema_directories(repo_path) if repo_path else []
    if schema_dirs != st.session_state.schema_directories:
        st.session_state.schema_directories = schema_dirs
        # Auto-seleccionar DBAPER si está disponible
        if "DBAPER" in schema_dirs and st.session_state.selected_schema not in schema_dirs:
            st.session_state.selected_schema = "DBAPER"
        st.rerun()
    
    schema_options = ["-- Selecciona un esquema --"] + schema_dirs
    selected_idx = 0
    if st.session_state.selected_schema in schema_dirs:
        selected_idx = schema_options.index(st.session_state.selected_schema)
    
    selected_schema_idx = st.selectbox(
        "Esquema de Base de Datos:",
        range(len(schema_options)),
        format_func=lambda x: schema_options[x].upper() if x > 0 else schema_options[x],
        index=selected_idx,
        key="schema_selectbox",
        disabled=not bool(schema_dirs)
    )
    
    st.session_state.selected_schema = (
        schema_options[selected_schema_idx] if selected_schema_idx > 0 else None
    )
    
    # Input 3: Nombre de rama
    branch_name = st.text_input(
        "Nombre del Branch:",
        value=st.session_state.branch_name_input,
        help="Debe comenzar con 'F_', sin espacios. Se convertirá a mayúsculas.",
        key="branch_name_input"
    )
    st.session_state.branch_name_input = branch_name
    
    # Validación
    repo_valid, schema_valid, branch_valid, level_2_valid = validate_inputs(
        repo_path, st.session_state.selected_schema, branch_name
    )
    
    # Mostrar estado de validación
    with st.expander("Estado de Validación", expanded=current_level == 2):
        st.write(f"✅ Repositorio válido: {repo_valid}")
        st.write(f"✅ Esquema seleccionado: {schema_valid}")
        st.write(f"✅ Formato de rama válido: {branch_valid}")
        st.write(f"👉 Nivel 2 completo: {level_2_valid}")
    
    # Transición de niveles
    analysis_ok = (st.session_state.analysis_done and 
                  st.session_state.analysis_result.total_issues == 0)
    
    if analysis_ok and level_2_valid and current_level < 3:
        st.session_state.level = 3
        st.rerun()
    elif not level_2_valid and current_level == 3:
        st.session_state.level = 2
        st.rerun()
    
    # El botón está habilitado solo si se pasaron los Niveles 1 y 2 (es decir, si el nivel actual es 3)
    disable_button = not (current_level == 3)

    # Botón de acción (Nivel 3)
    if st.button("🚀 Crear Rama, Copiar Archivos y Generar Manifiesto", disabled=disable_button):
        st.info("🛠️ Iniciando proceso de Azure DevOps (Nivel 3)...")

        repo_path = st.session_state.repo_path_input.strip()
        branch_name = st.session_state.branch_name_input.strip().upper() # Usar mayúsculas para el nombre de la rama en Git
        schema_name = st.session_state.selected_schema # Usar el esquema seleccionado del dropdown
        temp_dir = st.session_state.temp_dir
        files_data_for_manifest = st.session_state.temp_extracted_files_data

        # Asegurarse de que los inputs son válidos justo antes de ejecutar (doble verificación)
        if not (repo_path and os.path.isdir(repo_path) and schema_name and branch_name.startswith("F_") and " " not in branch_name):
             st.error("❌ Error de validación interna antes de ejecutar el proceso. Por favor, revisa los inputs del Nivel 2.")
             st.session_state.level = 2 # Regresar al Nivel 2 si la validación falla aquí
             st.rerun() # Forzar rerun para actualizar la UI
        else:
             # Ejecutar los pasos del Nivel 3
             # 1. Crear y cambiar a la nueva rama
             if GitManager.create_and_checkout_branch(repo_path, branch_name):
                 # 2. Copiar archivos extraídos al repositorio
                 # Pasar el nombre del esquema (del dropdown) y los datos de los archivos
                 if FileManager.copy_files_to_repo(files_data_for_manifest, repo_path, schema_name):
                      # 3. Generar el contenido del manifest.txt
                      manifest_content = ManifestGenerator.generate_manifest_content(schema_name, branch_name, files_data_for_manifest)
                      # 4. Escribir el manifest.txt en el repositorio
                      if FileManager.write_manifest(repo_path, branch_name, schema_name, manifest_content):
                           st.success("🥳🎉 ¡Proceso de Azure DevOps completado exitosamente! ¡Nivel 3 Superado!")
                           st.balloons()

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
if st.session_state.get('temp_dir') and os.path.exists(st.session_state.temp_dir):
     col1, col2 = st.columns([0.4, 0.6])
     with col1:
          if st.button("🧹 Limpiar Directorio Temporal y Reiniciar", key="cleanup_button"):
               try:
                    if st.session_state.get('temp_dir') and os.path.exists(st.session_state.temp_dir):
                         shutil.rmtree(st.session_state.temp_dir)
                         st.success("✨ Directorio temporal limpiado.")
                    # Limpiar solo los estados relevantes para reiniciar la aplicación
                    # NO BORRAR 'repo_path_input', 'branch_name_input', 'selected_schema'
                    keys_to_clear = [
                         'temp_dir', 'archive_extracted', 'analysis_done', 'analysis_result',
                         'last_uploaded_filename', 'level'
                    ]
                    for key in keys_to_clear:
                         if key in st.session_state:
                              del st.session_state[key]

                    st.success("✨ Estado de análisis y temporal reiniciado.")
                    st.rerun() # Forzar un rerun DESPUÉS de la limpieza y reseteo de estado
               except Exception as e:
                    st.error(f"❌ Error al limpiar el directorio temporal: {e}")

     with col2:
          st.info("Borra los archivos temporales extraídos y reinicia la aplicación a su estado inicial.")

# === Punto de entrada principal ===
# Inicializar el estado de la sesión al inicio
SessionManager.initialize_session()

# Renderizar la interfaz según el nivel actual
if st.session_state.level == 1:
    render_level_1()
elif st.session_state.level >= 2:
    render_level_2_and_3()
                  