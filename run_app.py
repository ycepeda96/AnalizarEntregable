import streamlit.web.cli as stcli
import os
import sys


def resolve_path(path):
    """Resuelve rutas relativas al directorio del ejecutable."""
    if hasattr(sys, "_MEIPASS"):
        # Si estamos en un ejecutable de PyInstaller, _MEIPASS es el directorio temporal
        return os.path.join(sys._MEIPASS, path)
    return os.path.abspath(os.path.join(os.getcwd(), path))


if __name__ == "__main__":
    # Importante: el nombre de tu archivo Streamlit debe ser el segundo argumento
    app_script_path = resolve_path("steamlit_Analizar_entregable.py")

    # Configura los argumentos para que Streamlit se ejecute correctamente
    sys.argv = [
        "streamlit",
        "run",
        app_script_path,
        "--global.developmentMode=false",  # Desactiva el modo desarrollo
        "--server.port=8501",  # Puedes cambiar el puerto si es necesario
        "--browser.gatherUsageStats=False",  # Desactiva el envío de estadísticas de uso
        "--server.headless=true"  # Esto puede ayudar en algunos entornos
    ]

    # Ejecuta Streamlit
    sys.exit(stcli.main())