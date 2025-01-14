from setuptools import setup, find_packages

# Leer el contenido del archivo README.md
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="Extreme event analysis",  # Reemplaza con el nombre de tu proyecto
    version="0.1.0",  # Versión de tu proyecto
    author="José Álvaro Sañudo Díaz",  # Tu nombre
    description="Análisis de eventos extremos utilizando datos meteorológicos y técnicas de machine learning",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sanyudo/extreme_event_analysis",  # URL del repositorio de tu proyecto
    packages=find_packages(),  # Encuentra automáticamente todos los paquetes y subpaquetes
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Not set",  # Reemplaza con la licencia que uses
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',  # Versión mínima de Python requerida
    install_requires=[
        "pandas",
        "dash",
        "dash-leaflet",
        "dash-bootstrap-components",
        "requests",
        "shapely",
        "tenacity",
        # Añade aquí otras dependencias de tu proyecto
    ],
    entry_points={
        'console_scripts': [
            'nombre_comando=modulo_principal:funcion_principal',  # Reemplaza con tu comando y función principal
        ],
    },
    include_package_data=True,  # Incluye archivos adicionales especificados en MANIFEST.in
)