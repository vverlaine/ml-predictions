# Changelog

Todas las notas importantes de este proyecto serán documentadas en este archivo.

## [1.0.0] - 2024-07-05

### Añadido

- Creación inicial del proyecto de Machine Learning con estructura de carpetas organizada.
- Funciones de carga, preprocesamiento y visualización de datos en el directorio `src`.
- Funciones de entrenamiento y evaluación de modelos en el directorio `src/models`.
- Scripts de Jupyter Notebook en el directorio `notebooks`.
- Script de configuración `setup.sh` para la creación del entorno virtual y la instalación de dependencias.
- Inclusión de dependencias en el archivo `requirements.txt`.
- README detallado para describir la estructura y funcionalidad del proyecto.
- Archivo `.gitignore` para excluir archivos innecesarios del control de versiones.
- Alias para la activación del entorno virtual en `~/.zshrc`.
- Inclusión de las dependencias opcionales `PySpark` y `Jupyter Notebook`.

### Cambiado

- Actualización del script `setup.sh` para manejar la instalación opcional de dependencias.
- Mejoras en el script `setup.sh` para copiar correctamente la estructura del proyecto al nuevo directorio seleccionado.
- Inclusión de prompts de usuario en el script `setup.sh` para una configuración interactiva.
- Separación de dependencias seleccionadas por el usuario en `temp_requirements.txt` y posterior movimiento al nuevo entorno.

### Eliminado

- Eliminación de pasos innecesarios en el script `setup.sh` para simplificar el proceso de configuración.
- Eliminación de autenticación con GitHub en el script `setup.sh` para evitar conflictos de sesión.

## [0.1.0] - 2024-07-01

### Añadido

- Estructura inicial del proyecto.
- Inclusión del archivo `setup.sh` básico para la creación de entornos virtuales.
- Inclusión del archivo `requirements.txt` con dependencias básicas de Python.