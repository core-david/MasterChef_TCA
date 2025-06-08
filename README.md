# Time Series Forecasting Platform: XGBoost & LSTM with Streamlit + Snowflake  
**Plataforma de Predicción de Series Temporales con XGBoost y LSTM desplegada en Streamlit + Snowflake**

Este repositorio contiene la arquitectura general del proyecto, que incluye el análisis exploratorio de datos (EDA), análisis topológico (TDA), despliegue, y los modelos predictivos entrenados con XGBoost y LSTM.

---

## Modelos Utilizados

Entrenamos y evaluamos dos enfoques de modelado:

- **XGBoost**: Modelo basado en árboles de decisión optimizados con boosting.
- **LSTM**: Red neuronal recurrente enfocada en secuencias temporales.

Ambos modelos fueron entrenados por separado y sus artefactos (`.pkl`) fueron subidos a **Snowflake**, desde donde se leen en una aplicación **Streamlit**.

Cada archivo `.zip` (`lstm-main.zip` y `tca-xg-main.zip`) contiene:
- Código fuente del pipeline en Kedro
- README específico para ejecución local
- Integración con MLflow y Docker

---

## Visualización de Resultados

Los resultados se despliegan mediante una aplicación en **Streamlit**, conectada directamente a **Snowflake**.  
Desde esta interfaz, se pueden consultar:
- Predicciones generadas por ambos modelos
- Comparación de resultados
- Visualización de métricas relevantes

> Los modelos no se vuelven a entrenar desde la aplicación. Solo se cargan los modelos `.pkl` guardados en Snowflake.


## Despliegue

- El frontend fue desarrollado en **Streamlit**.
- El backend se apoya en **Snowflake**, que aloja los modelos `.pkl` y sirve los datos para predicción.
- Se puede extender fácilmente con nuevos modelos, visualizaciones o actualizaciones desde Kedro.

---

## Notas

- Los modelos se encuentran versionados en sus respectivos repositorios (`lstm-main.zip` y `tca-xg-main.zip`).
- No se incluyen datos crudos por motivos de privacidad.
- El TDA se documenta pero no fue integrado directamente en los modelos productivos.

---

## 👨‍🍳 Desarrollado por el equipo MasterChefs

- [Iván Ortiz](https://github.com/IvanAOrtiz)  
- [David Vargas](https://github.com/core-david)  
- [Mariano Luna](https://github.com/Elma-reano)  
- [Diego Garza](https://github.com/DiegoGarzaGzz)  
- [Franco Mendoza]()
