import streamlit as st
import pandas as pd
import numpy as np
import json
import requests

# import pickle
# import os
# import plotly.express as px
from snowflake.snowpark.context import get_active_session

@st.cache_data
def get_xgboost_test_data():
    with session.file.get_stream("@my_streamlit.public.streamlit_stage/testapi_xgb_v1.csv") as file:
        df = pd.read_csv(file)
    return df

@st.cache_data
def get_dropdowns_info():
    canales = pd.DataFrame(session.table("canales_uniques").to_pandas())
    agencia = pd.DataFrame(session.table("agencia_uniques").to_pandas())
    tipo_habitacion = pd.DataFrame(session.table("tipo_habitacion_uniques").to_pandas())
    return (canales, agencia, tipo_habitacion)

def dummify(df_list: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(
        [
            pd.get_dummies(df) for df in df_list
        ],
        axis= 1
    )

def send_data_to_api(url: str,
                     data: pd.DataFrame
                    ) -> pd.DataFrame:

    #Formateado para la API:
    data_json = json.dumps({"data": [data.to_dict(orient='list')]})
    
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, data=data_json, headers=headers)

        if response.status_code == 200:
          result = json.loads(response.json())
          data["Predicted"] = result
        else:
          st.text(f"Error: {response.text}")
    except Exception as e:
        st.text(f"Error: {e}")
    
    
    
st.set_page_config(page_title="Dashboard Ocupación Hotelera", layout="wide")

session = get_active_session()
df = session.table("MY_STREAMLIT.public.reservas_processed")
# st.dataframe(data=df, use_container_width=True)
# df.columns = pd.Series(df.columns).str.lower()

ds_uniques = session.table("MY_STREAMLIT.public.ds_uniques")
ocupacion_total_metrics = session.table("MY_STREAMLIT.public.ocupacion_total_metrics")

# st.dataframe(ds_uniques)
#st.stop()

# Agregar otra tab para hacer predicciones
tab1, tab2 = st.tabs(["📈 Dashboard Ocupación Hotelera", "🔮 Predicciones"])
with tab1:
    st.title("📊 Dashboard Interactivo - Ocupación Hotelera")

    st.subheader("🔍 Resumen de métricas clave")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total días", ds_uniques.collect()[0]["COUNT"])

    col2.metric("Ocupación total", ocupacion_total_metrics.collect()[0]["SUMA"])
    col3.metric("Promedio diario", ocupacion_total_metrics.collect()[0]["PROMEDIO"])
    col4.metric("Máximo diario", ocupacion_total_metrics.collect()[0]["MAXIMO"])

    st.markdown("---")

    st.subheader("📈 Ocupación total por día")
    st.line_chart(df, x='DS', y='OCUPACION_TOTAL')

    st.subheader("📊 Ocupación promedio por mes")
    ocupacion_mes = session.table("MY_STREAMLIT.public.ocupacion_mensual")
    st.bar_chart(ocupacion_mes, x='MES', y='OCUPACION_MENSUAL_PROMEDIO')

    st.subheader("Predicción: XGBoost (API)")
    xgb_api_data = get_xgboost_test_data()
    # st.dataframe(xgb_data)
    xgb_api_btn = st.button("Predecir con XGBoost")

    if xgb_api_btn:
        xgb_api_pred = send_data_to_api(url= 'http://d1dad3e6-7f1c-49b9-9799-9f81b67ba506.centralindia.azurecontainer.io/score',
                                    data= xgb_api_data.copy())
        st.dataframe(xgb_api_pred)

    # try:
    #     r = requests.get("https://httpbin.org/get")
    #     st.write("HTTPBin status:", r.status_code)
    #     st.json(r.json())
    # except Exception as e:
    #     st.error(f"Falló la llamada: {e}")

    st.subheader("Predicción: XGBoost (Pickle)")
    xgb_btn = st.button("Predecir con XGboost")

    if xgb_btn:
        xgb_df = pd.DataFrame(
            session.sql("SELECT * FROM TABLE(xgb_predict())").to_pandas()
        )
        st.dataframe(xgb_df)
        st.line_chart(xgb_df, x='FECHA', y='RESULT')

    st.subheader("Predicción: LSTM (Pickle)")
    st.text('Muy Pronto!')
    #lstm_btn = st.button("Predecir con LSTM")

    #if lstm_btn:
    #    lstm_df = pd.DataFrame(
    #        session.sql("SELECT * FROM TABLE(lstm_predict())").to_pandas()
    #    )
    #    st.dataframe(lstm_df)

    st.subheader("📄 Datos diarios")
    st.dataframe(df)

    #data = pickle.load(
    #    session.file.get_stream("@my_streamlit.public.streamlit_stage/data.pkl")
    #)
    #xgb = pickle.load(
    #    session.file.get_stream("@my_streamlit.public.streamlit_stage/xgboost_model_results.pkl")
    #)
    #lstm = pickle.load(
    #    session.file.get_stream("@my_streamlit.public.streamlit_stage/lstm_model_results.pkl")
    #)

    # st.text(data)    


with tab2:
    st.title("🔮 Predicciones de Ocupación Hotelera")

    canales_uniques, agencia_uniques, tipo_habitacion_uniques = get_dropdowns_info()

    with st.form(key= "formulario_predicciones"):
        # Aquí puedes agregar los elementos de la segunda pestaña para hacer predicciones
        fecha = st.date_input("Selecciona una fecha para predecir la ocupación", value=pd.to_datetime("2020-01-01"))
        mes = fecha.month
        dia_semana = fecha.weekday()  # 0=Monday, 6=Sunday
        dia_mes = fecha.day
        dia_semana = fecha.weekday()  # 0=Monday, 6=Sunday
        canal = st.selectbox("Selecciona el canal mas usado:", canales_uniques)
        agencia = st.selectbox("Selecciona la agencia mas usada:", agencia_uniques)
        tipo_habitacion = st.selectbox("Selecciona el tipo de habitacion:", tipo_habitacion_uniques)
        submit = st.form_submit_button("Enviar")
        # canal = st.selectbox("Selecciona el canal mas usado:", df['canal'].unique())
        # agencia = st.selectbox("Selecciona la agencia mas usada:", df['agencia'].unique())
        # tipo_habitacion = st.selectbox("Selecciona el tipo de habitación mas usada:", df['tipo_habitacion_nombre'].unique())

    #st.text(canales_uniques.to_dict())
    #st.text(agencia_uniques.to_dict())
    #st.text(tipo_habitacion_uniques.to_dict())
    
    if submit:
        #df_pred = pd.DataFrame({
        #    'mes': mes, 
        #    'dia_semana': dia_semana,
        #    'dia': dia_mes,
        #    'canal': [canal], 
        #    'agencia': [agencia], 
        #    'tipo_habitacion': [tipo_habitacion]
       # })
        st.text(f"{mes}, {dia_semana} {dia_mes}")
        st.text("Muy pronto...")
        #st.dataframe(df_pred)
       # #st.dataframe(pd.get_dummies(
        #    df_pred,
        #    columns = ['canal', 'agencia', 'tipo_habitacion']
        #))

        #st.text(df_pred.to_dict())

        # predict con el uri
st.markdown("---")
st.markdown("Desarrollado por Masterchefs 👨‍🍳")
