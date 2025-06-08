CREATE OR REPLACE FUNCTION xgb_predict()
RETURNS TABLE(fecha date, Result FLOAT, extra TEXT)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('pandas','numpy', 'joblib')
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
ARTIFACT_REPOSITORY_PACKAGES = ('scikit-learn', 'tensorflow', 'keras', 'xgboost')
IMPORTS = ('@streamlit_stage/xg_model.pkl', '@streamlit_stage/xg_data.csv')
HANDLER = 'MainTable'
AS
$$
import pandas as pd
import pickle
import sys
import os
import json
import joblib

def get_table():
    model = None
    import_dir = sys._xoptions.get("snowflake_import_directory")
    with open(os.path.join(import_dir, 'xg_model.pkl'), 'rb') as file:
        model = joblib.load(file)
    with open(os.path.join(import_dir, 'xg_data.csv'), 'r') as file:
        df = pd.read_csv(file)

    try:
        prediction = model.predict(df)
        df['fecha'] = (
            pd.Series(df.index).apply(lambda x: '2019-' if x < 322 else '2020-') +
            df['mes'].astype(str) + '-' +
            df['dia'].astype(str)
        )
        df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d')
        #df['fecha'] = df['mes'].astype(str) + ' - ' + df['dia'].astype(str)
        df['result'] = [i for i in prediction]
        df['extra'] = 'ok'
        return df
    except Exception as e:
        df['result'] = '99.99'
        df['extra'] = e
        return df
        

class MainTable:
    def process(self):
        df = get_table()
        for row in df.itertuples(index= False):
            yield (row.fecha, row.result, row.extra)
$$;

--SELECT * FROM TABLE(xgb_predict());

CREATE OR REPLACE FUNCTION xgb_predict_api()
  RETURNS TABLE(fecha DATE, Result FLOAT)
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.10
  PACKAGES = ('pandas', 'numpy', 'requests')
  IMPORTS = ('@streamlit_stage/xg_data.csv')
  HANDLER = 'MainTable'
  EXTERNAL_ACCESS_INTEGRATIONS = (AZURE_CALL_INTEGRATION)
  AS
  $$
import json
import requests
import numpy as np
import pandas as pd
import sys
import os

def send_data_to_api(url: str,
                     data: pd.DataFrame
                    ) -> pd.DataFrame:
    
    
    # Formatear para la API
    data_json = json.dumps({'data': [
       data.to_dict(orient='list')
    ]})

    # header de la solicitud
    headers= {'Content-Type': 'application/json'}
    # solicitud
    response = requests.post(
        url,
        data= data_json,
        headers= headers
    )

    # si la solicitud es exitosa, se agregan los resultados al df
    if response.status_code == 200:
        result = json.loads(response.json())
        # print(result)
        data.loc[:, 'result'] = result
        # display(data)
        return data
    else:
        # print(f"Error: {response.text}")
        pass
    return

def get_table():
        
    import_dir = sys._xoptions.get("snowflake_import_directory")
    with open(os.path.join(import_dir, 'xg_data.csv'), 'r') as file:
        df = pd.read_csv(file)

    xgb_uri = 'http://4d367d9c-7ff5-4ca4-87d0-4747cb612a1c.centralindia.azurecontainer.io/score'
    df_pred = send_data_to_api(xgb_uri, df.copy())
    df_pred['date'] = (
        pd.Series(df_pred.index).apply(lambda x: '2019-' if x < 322 else '2020-') +
        df_pred['mes'].astype(str) + '-' +
        df_pred['dia'].astype(str)
    )
    return df_pred[['date', 'result']]
    # return df_pred[['date', 'result']].itertuples(index=False, name=None)

class MainTable:
    def process(self):
        df = get_table()
        for row in df.itertuples(index= False):
            yield (row.date, row.result)
$$;

--SELECT * FROM TABLE(xgb_predict());

CREATE OR REPLACE FUNCTION lstm_predict()
RETURNS TABLE(result FLOAT)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('pandas','numpy', 'joblib')
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
ARTIFACT_REPOSITORY_PACKAGES = ('scikit-learn', 'tensorflow', 'keras')
IMPORTS = ('@streamlit_stage/lstm_model.pkl', '@streamlit_stage/lstm_data.csv')
HANDLER = 'MainTable'
AS
$$
import pandas as pd
import pickle
import sys
import os
import json
import joblib

def get_data():
    model = None
    import_dir = sys._xoptions.get("snowflake_import_directory")
    with open(os.path.join(import_dir, 'lstm_model.pkl'), 'rb') as file:
        model = joblib.load(file)
    with open(os.path.join(import_dir, 'lstm_data.csv'), 'r') as file:
        df = pd.read_csv(file)

    try:
        prediction = model.predict(df['ocupacion_rolling_7d'])
        return {'result': [i[0] for i in prediction]}
    except Exception as e:
        return json.dumps({'error': str(e)})

class MainTable:
    def process(self):
        df = pd.DataFrame(get_data())
        for row in df.itertuples(index=False):
            yield(row.result,)
$$;

--SELECT * FROM TABLE(lstm_predict());
