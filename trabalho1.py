import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator, TriggerDagRunLink
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

URL = "/tmp/tabela_unica.csv"

default_args = {
    'owner'          : "Alice",
    "depends_on_past": False,
    'start_date'     : datetime (2022, 10, 10)
}

@dag(default_args      = default_args, 
     description       = "Trabalho final - parte 2",
     schedule_interval = None, 
     catchup           = False, 
     tags              = ['Trabalho_Final','Titanic'])

def segundaParte():

    @task
    def buscaDados():
        df = pd.read_csv(URL, sep=";")
        df.to_csv(URL, index=False, header=True, sep=";")
        return URL

    @task
    def geraTabela(URL):
        TABELA_RESULTADOS = "/tmp/resultados.csv"
        df  = pd.read_csv(URL, sep=";")
        res = df.groupby(['Sex']).agg({"PassengerId":"mean", "Fare":"mean", "parentes":"mean"}).reset_index()
        print(res)
        res.to_csv(TABELA_RESULTADOS, index=False, sep=";")
        return TABELA_RESULTADOS

    start = EmptyOperator(task_id="inicio")
    end   = EmptyOperator(task_id="fim")

    unica      = buscaDados()
    resultados = geraTabela(unica)

    start >> unica >> resultados >> end
 
execucao = segundaParte()