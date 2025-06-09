import pandas as pd
import numpy as np
import basedosdados as bd
import os

#Extração dos Dados
query = """
  SELECT *
  FROM `basedosdados.br_inpe_queimadas.microdados` AS dados
  WHERE dados.sigla_uf = "PB"
"""
df = bd.read_sql(query, billing_project_id="seu_billing_project_id")

#Transformação dos Dados
df.loc[df["dias_sem_chuva"] < 0, "dias_sem_chuva"] = 0
df.loc[df["precipitacao"] < 0, "precipitacao"] = 0
df.loc[df["risco_fogo"] < 0, "risco_fogo"] = 0
df.loc[df["potencia_radiativa_fogo"] < 0, "potencia_radiativa_fogo"] = 0

#Substituindo valores nulos pela média do grupo de cidade e mês,
#pois preserva variações locais, tornando o preenchimento mais preciso e consistente.
df["dias_sem_chuva"] = df.groupby(["id_municipio", "mes"])["dias_sem_chuva"].transform(lambda x: x.fillna(int(x.mean())) if not x.isna().all() else 0).astype(int)
colunas = ["risco_fogo", "precipitacao", "potencia_radiativa_fogo"]
for col in colunas:
    df[col] = df.groupby(["id_municipio", "mes"])[col].transform(lambda x: x.fillna(x.mean()))

for col in colunas:
    df[col] = df[col].fillna(df[col].mean())

#Alterando a coluna de data_hora para o formato de data
df["data_hora"] = pd.to_datetime(df["data_hora"])
df["data_hora"] = df["data_hora"].dt.date
df.rename(columns={
    "data_hora": "data"
}, inplace=True, errors="ignore")

#Retirando colunas desnecessárias
df.drop(columns=[
    "sigla_uf",
    "satelite"
], inplace=True, errors="ignore")

#Carregamento dos Dados
caminho_arquivo = "Dados/queimadas_pb.csv"
if os.path.exists(caminho_arquivo):
    df = pd.read_csv(caminho_arquivo)
    print("Arquivo lido com sucesso!")
else:
    df.to_csv(caminho_arquivo, index=False)
    print("Dados carregados com sucesso!")