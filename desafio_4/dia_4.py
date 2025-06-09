from pandera import Column, DataFrameSchema, Check
import pandas as pd
import requests as req


url = 'https://api.github.com/users/NandoSecOp/events'
try:
    resposta = req.get(url)
    if resposta.status_code == 200:
        dados = resposta.json()
        df = pd.json_normalize(dados)
        tipos_permitidos = ["PushEvent", "PullRequestEvent", "IssuesEvent"]
        df = df[df["type"].isin(tipos_permitidos)]

        schema = DataFrameSchema({
            "type": Column(str, Check.isin(tipos_permitidos), nullable=False),
            "created_at": Column(str, Check.str_matches(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"), nullable=False),
            "repo.name": Column(str, nullable=True),
        })

        try:
            schema.validate(df)
            print('Dados válidos!')
        except Exception as e:
            print('Dados inválidos!')
            print(f'Erro na validação: {e}')

except Exception as e:
    print(f'Erro na requisição: {e}')
