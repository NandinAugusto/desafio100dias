import pandas as p
import requests
import os

dados_produtos = []
caminho = os.path.join(os.path.dirname(__file__), '..', 'arquivos_diversos')
caminho = os.path.abspath(caminho)

os.makedirs(caminho, exist_ok=True)


try:
    for i in range(1, 21):
        url = f"https://fakestoreapi.com/products/{i}"
        response = requests.get(url)
        
        if response.status_code == 200:
            produto = response.json()
            dados_produtos.append(produto)
        else:
            print(f"Erro ao buscar produto {i}")
    
    df = p.DataFrame(dados_produtos)
    df.to_csv(os.path.join(caminho, 'dados_produtos.csv'), index=False, encoding='utf-8-sig')
    if(os.path.exists(os.path.join(caminho, 'dados_produtos.csv'))):
        print(f"Salvo em: {os.path.join(caminho, 'dados_produtos.csv')}") 
    else:
        print("Erro ao criar o arquivo CSV.")


except Exception as e:
    print(f"Erro: {e}")
