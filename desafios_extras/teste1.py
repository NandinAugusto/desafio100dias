import os
import requests as r
import json as js
import pandas as pd
from tqdm import tqdm

def main():
    while True:
        user = input('Digite o nome do usuário do GitHub: ')
        url = f'https://api.github.com/users/{user}/events'

        try:
            print('Buscando eventos...')
            resposta = r.get(url)

            if resposta.status_code == 200 and resposta.json():
                for _ in tqdm(range(10)):
                    pass

                df = pd.DataFrame(resposta.json())
                df.to_json(f'usuario_{user}_events.json', index=False)
                print(f'Dados salvos em usuario_{user}_events.json\n')
                break
            else:
                print('Usuário não encontrado ou sem eventos públicos. Tente novamente.\n')

        except Exception as e:
            print(f'Erro ao tentar buscar os dados: {e}\n')

if __name__ == '__main__':
    main()
