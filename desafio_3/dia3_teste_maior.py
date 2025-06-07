import requests
import os
import pandas as pd
from typing import Dict, List, Optional
import time
from tqdm import tqdm

def obter_info_varios_paises(lista_codigos: List[str]) -> Dict[str, Dict]:
    """
    Obtém informações detalhadas de vários países a partir da RestCountries API.
    """
    if not lista_codigos:
        return {}
        
    try:
        with tqdm(desc="Preparando consulta API", total=3) as pbar:
            pbar.set_description("Limpando codigos de paises")
            codigos_limpos = list(set([codigo.strip() for codigo in lista_codigos if codigo.strip()]))
            pbar.update(1)
            
            if not codigos_limpos:
                pbar.set_description("Nenhum codigo valido encontrado")
                pbar.update(2)
                return {}
                
            pbar.set_description("Preparando URL da API")
            codigos_str = ",".join(codigos_limpos)
            url = f"https://restcountries.com/v3.1/alpha?codes={codigos_str}"
            pbar.update(1)
            
            pbar.set_description(f"Consultando API para {len(codigos_limpos)} paises")
            pbar.update(1)
        
        resposta = requests.get(url, timeout=15)

        if resposta.status_code == 200:
            dados = resposta.json()
            info_paises = {}
            
            with tqdm(dados, desc="Processando dados dos paises", unit="pais") as pbar:
                for pais in pbar:
                    codigo = pais.get("cca2", "")
                    pbar.set_description(f"Processando {pais.get('name', {}).get('common', 'N/A')}")
                    
                    moeda_principal = None
                    if pais.get('currencies'):
                        moeda_principal = list(pais.get('currencies').keys())[0]
                    
                    idioma_principal = None
                    if pais.get('languages'):
                        idioma_principal = list(pais.get('languages').values())[0]
                    
                    info_paises[codigo] = {
                        'nome_pais': pais.get('name', {}).get('common', 'N/A'),
                        'nome_oficial': pais.get('name', {}).get('official', 'N/A'),
                        'regiao': pais.get('region', 'N/A'),
                        'subregiao': pais.get('subregion', 'N/A'),
                        'populacao': pais.get('population', 0),
                        'area': pais.get('area', 0),
                        'capital': pais.get('capital', ['N/A'])[0] if pais.get('capital') else 'N/A',
                        'moeda_principal': moeda_principal,
                        'todas_moedas': list(pais.get('currencies', {}).keys()) if pais.get('currencies') else [],
                        'idioma_principal': idioma_principal,
                        'todos_idiomas': list(pais.get('languages', {}).values()) if pais.get('languages') else [],
                        'codigo_telefone': pais.get('idd', {}).get('root', '') + (pais.get('idd', {}).get('suffixes', [''])[0] if pais.get('idd', {}).get('suffixes') else ''),
                        'continente': pais.get('continents', ['N/A'])[0] if pais.get('continents') else 'N/A'
                    }
            
            print(f"Dados obtidos para {len(info_paises)} paises com sucesso.")
            return info_paises
            
        elif resposta.status_code == 404:
            print("Alguns codigos de paises nao foram encontrados na API.")
            return {}
        else:
            print(f"Erro na API RestCountries: {resposta.status_code} - {resposta.text}")
            return {}
            
    except requests.exceptions.Timeout:
        print("Timeout na consulta a API. Tente novamente mais tarde.")
        return {}
    except requests.exceptions.ConnectionError:
        print("Erro de conexao com a API. Verifique sua conexao com a internet.")
        return {}
    except Exception as e:
        print(f"Erro inesperado ao consultar paises: {e}")
        return {}

def validar_codigos_pais(codigos: List[str]) -> List[str]:
    """
    Valida e limpa códigos de países.
    """
    codigos_validos = []
    
    with tqdm(codigos, desc="Validando codigos de pais", unit="codigo") as pbar:
        for codigo in pbar:
            pbar.set_description(f"Validando: {codigo}")
            if isinstance(codigo, str) and len(codigo.strip()) == 2:
                codigos_validos.append(codigo.strip().upper())
            elif isinstance(codigo, str) and codigo.strip():
                print(f"Aviso: Codigo '{codigo}' parece invalido (deve ter 2 caracteres).")
    
    codigos_unicos = list(set(codigos_validos))  # Remove duplicatas
    print(f"Validacao concluida: {len(codigos_unicos)} codigos validos encontrados")
    return codigos_unicos

def enriquecer_com_dados_pais(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas de informações de países ao DataFrame, baseado na coluna 'company_location'.
    """
    with tqdm(desc="Iniciando enriquecimento", total=6) as main_pbar:
        main_pbar.set_description("Verificando coluna de localizacao")
        
        if 'company_location' not in df.columns:
            print("Aviso: Coluna 'company_location' nao encontrada. Tentando outras colunas...")
            colunas_possiveis = [col for col in df.columns if 'location' in col.lower() or 'country' in col.lower() or 'pais' in col.lower()]
            if colunas_possiveis:
                coluna_escolhida = colunas_possiveis[0]
                print(f"Usando coluna '{coluna_escolhida}' como referencia para paises.")
                df = df.rename(columns={coluna_escolhida: 'company_location'})
            else:
                print("Erro: Nenhuma coluna de localizacao encontrada. O enriquecimento nao sera possivel.")
                return df
        main_pbar.update(1)

        main_pbar.set_description("Limpando dados de localizacao")
        df['company_location'] = df['company_location'].astype(str).str.strip()
        df_limpo = df[~df['company_location'].isin(['nan', 'None', '', 'NaN'])]
        if df_limpo.empty:
            print("Aviso: Nenhum codigo de pais valido encontrado.")
            return df
        main_pbar.update(1)

        main_pbar.set_description("Extraindo codigos unicos")
        codigos_unicos = df_limpo['company_location'].str.upper().unique().tolist()
        main_pbar.update(1)
        
        main_pbar.set_description("Validando codigos de pais")
        codigos_validos = validar_codigos_pais(codigos_unicos)
        if not codigos_validos:
            print("Erro: Nenhum codigo de pais valido encontrado apos validacao.")
            return df
        main_pbar.update(1)

        main_pbar.set_description("Consultando API de paises")
        dados_paises = obter_info_varios_paises(codigos_validos)
        if not dados_paises:
            print("Aviso: Nenhum dado de pais foi obtido da API.")
            return df
        main_pbar.update(1)

        main_pbar.set_description("Mapeando dados para DataFrame")
        print("Adicionando colunas de informacoes dos paises...")
        
        colunas_mapeamento = [
            ('nome_pais', 'nome_pais'),
            ('nome_oficial_pais', 'nome_oficial'),
            ('regiao', 'regiao'),
            ('subregiao', 'subregiao'),
            ('continente', 'continente'),
            ('capital_pais', 'capital'),
            ('populacao_pais', 'populacao'),
            ('area_pais', 'area'),
            ('moeda_principal', 'moeda_principal'),
            ('idioma_principal', 'idioma_principal'),
            ('codigo_telefone', 'codigo_telefone')
        ]
        
        with tqdm(colunas_mapeamento, desc="Criando colunas de pais", unit="coluna") as col_pbar:
            for nome_coluna, chave_dados in col_pbar:
                col_pbar.set_description(f"Criando coluna: {nome_coluna}")
                valor_padrao = 'N/A' if chave_dados not in ['populacao', 'area'] else 0
                df[nome_coluna] = df['company_location'].str.upper().map(
                    lambda x: dados_paises.get(x, {}).get(chave_dados, valor_padrao)
                )
        
        with tqdm(desc="Processando listas de dados", total=2) as list_pbar:
            list_pbar.set_description("Criando coluna: todas_moedas")
            df['todas_moedas'] = df['company_location'].str.upper().map(
                lambda x: ', '.join(dados_paises.get(x, {}).get('todas_moedas', [])) or 'N/A'
            )
            list_pbar.update(1)
            
            list_pbar.set_description("Criando coluna: todos_idiomas")
            df['todos_idiomas'] = df['company_location'].str.upper().map(
                lambda x: ', '.join(dados_paises.get(x, {}).get('todos_idiomas', [])) or 'N/A'
            )
            list_pbar.update(1)

        main_pbar.update(1)
        main_pbar.set_description("Enriquecimento concluido!")

    print("Enriquecimento de dados concluido com sucesso!")
    return df

def criar_csv_enriquecido(df: pd.DataFrame, nome_arquivo: str = "dados_enriquecidos.csv") -> Optional[str]:
    """
    Processa o DataFrame, enriquece com dados de países e salva em um arquivo CSV.
    """
    try:
        with tqdm(desc="Processamento geral", total=5) as main_pbar:
            main_pbar.set_description(f"Iniciando processamento de {len(df)} registros")
            main_pbar.update(1)
            
            main_pbar.set_description("Enriquecendo dados com informacoes de paises")
            df_enriquecido = enriquecer_com_dados_pais(df)
            main_pbar.update(1)

            main_pbar.set_description("Preparando diretorio de destino")
            pasta_script = os.path.dirname(os.path.abspath(__file__))  # pasta do script atual
            pasta_programinhas = os.path.dirname(pasta_script)  # pasta pai
            pasta_destino = os.path.join(pasta_programinhas, 'arquivos_diversos')
            os.makedirs(pasta_destino, exist_ok=True)
            main_pbar.update(1)

            main_pbar.set_description("Preparando nome do arquivo")
            nome_base, extensao = os.path.splitext(nome_arquivo)
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            nome_arquivo_final = f"{nome_base}_{timestamp}{extensao}"
            caminho_completo_arquivo = os.path.join(pasta_destino, nome_arquivo_final)
            main_pbar.update(1)

            main_pbar.set_description("Salvando arquivo CSV")
            with tqdm(desc="Salvando dados", unit="KB") as save_pbar:
                df_enriquecido.to_csv(caminho_completo_arquivo, index=False, encoding="utf-8-sig")
                save_pbar.update(1)
                save_pbar.set_description("Arquivo salvo com sucesso")
            
            main_pbar.update(1)
            main_pbar.set_description("Processamento concluido")
        
        print(f"OK: {len(df_enriquecido)} registros processados e salvos.")
        print(f"Arquivo salvo em: {caminho_completo_arquivo}")
        
        with tqdm(desc="Calculando estatisticas", total=3) as stats_pbar:
            stats_pbar.set_description("Contando paises unicos")
            total_paises = df_enriquecido['company_location'].nunique()
            stats_pbar.update(1)
            
            stats_pbar.set_description("Contando paises enriquecidos")
            paises_enriquecidos = df_enriquecido[df_enriquecido['nome_pais'] != 'N/A']['company_location'].nunique()
            stats_pbar.update(1)
            
            stats_pbar.set_description("Calculando taxa de sucesso")
            taxa_sucesso = (paises_enriquecidos/total_paises*100) if total_paises else 0
            stats_pbar.update(1)
        
        print(f"Taxa de sucesso do enriquecimento: {taxa_sucesso:.2f}%")
        return caminho_completo_arquivo
    
    except Exception as e:
        print(f"Erro ao criar CSV enriquecido: {e}")
        return None

def main():
    print("*** Iniciando enriquecimento de dados com informacoes de paises ***")
    print("=" * 60)

    caminho_arquivo = os.path.join(os.getcwd(), "dados_produtos.csv")
    
    if not os.path.isfile(caminho_arquivo):
        print(f"Arquivo '{caminho_arquivo}' nao encontrado. Coloque o arquivo 'dados_produtos.csv' na pasta do script.")
        return

    print(f"Lendo arquivo: {caminho_arquivo}")
    df = pd.read_csv(caminho_arquivo)
    
    caminho_salvo = criar_csv_enriquecido(df)
    
    if caminho_salvo:
        print(f"Processo finalizado com sucesso. Arquivo gerado em:\n{caminho_salvo}")
    else:
        print("Processo finalizado com erros.")

if __name__ == "__main__":
    main()
