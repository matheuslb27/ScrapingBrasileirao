import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from db.conexao import conectar
import os

class Classificacao:

    #Salvar Classificação no Banco
    @staticmethod
    def salvar_class():
        
        url = os.getenv('url1')
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        tabela_classificacao = soup.find('table', id='results2025241_overall')
        
        linhas = tabela_classificacao.find_all('tr')

        dados = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas]

        df = pd.DataFrame(dados)

        df_filtrado = df[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 16]]

        df_filtrado.columns = df_filtrado.iloc[0]
        df_filtrado = df_filtrado[1:]
        df_filtrado.reset_index(drop=True, inplace=True)

        conn = conectar()
        cursor = conn.cursor()

        for _, row in df_filtrado.iterrows():
            cursor.execute("""                   
                INSERT INTO Classificacao (Posicao, Equipe, Jogos, VIT, E, DER, GP, GC, SG, Pts, 
                    PtsporJogo, Ultimos5, Publico)
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                row['Rk'],
                row['Squad'],
                int(row['MP']),
                int(row['W']),
                int(row['D']),
                int(row['L']),
                int(row['GF']),
                int(row['GA']),
                int(row['GD']),
                int(row['Pts']),
                float(row['Pts/MP'].replace(',', '.') or 0),
                row['Last 5'],
                float(row['Attendance'].replace(',', '.') or 0)
            )

        conn.commit()
        cursor.close()
        conn.close()

        print("Tabela de classificação inserida com sucesso!")



    #Atualizar Classificação no Banco
    @staticmethod
    def atualizar_classificacao():

        url = os.getenv('url1')
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        tabela = soup.find('table', id='results2025241_overall')
        
        linhas = tabela.find_all('tr')

        dados = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas]

        df = pd.DataFrame(dados)

        df_filtrado = df[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 16]]
        
        df_filtrado.columns = df_filtrado.iloc[0]
        df_filtrado = df_filtrado[1:]
        df_filtrado.reset_index(drop=True, inplace=True)
        df_filtrado['Last 5'] = df_filtrado['Last 5'].apply(lambda x: ' '.join(list(str(x))))
        df_filtrado['Last 5'] = df_filtrado['Last 5'].replace('W L D W', 'V D E V')

        conn = conectar()
        cursor = conn.cursor()

        for _, row in df_filtrado.iterrows():
            cursor.execute("""
                UPDATE Classificacao
                SET Equipe = ?, Jogos = ?, VIT = ?, E = ?, DER = ?, GP = ?, GC = ?, SG = ?, Pts = ?,
                    PtsporJogo = ?, Ultimos5 = ?, Publico = ?
                WHERE Posicao = ?
            """,
                row['Squad'],
                int(row['MP']),
                int(row['W']),
                int(row['D']),
                int(row['L']),
                int(row['GF']),
                int(row['GA']),
                int(row['GD']),
                int(row['Pts']),
                float(row['Pts/MP'].replace(',', '.') or 0),
                row['Last 5'],
                float(row['Attendance'].replace(',', '.') or 0),
                row['Rk']         
            )

        conn.commit()
        cursor.close()
        conn.close()

        print('Tabela de classificação atualizada com sucesso!')
