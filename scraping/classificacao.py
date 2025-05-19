import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from db.conexao import conectar

class Classificacao:

    @staticmethod
    def salvar_class():
        url = 'https://fbref.com/pt/comps/24/Serie-A-Estatisticas'
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        tabela_html = soup.find('table', id='results2025241_overall')

        colunas_usar = ['Equipe', 'MP', 'V', 'E', 'D', 'GP', 'GC', 'GD', 'Pt']

        dados = []
        for row in tabela_html.find('tbody').find_all('tr'):
            colunas = [td.text.strip() for td in row.find_all('td')]
            if colunas:
                linha = [str(len(dados) + 1)] + [colunas[colunas_usar.index(col)] for col in colunas_usar]
                dados.append(linha)

        cabecalhos = ['Cl'] + colunas_usar

        df = pd.DataFrame(dados, columns=cabecalhos)
        print(df)

        conn = conectar()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                UPDATE Classificacao2025
                SET Equipe = ?, Jogos = ?, VIT = ?, E = ?, DER = ?, GP = ?, GC = ?, SG = ?, Pts = ?
                WHERE Pos = ?
            """,
                row['Equipe'],
                int(row['MP']),
                int(row['V']),
                int(row['E']),
                int(row['D']),
                int(row['GP']),
                int(row['GC']),
                int(row['GD']),
                int(row['Pt']),
                int(row['Cl'])
            )

        conn.commit()
        cursor.close()
        conn.close()

    @staticmethod
    def atualizar_classificacao():
        url = 'https://fbref.com/pt/comps/24/Serie-A-Estatisticas'
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        tabela_html = soup.find('table', id='results2025241_overall')

        colunas_usar = ['Equipe', 'MP', 'V', 'E', 'D', 'GP', 'GC', 'GD', 'Pt']

        dados = []
        for row in tabela_html.find('tbody').find_all('tr'):
            colunas = [td.text.strip() for td in row.find_all('td')]
            if colunas:
                linha = [str(len(dados) + 1)] + [colunas[colunas_usar.index(col)] for col in colunas_usar]
                dados.append(linha)

        cabecalhos = ['Cl'] + colunas_usar

        df = pd.DataFrame(dados, columns=cabecalhos)
        print(df)

        conn = conectar()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                UPDATE Classificacao2025
                SET Equipe = ?, Jogos = ?, VIT = ?, E = ?, DER = ?, GP = ?, GC = ?, SG = ?, Pts = ?
                WHERE Pos = ?
            """,
                row['Equipe'],
                int(row['MP']),
                int(row['V']),
                int(row['E']),
                int(row['D']),
                int(row['GP']),
                int(row['GC']),
                int(row['GD']),
                int(row['Pt']),
                int(row['Cl'])
            )

        conn.commit()
        cursor.close()
        conn.close()
