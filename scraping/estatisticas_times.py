import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from db.conexao import conectar

class Estatisticas_times:

    @staticmethod
    def salvar_estats():

        url = 'https://fbref.com/pt/comps/24/Serie-A-Estatisticas'
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        stats = soup.find('table', id='stats_squads_standard_for')

        colunas_usar = ['Idade', 'Posse', 'Gols', 'Assis.', 'G+A', 'PB', 'PT', 'CrtsA','CrtV']

        dados = []

        #for th in stats.find('thead').find_all('th'):
            #print(th.text)

        for row in stats.find('tbody').find_all('tr'):
            colunas = [td.text.strip() for td in row.find_all('td')]
            if colunas:
                linha = linha = [str(len(dados) + 1)]+[colunas[colunas_usar.index(col)] for col in colunas_usar]
                dados.append(linha)

        cabecalhos = ['Equipe'] + colunas_usar

        df = pd.DataFrame(dados, columns=cabecalhos)
        print(df)