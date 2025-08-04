import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from db.conexao import conectar
import os

class UFMG_Estatisticas:

    @staticmethod
    def salvar_estats_ufmg():

        url = "https://www.mat.ufmg.br/futebol/tabela-da-proxima-rodada_seriea/"
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        prob_jogos = soup.find('table', id='tabelaCL')
        linhas = prob_jogos.find_all('tr')
        dados = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas]

        df = pd.DataFrame(dados)
        print(df)
