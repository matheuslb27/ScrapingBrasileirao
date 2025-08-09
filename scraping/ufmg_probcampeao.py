import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from db.conexao import conectar

class UFMG_ProbCampeao:

    @staticmethod
    def salvar_probcampeao():

        url = "https://www.mat.ufmg.br/futebol/campeao_seriea/"
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        prob_jogos = soup.find('table', id='tabelaCL')
        linhas = prob_jogos.find_all('tr')
        dados = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas]

        df = pd.DataFrame(dados)
        df.columns = df.iloc[0]
        df = df.drop(index=0)
        df.reset_index(drop=True, inplace=True)
        #print(df)

        tratamento_times = {
            'ATLÉTICO': 'Atlético Mineiro',
            'BAHIA': 'Bahia',
            'BOTAFOGO': 'Botafogo (RJ)',
            'CEARÁ': 'Ceará',
            'CORINTHIANS': 'Corinthians',
            'CRUZEIRO': 'Cruzeiro',
            'FLAMENGO': 'Flamengo',
            'FLUMINENSE': 'Fluminense',
            'FORTALEZA': 'Fortaleza',
            'GRÊMIO': 'Grêmio',
            'INTERNACIONAL': 'Internacional',
            'JUVENTUDE': 'Juventude',
            'MIRASSOL': 'Mirassol',
            'PALMEIRAS': 'Palmeiras',
            'BRAGANTINO': 'RB Bragantino',
            'SANTOS': 'Santos',
            'SÃO PAULO': 'São Paulo',
            'SPORT': 'Sport Recife',
            'VASCO DA GAMA': 'Vasco da Gama',
            'VITÓRIA': 'Vitória'
        }

        df['TimesTratados'] = df['Times'].map(tratamento_times)
        
        conn = conectar()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                UPDATE EstatisticasTimes
                SET ProbCampeao = ?
                WHERE Equipe = ?
            """, (row['Prob(%)'], row['TimesTratados'])
            )
            
        conn.commit()
        cursor.close()
        conn.close()

        print('Probilidade de campeão atualizada com sucesso!')