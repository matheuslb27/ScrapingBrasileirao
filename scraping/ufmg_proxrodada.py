import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from db.conexao import conectar

class UFMG_ProxRodada:

    @staticmethod
    def salvar_proxrodada():

        url = "https://www.mat.ufmg.br/futebol/tabela-da-proxima-rodada_seriea/"
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        prox_rodada = soup.find('table', id="tabelaCL")
        linhas = prox_rodada.find_all('tr')
        dados = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas]

        df = pd.DataFrame(dados)
        df.columns = df.iloc[0]
        df = df.drop(index=0)
        df.reset_index(drop=True, inplace=True)
        df = df.dropna(subset=['PVM', 'PE', 'PVV'])

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

        df['MandanteTratado'] = df['MANDANTE'].map(tratamento_times)
        df['VisitanteTratado'] = df['VISITANTE'].map(tratamento_times)
        
        conn = conectar()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                UPDATE CalendarioRodadas
                SET ProbMandante = ?,
                ProbEmpate = ?,
                ProbVisitante = ?
                WHERE TimeCasa = ? AND TimeVisitante = ?
            """,
            (
            row['PVM'],
            row['PE'],
            row['PVV'],
            row['MandanteTratado'],
            row['VisitanteTratado']
            )
        )

        conn.commit()
        cursor.close()
        conn.close()

        print('Probilidade da próxima rodada atualizada com sucesso!')