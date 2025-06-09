import cloudscraper
from bs4 import BeautifulSoup, Comment
import pandas as pd
from db.conexao import conectar
import os

class Estatisticas_jogadores:

    @staticmethod
    def salvar_estats_jog():
        
        url = os.getenv('url3')
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        #Removendo comentário da tabela para puxar os dados
        tabela = None
        comentarios = soup.find_all(string=lambda texto: isinstance(texto, Comment))
        for c in comentarios:
            if 'id="stats_standard"' in c:
                soup_comentado = BeautifulSoup(c, 'html.parser')
                tabela = soup_comentado.find('table', id='stats_standard')
                break

        linha = [tr for tr in tabela.find_all('tr') if 'thead' not in tr.get('class', [])]

        dados = [[col.get_text(strip=True) for col in linhas.find_all(['th', 'td'])] for linhas in linha][1:]

        df = pd.DataFrame(dados)
        
        df_filtrado = df[[0, 1, 2, 3, 4, 5, 6, 7, 9, 11, 12, 17, 18]]
        df_filtrado.columns = df_filtrado.iloc[0]
        df_filtrado = df_filtrado[1:]
        df_filtrado.reset_index(drop=True, inplace=True)
        df_filtrado.replace('', '0', inplace=True)
        df_filtrado.fillna('0', inplace=True)
        #print(df_filtrado.columns.tolist())
        
        #Funções para converter colunas para int e float
        def to_int(value):
            try:
                return int(str(value).replace('.', '').replace(',', ''))
            except:
                return 0
        
        conn = conectar()
        cursor = conn.cursor()

        #Cálculo da média de gols e assistencias
        df_filtrado['Gols'] = pd.to_numeric(df_filtrado['Gols'], errors='coerce')
        df_filtrado['MP'] = pd.to_numeric(df_filtrado['MP'], errors='coerce')
        df_filtrado['Assis.'] = pd.to_numeric(df_filtrado['Assis.'], errors='coerce')

        df_filtrado['mediaGols'] = (df_filtrado['Gols'] / df_filtrado['MP']).round(2)
        df_filtrado['mediaAssis'] = (df_filtrado['Assis.'] / df_filtrado['MP']).round(2)

        for _, row in df_filtrado.iterrows():
            cursor.execute("""                   
                INSERT INTO EstatisticasJogadores (IdJogador, NomeJogador, Nacionalidade, Posicao, Equipe, Nascimento,
                            JogosDisputados, Minutagem, Gols, Assistencias, CrtsA, CrtsV, MediaGols, MediaAssistencias )
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                to_int(row['Class.']),
                row['Jogador'],
                row['Nação'],
                row['Pos.'],
                row['Equipe'],
                to_int(row['Nascimento']),
                to_int(row['MP']),
                to_int(row['Min.']),
                to_int(row['Gols']),
                to_int(row['Assis.']),
                to_int(row['CrtsA']),
                to_int(row['CrtV']),
                float(row['mediaGols']),
                float(row['mediaAssis'])
            )

        conn.commit()
        cursor.close()
        conn.close()

        print("Tabela de Estatísticas dos jogadores inserida com sucesso!")

    @staticmethod
    def atualizar_estats_jog():
        
        url = os.getenv('url3')
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        #Removendo comentário da tabela para puxar os dados
        tabela = None
        comentarios = soup.find_all(string=lambda texto: isinstance(texto, Comment))
        for c in comentarios:
            if 'id="stats_standard"' in c:
                soup_comentado = BeautifulSoup(c, 'html.parser')
                tabela = soup_comentado.find('table', id='stats_standard')
                break

        linha = [tr for tr in tabela.find_all('tr') if 'thead' not in tr.get('class', [])]

        dados = [[col.get_text(strip=True) for col in linhas.find_all(['th', 'td'])] for linhas in linha][1:]

        df = pd.DataFrame(dados)
        
        df_filtrado = df[[0, 1, 2, 3, 4, 5, 6, 7, 9, 11, 12, 17, 18]]
        df_filtrado.columns = df_filtrado.iloc[0]
        df_filtrado = df_filtrado[1:]
        df_filtrado.reset_index(drop=True, inplace=True)
        df_filtrado.replace('', '0', inplace=True)
        df_filtrado.fillna('0', inplace=True)
    
        def to_int(value):
            try:
                return int(str(value).replace('.', '').replace(',', ''))
            except:
                return 0
        
        conn = conectar()
        cursor = conn.cursor()

        df_filtrado['Gols'] = pd.to_numeric(df_filtrado['Gols'], errors='coerce')
        df_filtrado['MP'] = pd.to_numeric(df_filtrado['MP'], errors='coerce')
        df_filtrado['Assis.'] = pd.to_numeric(df_filtrado['Assis.'], errors='coerce')

        df_filtrado['mediaGols'] = (df_filtrado['Gols'] / df_filtrado['MP']).round(2)
        df_filtrado['mediaAssis'] = (df_filtrado['Assis.'] / df_filtrado['MP']).round(2)

        for _, row in df_filtrado.iterrows():
            cursor.execute("""                   
                UPDATE EstatisticasJogadores
                SET NomeJogador = ?, Nacionalidade = ?, Posicao = ?, Equipe = ?, Nascimento = ?,
                    JogosDisputados = ?, Minutagem = ?, Gols = ?, Assistencias = ?, CrtsA = ?, CrtsV = ?,
                    MediaGols = ?, MediaAssistencias = ?
                WHERE IdJogador = ?
            """,
                row['Jogador'],
                row['Nação'],
                row['Pos.'],
                row['Equipe'],
                to_int(row['Nascimento']),
                to_int(row['MP']),
                to_int(row['Min.']),
                to_int(row['Gols']),
                to_int(row['Assis.']),
                to_int(row['CrtsA']),
                to_int(row['CrtV']),
                float(row['mediaGols']),
                float(row['mediaAssis']),
                to_int(row['Class.'])
            )

        conn.commit()
        cursor.close()
        conn.close()

        print("Tabela de Estatísticas dos jogadores atualizada com sucesso!")