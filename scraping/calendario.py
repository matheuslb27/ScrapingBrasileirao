import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from db.conexao import conectar
import os
from datetime import datetime

class Calendario:
     
    @staticmethod
    def salvar_rodadas():

        url = os.getenv('url2')
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        calendario = soup.find('table', id = 'sched_2025_24_1')
        
        #list comprehension para remover linhas visuais em branco
        linha = [tr for tr in calendario.find_all('tr') if 'spacer' not in tr.get('class', [])]

        dados = [[col.get_text(strip=True) for col in linhas.find_all(['th', 'td'])] for linhas in linha]

        df = pd.DataFrame(dados)

        df_filtrado = df[[0, 1, 2, 3, 4, 6, 8, 9, 10]]
        df_filtrado.columns = df_filtrado.iloc[0]
        df_filtrado = df_filtrado[1:]
        df_filtrado.reset_index(drop=True, inplace=True)
        df_filtrado.dropna(how='all', inplace=True)
        df_filtrado.fillna('', inplace=True)
        #df_filtrado.to_excel('rodadas.xlsx', index=False)     
        #print(df_filtrado)
        
        conn = conectar()
        cursor = conn.cursor()

        id_partida = 1
        for _, row in df_filtrado.iterrows():
            
            if row['Date'] and row['Date'].strip():
                data_jogo = datetime.strptime(row['Date'].strip(), "%Y-%m-%d").date()  
            
            if row['Time'] and row['Time'].strip():
                horario_jogo = datetime.strptime(row['Time'].strip(), "%H:%M").time()

            cursor.execute("""                   
                INSERT INTO CalendarioRodadas (IdPartida, Rodada, Dia, DataJogo, Horario, TimeCasa, Resultado, 
                    TimeVisitante, Publico, Estadio)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                id_partida,
                int(row['Wk']),
                row['Day'],
                data_jogo,
                horario_jogo,
                row['Home'],
                row['Score'],
                row['Away'],
                float(row['Attendance'].replace(',', '') or 0),
                row['Venue']               
            )
            id_partida += 1

        conn.commit()
        cursor.close()
        conn.close()

        print("Calendario inserido com sucesso!")
        
    @staticmethod
    def atualizar_rodadas():

        url = os.getenv('url2')
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        calendario = soup.find('table', id = 'sched_2025_24_1')
        
        linha = [tr for tr in calendario.find_all('tr') if 'spacer' not in tr.get('class', [])]

        dados = [[col.get_text(strip=True) for col in linhas.find_all(['th', 'td'])] for linhas in linha]

        df = pd.DataFrame(dados)

        df_filtrado = df[[0, 1, 2, 3, 4, 6, 8, 9, 10]]
        df_filtrado.columns = df_filtrado.iloc[0]
        df_filtrado = df_filtrado[1:]
        df_filtrado.reset_index(drop=True, inplace=True)
        df_filtrado.dropna(how='all', inplace=True)
        df_filtrado.fillna('', inplace=True)
        #df_filtrado.to_excel('rodadas.xlsx', index=False)     
        #print(df_filtrado)

        conn = conectar()
        cursor = conn.cursor()

        id_partida = 1
        for _, row in df_filtrado.iterrows():
            
            #Conversão data e hora
            if row['Date'] and row['Date'].strip():
                data_jogo = datetime.strptime(row['Date'].strip(), "%Y-%m-%d").date()  
          
            if row['Time'] and row['Time'].strip():
                horario_jogo = datetime.strptime(row['Time'].strip(), "%H:%M").time()

            cursor.execute("""                   
                UPDATE CalendarioRodadas 
                SET Rodada = ?,Dia = ?, DataJogo = ?, Horario = ?, TimeCasa = ?, Resultado = ?, 
                    TimeVisitante = ?, Publico = ?, Estadio = ?
                WHERE IdPartida = ?
            """,
                int(row['Wk']),
                row['Day'],
                data_jogo,
                horario_jogo,
                row['Home'],
                row['Score'],
                row['Away'],
                float(row['Attendance'].replace(',', '') or 0),
                row['Venue'],
                id_partida                        
            )
            id_partida +=1

        conn.commit()
        cursor.close()
        conn.close()

        print("Calendario atualizado com sucesso!")
