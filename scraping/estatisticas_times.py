import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from db.conexao import conectar
import os
from dotenv import load_dotenv

class Estatisticas_times:

    @staticmethod
    def salvar_estats():

        url = os.getenv('url')
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        #Extraindo dados de tabelas diferentes
        tabela_1 = soup.find('table', id='stats_squads_standard_for')
        tabela_2 = soup.find('table', id='stats_squads_keeper_for')
        tabela_3 = soup.find('table', id='stats_squads_keeper_adv_for')
        tabela_4 = soup.find('table', id='stats_squads_shooting_for')
        tabela_5 = soup.find('table', id='stats_squads_passing_for')
        tabela_6 = soup.find('table', id='stats_squads_misc_for')

        linhas_tabela1 = tabela_1.find_all('tr')
        linhas_tabela2 = tabela_2.find_all('tr')
        linhas_tabela3 = tabela_3.find_all('tr')
        linhas_tabela4 = tabela_4.find_all('tr')
        linhas_tabela5 = tabela_5.find_all('tr')
        linhas_tabela6 = tabela_6.find_all('tr')

        dados_1 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela1[1:]]
        dados_2 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela2[1:]]
        dados_3 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela3[1:]]
        dados_4 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela4[1:]]
        dados_5 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela5[1:]]
        dados_6 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela6[1:]]

        df_1 = pd.DataFrame(dados_1)
        df_2 = pd.DataFrame(dados_2)
        df_3 = pd.DataFrame(dados_3)
        df_4 = pd.DataFrame(dados_4)
        df_5 = pd.DataFrame(dados_5)
        df_6 = pd.DataFrame(dados_6)

        df_1_filtrado = df_1[[0, 3, 12, 14, 15, 22, 23]]
        df_2_filtrado = df_2[[7, 8, 14, 15, 16, 17, 18, 19, 20]]
        df_3_filtrado = df_3[[5, 6, 7]]
        df_4_filtrado = df_4[[4, 5, 6, 7, 8]]
        df_5_filtrado = df_5[[3, 4, 5,]]
        df_6_filtrado = df_6[[6, 7, 8, 9, 16, 17, 18]]

        #Concatena as colunas escolhidas de cada tabela
        df_filtrado = pd.concat([
            df_1_filtrado, df_2_filtrado, df_3_filtrado,
            df_4_filtrado, df_5_filtrado, df_6_filtrado
        ], axis=1)

        df_filtrado.columns = df_filtrado.iloc[0]
        df_filtrado = df_filtrado[1:]
        df_filtrado.reset_index(drop=True, inplace=True)
        #df_filtrado.to_excel('Estatisticas.xlsx', index=False)
        #print(df_filtrado)
        #print(df_filtrado.columns)

        conn = conectar()
        cursor = conn.cursor()        

        for _, row in df_filtrado.iterrows():
            cursor.execute("""             
                INSERT INTO EstatisticasTimes (
                    Equipe, Posse, PB, CrtsA, CrtV, MediaGols, MediaAssist, 
                    GC90, CaGC, SV, PercentualSV, PT, GPC, PSV, GPp, 
                    PercentualDefesas, TD, GC, OG, TC, CaG, SoTPercentual, 
                    Sh90, SoT90, Cmp, Att, CmpPercentual, Fts, FltsP, 
                    Impedimentos, Crz, Ganhos, Perdas, GanhosPercentual
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, 
                row['Equipe'],
                float(row['Posse']),
                int(row['PB']),
                int(row['CrtsA']),
                int(row['CrtV']),
                float(row['Gols'].replace(',', '.')),
                float(row['Assis.'].replace(',', '.')),
                float(row['GC90'].replace(',', '.')),
                int(row['CaGC']),
                int(row['SV']),
                float(row['%SV'].replace(',', '.')),
                int(row['PT']),
                int(row['GPC']),
                int(row['Psv']),
                int(row['GPp']),
                float(row['%Defesas'].replace(',', '.')or 0),
                int(row['TD']),
                int(row['GC']),
                int(row['OG']),
                int(row['TC']),
                int(row['CaG']),
                float(row['SoT%'].replace(',', '.')),
                float(row['Sh/90'].replace(',', '.')),
                float(row['SoT/90'].replace(',', '.')),
                int(row['Cmp']),
                int(row['Att']),
                float(row['Cmp%'].replace(',', '.')),
                int(row['Fts']),
                int(row['FltsP']),
                int(row['Desativado']),
                int(row['Crz']),
                int(row['Ganhos']),
                int(row['Perdas']),
                float(row['Ganhos%'].replace(',', '.'))
        )
        
        conn.commit()
        cursor.close()
        conn.close()

        print("Estatisticas de times inseridos com sucesso!")



    @staticmethod
    def atualizar_estats():
        
        url = os.getenv('url')
        scraper = cloudscraper.create_scraper()
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')

        tabela_1 = soup.find('table', id='stats_squads_standard_for')
        tabela_2 = soup.find('table', id='stats_squads_keeper_for')
        tabela_3 = soup.find('table', id='stats_squads_keeper_adv_for')
        tabela_4 = soup.find('table', id='stats_squads_shooting_for')
        tabela_5 = soup.find('table', id='stats_squads_passing_for')
        tabela_6 = soup.find('table', id='stats_squads_misc_for')

        linhas_tabela1 = tabela_1.find_all('tr')
        linhas_tabela2 = tabela_2.find_all('tr')
        linhas_tabela3 = tabela_3.find_all('tr')
        linhas_tabela4 = tabela_4.find_all('tr')
        linhas_tabela5 = tabela_5.find_all('tr')
        linhas_tabela6 = tabela_6.find_all('tr')

        dados_1 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela1[1:]]
        dados_2 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela2[1:]]
        dados_3 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela3[1:]]
        dados_4 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela4[1:]]
        dados_5 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela5[1:]]
        dados_6 = [[col.get_text(strip=True) for col in linha.find_all(['th', 'td'])] for linha in linhas_tabela6[1:]]
        
        df_1 = pd.DataFrame(dados_1)
        df_2 = pd.DataFrame(dados_2)
        df_3 = pd.DataFrame(dados_3)
        df_4 = pd.DataFrame(dados_4)
        df_5 = pd.DataFrame(dados_5)
        df_6 = pd.DataFrame(dados_6)

        df_1_filtrado = df_1[[0, 3, 12, 14, 15, 22, 23]]
        df_2_filtrado = df_2[[7, 8, 14, 15, 16, 17, 18, 19, 20]]
        df_3_filtrado = df_3[[5, 6, 7]]
        df_4_filtrado = df_4[[4, 5, 6, 7, 8]]
        df_5_filtrado = df_5[[3, 4, 5,]]
        df_6_filtrado = df_6[[6, 7, 8, 9, 16, 17, 18]]

        df_filtrado = pd.concat([
            df_1_filtrado, df_2_filtrado, df_3_filtrado,
            df_4_filtrado, df_5_filtrado, df_6_filtrado
        ], axis=1)

        df_filtrado.columns = df_filtrado.iloc[0]
        df_filtrado = df_filtrado[1:]
        df_filtrado.reset_index(drop=True, inplace=True)
        #print(df_filtrado)

        conn = conectar()
        cursor = conn.cursor()

        for _, row in df_filtrado.iterrows():
            cursor.execute("""                    
                UPDATE EstatisticasTimes
                SET Posse = ?, PB = ?, CrtsA = ?, CrtV = ?, MediaGols = ?, MediaAssist = ?,
                GC90 = ?, CaGC = ?, SV = ?, PercentualSV = ?, PT = ?, GPC = ?, PSV = ?, GPp = ?,
                PercentualDefesas = ?, TD = ?, GC = ?, OG = ?, TC= ?, Cag= ?, SoTPercentual = ?,
                Sh90 = ?, SoT90 = ?, Cmp = ?, Att = ?, CmpPercentual = ?, Fts = ?, FltsP = ?,
                Impedimentos = ?, Crz = ?, Ganhos = ?, Perdas = ?, GanhosPercentual = ?
                WHERE Equipe = ?           
            """,
                float(row['Posse']),
                int(row['PB']),
                int(row['CrtsA']),
                int(row['CrtV']),
                float(row['Gols'].replace(',', '.')),
                float(row['Assis.'].replace(',', '.')),
                float(row['GC90'].replace(',', '.')),
                int(row['CaGC']),
                int(row['SV']),
                float(row['%SV'].replace(',', '.')),
                int(row['PT']),
                int(row['GPC']),
                int(row['Psv']),
                int(row['GPp']),
                float(row['%Defesas'].replace(',', '.')or 0),
                int(row['TD']),
                int(row['GC']),
                int(row['OG']),
                int(row['TC']),
                int(row['CaG']),
                float(row['SoT%'].replace(',', '.')),
                float(row['Sh/90'].replace(',', '.')),
                float(row['SoT/90'].replace(',', '.')),
                int(row['Cmp']),
                int(row['Att']),
                float(row['Cmp%'].replace(',', '.')),
                int(row['Fts']),
                int(row['FltsP']),
                int(row['Desativado']),
                int(row['Crz']),
                int(row['Ganhos']),
                int(row['Perdas']),
                float(row['Ganhos%'].replace(',', '.')),
                row['Equipe']
        )

        conn.commit()
        cursor.close()
        conn.close()

        print("Estatisticas dos times atualizados com sucesso!")