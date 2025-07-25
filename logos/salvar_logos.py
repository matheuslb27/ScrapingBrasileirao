import pyodbc
import os
import base64
from PIL import Image
import io

class Salvar_logos():

    @staticmethod
    def salvar_logos():

        conn = pyodbc.connect(
            f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            "Trusted_Connection=yes;"
        )
        cursor = conn.cursor()

        logos_times = {
            "Atlético Mineiro": "logos/logos_png/atleticomg_logo.png",
            "Bahia": "logos/logos_png/bahia_logo.png",
            "Botafogo (RJ)": "logos/logos_png/botafogo_logo.png",
            "Ceará": "logos/logos_png/ceara_logo.png",
            "Corinthians": "logos/logos_png/corinthians_logo.png",
            "Cruzeiro": "logos/logos_png/cruzeiro_logo.png",
            "Flamengo": "logos/logos_png/flamengo_logo.png",
            "Fluminense": "logos/logos_png/fluminense_logo.png",
            "Fortaleza": "logos/logos_png/fortaleza_logo.png",
            "Grêmio": "logos/logos_png/gremio_logo.png",
            "Internacional": "logos/logos_png/internacional_logo.png",
            "Juventude": "logos/logos_png/juventude_logo.png",
            "Mirassol": "logos/logos_png/mirassol_logo.png",
            "Palmeiras": "logos/logos_png/palmeiras_logo.png",
            "RB Bragantino": "logos/logos_png/bragantino_logo.png",
            "Santos": "logos/logos_png/santos_logo.png",
            "São Paulo": "logos/logos_png/saopaulo_logo.png",
            "Sport Recife": "logos/logos_png/sport_logo.png", 
            "Vasco da Gama": "logos/logos_png/vasco_logo.png",
            "Vitória": "logos/logos_png/vitoria_logo.png"
        }

        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Classificacao' AND COLUMN_NAME = 'Logo'
            )
            BEGIN
                ALTER TABLE Classificacao
                ADD Logo VARBINARY(MAX)
            END
        """)
        conn.commit()

        for equipe, caminho_logo in logos_times.items():
            if os.path.exists(caminho_logo):
                with open(caminho_logo, 'rb') as f:
                    #Redimensiona imagem para 40x40 pixels
                    imagem = Image.open(f)
                    imagem = imagem.resize((40, 40), Image.Resampling.LANCZOS)

                    #Cria um buffer de memória para salvar a imagem
                    buffer = io.BytesIO()
                    imagem.save(buffer, format="PNG")
                    logo_bin = buffer.getvalue()

                #Converte a imagem para Base64 e adiciona o prefixo para data URL
                logo_base64 = "data:image/png;base64," + base64.b64encode(logo_bin).decode('utf-8')

                cursor.execute("""
                    UPDATE Classificacao
                    SET Logo = ?
                    WHERE Equipe = ?
                """, logo_base64, equipe)

                print(f"Logo do {equipe} atualizada com sucesso.")
            else:
                print(f"Logo não encontrada para: {equipe} - {caminho_logo}")

        conn.commit()
        cursor.close()
        conn.close()