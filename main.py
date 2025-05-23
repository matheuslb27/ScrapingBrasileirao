from scraping.classificacao import Classificacao
from scraping.estatisticas_times import Estatisticas_times

if __name__ == "__main__":
    #Classificacao.salvar_class()
    Classificacao.atualizar_classificacao()
    Estatisticas_times.salvar_estats()

    