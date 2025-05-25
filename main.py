from scraping.classificacao import Classificacao
from scraping.estatisticas_times import Estatisticas_times
from scraping.calendario import Calendario


if __name__ == "__main__":
    
    #Classificacao.salvar_class()
    #Estatisticas_times.salvar_estats()
    #Calendario.salvar_rodadas()

    Classificacao.atualizar_classificacao()
    Estatisticas_times.atualizar_estats()
    Calendario.atualizar_rodadas()