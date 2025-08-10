from scraping import ufmg_proxrodada
from scraping.classificacao import Classificacao
from scraping.estatisticas_times import Estatisticas_times
from scraping.calendario import Calendario
from scraping.estatisticas_jogadores import Estatisticas_jogadores
from logos.salvar_logos import Salvar_logos
from scraping.ufmg_probcampeao import UFMG_ProbCampeao
from scraping.ufmg_proxrodada import UFMG_ProxRodada

if __name__ == "__main__":
    
    #Classificacao.salvar_class()
    #Estatisticas_times.salvar_estats()
    #Calendario.salvar_rodadas()
    #Estatisticas_jogadores.salvar_estats_jog()
    
    #Classificacao.atualizar_classificacao()
    Salvar_logos.atualizar_logos()
    #Estatisticas_times.atualizar_estats()
    #Calendario.atualizar_rodadas()
    #Estatisticas_jogadores.atualizar_estats_jog()
    #UFMG_ProbCampeao.salvar_probcampeao()
    #UFMG_ProxRodada.salvar_proxrodada()