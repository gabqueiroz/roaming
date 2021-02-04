import os, zipfile
from PacoteFuncoes import PacoteFuncoes as pf
from TransformacaoRomming import TrasformarRomming as tr

def main():
        pf.descompactar('/dados/cdrs-roaming', ".zip")
        if os.path.exists('/dados/cdrs-roaming/Fallback/BRACT/dlvrIMSI'):
                pf.converter(".GS3", '/dados/cdrs-roaming/Fallback/BRACT/dlvrIMSI')
                tr.atualizarMensal('/dados/persistence/kafka-connect/jars/mensal', '/dados/persistence/kafka-connect/jars/diario', ".csv")
        else:
                print('Erro em localizar o arquivo')

if __name__ == "__main__":
    # execute only if run as a script
    main()
