import os
import numpy as np
import pandas as pd
#from LeituraParalela import LeituraParalela as lp


class TrasformarRomming(object):

    @staticmethod
    def transformarGS3(file_name, name):
        print("EXEC: Processando: "+file_name)
        # ignorar o bract
        if 'BRACT' in name:
            print("ERRO: Arquivo grande d mais")
            return

        # puxar o arquivo pelo panda
        df = pd.read_csv(file_name)

        # tirar a ultima linha
        df.drop(df.tail(1).index, inplace=True)

        # pegar arquivo q tenha dados
        if df.size > 0:

            df[df.columns.values[0]] = df[df.columns.values[0]].map(lambda x: x[3:18].strip() +' '+ x[188:209].strip() +' '+ x[95:119].strip() +' '+ x[20:25].strip() +' '+ x[147:159].strip())
            df = df[df.columns.values[0]].str.split(expand=True)
            df[3] = df[3].replace({"BRABT": "OI", "BRATM": "OI","BRACS":"TIM", "BRARN":"TIM", "BRASP":"TIM", "BRATC":"VIVO", "BRAV1":"VIVO", "BRAV2":"VIVO", "BRAV3": "VIVO","BRATA":"CLARO","BRACL":"CLARO", "PRYHT":"INTERNACIONAL", "PRYNP":"INTERNACIONAL", "PRYTC": "INTERNACIONAL"})
            df[4] = pd.to_numeric(df[4])
            df = df.rename(columns={0: "imsi", 1: "msisdn", 2:"apn", 3:"operadora", 4: 'trafego'})
            dfsum = df.groupby(['imsi','operadora','msisdn', 'apn'])['trafego'].agg(['sum', 'count'])

            if 'BRAC3' in name:
                print("EXEC: salvando arquivo em BRAC3")
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRAC3.csv", "w")
            elif 'BRAC4' in name:
                print("EXEC: salvando arquivo em BRAC4")
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRAC4.csv", "w")
            elif 'BRACT' in name:
                print("EXEC: salvando arquivo em BRACT")
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRACT.csv", "w")
            elif 'BRAI1' in name:
                print("EXEC: salvando arquivo em BRAI1")
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRAI1.csv", "w")
            elif 'BRAI2' in name:
                print("EXEC: salvando arquivo em BRAI2")
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRAI2.csv", "w")
            elif 'BRAI3' in name:
                print("EXEC: salvando arquivo em BRAI3")
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/BRAI3.csv", "w")
            else:
                print("ERRO: salvando arquivo em erro")
                arquivo = open("/dados/persistence/kafka-connect/jars/diario/erro.csv", "w")

            arquivo.writelines(dfsum.to_csv())
            arquivo.close()
        else:
            print('ERRO: Arquivo muito pequeno ou muito grande')

    @staticmethod
    def atualizarMensal(dir_name_mensal, dir_name_diario, extension):
        
        os.chdir(dir_name_diario)
        for item in os.listdir(dir_name_diario):  # loop through items in dir
            if item.endswith(extension):  # check for ".csv" extension
                mensal_path = dir_name_mensal+'/mensal_'+item
                file_name = os.path.abspath(item)  # get full path of files
                if os.path.isfile(mensal_path):
                    juntarMensalDiario(mensal_path, file_name)
                else:
                    diario = pd.read_csv(file_name)
                    diario = agregarMensal(diario)
                    mensal = open(mensal_path, 'w')
                    mensal.writelines(diario.to_csv(index=False))
                    mensal.close()

    
def juntarMensalDiario(mensal_path, diario_path):
    # Abre os arquivos diario e mensal
    print("EXE: juntando "+ diario_path+ " com " + mensal_path)
    diario = pd.read_csv(diario_path)
    mensal = pd.read_csv(mensal_path)

    ## Colocar os a coluna Produto e agregar por produtos e operadora
    diario = agregarMensal(diario)

    ## Concatenar o novo diario com o mensal
    mensal_resu = pd.concat([diario,mensal]).groupby(['operadora','Produto'], as_index=False).sum()
        
    ## Salvar o novo mensal
    f = open(mensal_path, 'w')
    f.writelines(mensal_resu.to_csv(index=False))
    f.close()

def agregarMensal(df):
    ## transforma o imsi em string para poder comparar mais tarde
    f = lambda x: '' if type(x) == np.nan else str(x)
    df['imsi'] = df['imsi'].map(f)

    ## criando a coluna produto a partir da imsi
    df['Produto'] = df['imsi'].map(pegarProduto)
    return df.groupby(['operadora', 'Produto'])['sum','count'].sum().reset_index()

def pegarProduto(x):
    if x.startswith('7243287000') or x.startswith('7243287001'):
        return 'MVNO(Vecto)' 
    elif x.startswith('7243287100'):
        return 'ISM(JT)'
    elif x.startswith('7243202') or x.startswith('7243302') or x.startswith('7243402'):
        return 'IOT'
    else: 
        return 'Varejo'
