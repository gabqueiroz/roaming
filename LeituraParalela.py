import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

class LeituraParalela():

    @staticmethod
    def feature_calculation(df):
            df[df.columns.values[0]] = df[df.columns.values[0]].map(lambda x: x[3:18].strip() +' '+ x[188:209].strip() +' '+ x[95:119].strip() +' '+ x[20:25].strip() +' '+ x[147:159].strip())
            df = df[df.columns.values[0]].str.split(expand=True)
            df[3] = df[3].replace({"BRABT": "OI", "BRATM": "OI","BRACS":"TIM", "BRARN":"TIM", "BRASP":"TIM", "BRATC":"VIVO", "BRAV1":"VIVO", "BRAV2":"VIVO", "BRAV3": "VIVO","BRATA":"CLARO","BRACL":"CLARO"})
            df[4] = pd.to_numeric(df[4])
            df = df.rename(columns={0: "imsi", 1: "msisdn", 2:"apn", 3:"operadora", 4: 'trafego'})
            dfsum = df.groupby(['imsi','operadora','msisdn', 'apn'])['trafego'].agg(['sum', 'count'])
            return dfsum

    @staticmethod
    def parallel_feature_calculation_ppe(df, partitions=10, processes=4):
            # calculate features in paralell by splitting the dataframe into partitions and using paralell processes    
            df_split = np.array_split(df, partitions, axis=0)  # split dataframe into partitions column wise
            with ProcessPoolExecutor(processes) as pool:        
                    df = pd.concat(pool.map(LeituraParalela.feature_calculation, df_split))
            return df
