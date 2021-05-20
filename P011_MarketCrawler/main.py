'''
Created on 06.05.2021

@author: Markus
''' 

'''if __name__ == '__main__':
    pass
    '''
    
import MarketCrawler.crawler.main as mc;
import MarketCrawler.market.onvista as onvista;


import numpy as np
import pandas as pd
import time

mcm = mc.Measure();

#Machen Sie dieses Beispiel reproduzierbar
#np.random.seed(0)

# Datensatz erstellen
#period = np.arange(1, 101, 1)
#leads = np.random.uniform(1, 20, 100)
#sales = 60 + 2*period + np.random.normal(loc=0, scale=.5*period, size=100)
#df = pd.DataFrame({'period': period, 'leads': leads, 'sales': sales})

# die ersten 10 Zeilen anzeigen
#print(df.head(10))

#df['rolling_sales_5'] = df['sales'].rolling(5).mean() 
#print(df.head(10))

print (mcm.SMA(38))
#print(mcm.EMA(38))
#webCrawler = mc.Web();
#mOnvista = onvista.OnVista();

#webCrawler.Init_AssetClass();
#print('Fin Init_AssetClass')

#mOnvista.Extract_Index_Profil(Init=0); # Stammdaten
#print('Fin Extract_Index_Profil()')

#mOnvista.Extract_Kurs(1);
#print('Fin Extract_Kurs(1)')

#mOnvista.Extract_Index_Einzelwerte();
#print('Fin Extract_Index_Einzelwerte()')

#mOnvista.Extract_Profil(Init=0);
#print('Fin Extract_Profil()')

#mOnvista.Extract_Kurs(2);
#print('Fin Extract_Kurs(2)')

print('Fin')