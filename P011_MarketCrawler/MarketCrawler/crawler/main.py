'''
Created on 06.05.2021

@author: Markus
'''

# Portale
import MarketCrawler.market.onvista as onvista;

# 
import pandas as pd;
import time;

# Datenbank
import MarketCrawler.database.main as db;

# Web URL
import requests;
from bs4 import BeautifulSoup;

class Web():
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''     
        self.db2 = db.Database();
#        self.CrawlerDB = self.db2.Connect('.\\json\\DB_MarketCrawler.json');
        self.CrawlerDB = self.db2.Connect('.\\json\\DB_MarketCrawler_MSSQL.json');
    
    def Init_AssetClass(self):
        '''
        
        '''
        
        # inistal s02 kurs last auf 2000-01-01
        # inistal d_Assetclass index, aktien ...
        # initail märkte ??
        
        mOnvista = onvista.OnVista();
        
        v_query = "select a.ID, b.URL, a.URL_AssetClass \
                from s01_stage.Portal_AssetClass a \
                left join s01_stage.Portal b on a.Portal_ID_F = b.ID \
                WHERE a.Initial = 1;"
        
        result_q = self.db2.Select(self.CrawlerDB, v_query)
        
        print('Init_AssetClass')
        print(result_q)

        if result_q:
            for result in result_q:
                try:
                    print('>> URL : ' + result[1] + result[2]);
            
                    html_doc = self.FetchPage(result[1] + result[2]);
                    
                    mOnvista.Extract_Indizies(html_doc);
                    
                    v_query = "UPDATE 01_stage.Portal_AssetClass SET Asset_Initial = 0 WHERE ID = " + str(result[0]) + ";"
                    
    #                self.db2.Update(self.CrawlerDB, v_query)
                    
                except BaseException as e:
                    print('Error : ', e)
        return;
        
    def FetchPage(self, v_url):
        '''
        
        '''
        print ('>> FetchPage : ' + v_url)
        r = requests.get(v_url);
        
        doc = BeautifulSoup(r.text, 'html5lib');
        
        return doc;
    
    def Download_File(self, v_Portal, v_ID, v_url):
        '''
        
        '''
        print ('>> Download_File : ' + v_url)
        
        time.sleep(1);
        
        r = requests.get(v_url);
        
        print('>> Download_File :', r)
        
        v_file = '.\\kursdaten\\' + v_Portal + '_'+ v_ID + '_kurs_daten.csv'
        
        open(v_file, 'wb').write(r.content)
        
        return;
    
    def Read_File(self, v_Portal, v_ID):
        '''
        x
        '''
        
        v_file = '.\\kursdaten\\' + v_Portal +'_' + v_ID + '_kurs_daten.csv'
        
        try:        
            df = pd.read_csv(v_file, dayfirst=True, parse_dates=[0], delimiter=';', thousands='.', decimal=',');
        except:
            df = pd.DataFrame()
        
        return df;
    
    def DataFrame_Asset(self):
        '''
        x
        '''
        
        df_name = {'Portal_ID_F': [], 'Assetclass_ID_F': [], 'Name': [], 'URL_Page': [], 'URL_Page_2': [], 'Titel': [], 'Asset_Initial': [], 'ExterneID':[], 'Kurs_Daten':[], 'Top_Asset_ID_F':[], 'Kurs_Datum':[]}
        df = pd.DataFrame(df_name)
        
        return df;
    
    def DataFrame_Asset_TopAsset(self):
        '''
        x
        '''
        
        df_name = {'Portal_ID_F': [], 'Asset_ID_F': [], 'Top_Asset_ID_F': []}
        df = pd.DataFrame(df_name)
        
        return df;
    
    def Dataframe_P1_Asset_Beschaeftigte(self):
        '''
        x
        '''
        
        df_name = {'Portal_ID_F':[], 'AssetClass_ID_F':[], 'Asset_ID_F':[], 'Jahr':[], 'Beschaeftigt':[]}
        df = pd.DataFrame(df_name)
        
        return df;
        
    def Dataframe_P1_Asset_Kontakt(self):
        '''
        x
        '''
        
        df_name = {'Portal_ID_F':[], 'AssetClass_ID_F':[], 'Asset_ID_F':[], 'Name':[], 'Strasse':[], 'PLZ':[], 'Ort':[], 'Telefon':[], 'Fax':[], 'EMail':[], 'Url':[]}
        
        df = pd.DataFrame(df_name)
        
        return df;  
        
    def Dataframe_P1_Asset_Marktdaten(self):
        '''
        x
        '''
        
        df_name = {'Portal_ID_F':[], 'AssetClass_ID_F':[], 'Asset_ID_F':[], 'Kapitalisierung':[], 'Anzahl_Aktien':[], 'Streubesitz':[]}
        
        df = pd.DataFrame(df_name)
        
        return df;    
    
    def Dataframe_P1_Asset_Profil(self):
        '''
        x
        '''
        
        df_name = {'Portal_ID_F':[], 'AssetClass_ID_F':[], 'Asset_ID_F':[], 'Profil':[]}
        
        df = pd.DataFrame(df_name)
        
        return df;
        
    def Dataframe_P1_Asset_Stammdaten(self):
        '''
        x
        '''
        
        df_name = {'Portal_ID_F':[], 'AssetClass_ID_F':[], 'Asset_ID_F':[], 'WKN':[], 'ISIN':[], 'Symbol':[], 'Land_ID_F':[], 'Branche_ID_F':[], 'Sektor_ID_F':[], 'Typ_ID_F':[], 'Nennwert':[], 'Unternehmen': [], 'Risiko':[], 'Chance':[], 'Familie':[]}
        df = pd.DataFrame(df_name)
        
        return df;
    
    def Dataframe_P1_Asset_Termine(self):
        '''
        x
        '''
        
        df_name = {'Portal_ID_F':[], 'AssetClass_ID_F':[], 'Asset_ID_F':[], 'Datum':[], 'Anlass':[]}
        
        df = pd.DataFrame(df_name)
        
        return df;
    
class Setup(Web):
    
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''     
    
class Measure(Web):
    
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        Web.__init__(self)
        
        print('Init Measure');
        
    def SMA(self, x=None, df=None):
        '''
        x
        '''
        
        r = self.__getAsset()
        
        df_result = pd.DataFrame()
        
        v_sql = "  SELECT a.ID, a.Portal_ID_F, a.AssetClass_ID_F, a.Asset_ID_F, a.Datum, a.[Close], b.ID as ID_F \
            FROM s02_dwh.Asset_Kursdaten a \
            LEFT JOIN s02_dwh.Asset_Measure b on a.Portal_ID_F = b.Portal_ID_F \
                AND a.AssetClass_ID_F = b.AssetClass_ID_F \
                AND a.Asset_ID_F = b.Asset_ID_F \
                AND a.Datum = b.Datum \
            WHERE a.Portal_ID_F = {1} AND a.AssetClass_ID_F = {2} AND a.Asset_ID_F = {3} AND Update_Date IS NULL \
            ORDER BY a.Portal_ID_F, a.AssetClass_ID_F, a.Asset_ID_F, a.Datum;"
            
        for row in r:
            
            v_query = v_sql
            v_query = v_query.replace('{1}', str(row[1]))
            v_query = v_query.replace('{2}', str(row[2]))
            v_query = v_query.replace('{3}', str(row[3]))
            
            r3 = self.db2.Select(self.CrawlerDB, v_query)
            
            df = pd.DataFrame.from_records(r3, columns=['ID', 'Portal_ID_F', 'AssetClass_ID_F', 'Asset_ID_F', 'Datum', 'Close', 'ID_F'])
        
            df['SMA38']=df['Close'].rolling(38).mean()
            df['SMA90']=df['Close'].rolling(90).mean()
            df['SMA200']=df['Close'].rolling(200).mean()
            
            df['SMA38'] = df['SMA38'].fillna(0)            
            df['SMA90'] = df['SMA90'].fillna(0)            
            df['SMA200'] = df['SMA200'].fillna(0)
            
            df_result = df_result.append(df)

            self.db2.Update_Measure(self.CrawlerDB, df_result)
            
            df_result = df_result.drop(df_result.index)
            
        return
    
    def EMA(self, x, df=None):
        '''
        x
        '''
        df = self.__getClose()
        
        df['EMA38']=df['Close'].ewm(span=x, adjust=False).mean()

        return
        
        return
    
    def __getAsset(self):
        '''
        x
        '''
        
        v_query = "SELECT ID, Portal_ID_F, AssetClass_ID_F, Asset_ID \
            FROM s02_dwh.Asset \
            where Kurs_Daten = 1 \
            order by Portal_ID_F, AssetClass_ID_F, Asset_ID;"
            
        r = self.db2.Select(self.CrawlerDB, v_query);
        
#        df = pd.DataFrame.from_records(r, columns=['ID', 'Portal_ID_F', 'AssetClass_ID_F', 'Asset_ID_F', 'Datum', 'Close'])
        
#        df_asset = df_close.iloc[:,1:4].drop_duplicates()
#        df_asset = df_asset.reset_index(drop=True)
        
        return r