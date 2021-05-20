'''
Created on 06.05.2021

@author: Markus
'''

# Pandas
import pandas  as pd;

from datetime import datetime;

from dateutil.relativedelta import relativedelta;

import MarketCrawler.crawler.main as mc;

import MarketCrawler.database.main as db;

pd.options.display.max_columns = None;

class OnVista():
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        
        self.Portal = 'OnVista'
    
        self.db2 = db.Database();
#        self.CrawlerDB = self.db2.Connect('.\\json\\DB_MarketCrawler.json');
        self.CrawlerDB = self.db2.Connect('.\\json\\DB_MarketCrawler_MSSQL.json');
     
        v_query = "select ID, NAME, URL, Kurs_Datum_Initial from s01_stage.Portal where Name = '"+ self.Portal + "';"
    
        self.Portal = self.db2.Select(self.CrawlerDB, v_query); # Return List mit Tuple
        print ('Portal')
        print(self.Portal)
        
        self.Portal = self.Portal[0]; # Return Tuple            # MariaDB
        self.Portal_ID = self.Portal[0]; # 1 Wert Tuple        # MariaDB
        
        
#        self.Portal_ID = self.Portal[0]; # 1 Wert Tuple        # MS SQL
        
        self.Portal_Name = self.Portal[1];
        self.Portal_URL = self.Portal[2];
        self.Kurs_Datum_Initial = self.Portal[3];
        
        self.crawler = mc.Web();
            
    def Extract_Indizies(self, v_doc):
        '''
        x
        '''
        
#        crawler = mc.Web()

        df = self.crawler.DataFrame_Asset();
        
        lvl_01 = v_doc.find(class_ = 'MARKTUEBERSICHT');
        
        lvl_02 = lvl_01.find('table');
        
        lvl_03 = lvl_02.find('tbody');
        
        lvl_04 = lvl_03.find_all('tr');
        
        for row in lvl_04:
                        
            if row.find('th'):
                continue;

            lst = [];
            
            data = row.find_all('td');
            
            link = data[1].find_all('a', href = True);
            notID = data[7].find_all('span');
            
            db_Portal_ID = self.Portal_ID;
            db_Assetclass_ID_F = 1; # *************************************************** !!!!!!!!!!!!!!!!
            db_Name = data[1].find('a').contents[0];
            db_URL_Page =  link[0]['href'];
            
            lst_split = db_URL_Page.split('/');
            db_URL_Page_2 = lst_split[-1];
            
            db_Titel = link[0]['title'];
            db_Initial = None; # wird initial in der Datenbank auf True gestellt
            db_notationID = notID[0]['data-id']
            db_Kursdaten = None;# wird Asset_Initial in der Datenbank auf True gestellt
            db_Kurs_Datum = self.Kurs_Datum_Initial;
            db_Top_Asset = None;
            
            lst.extend((db_Portal_ID, db_Assetclass_ID_F, db_Name, db_URL_Page, db_URL_Page_2, db_Titel, db_Initial, db_notationID, db_Kursdaten, db_Top_Asset, db_Kurs_Datum));

            lst_series = pd.Series(lst,  index=df.columns);
            
            df = df.append(lst_series, ignore_index=True);
            
        self.db2.Insert(self.CrawlerDB, 'Asset', df)
        
        return;
    
    def Extract_Kurs(self, v_AssetClass, v_Index=None):
        '''
        x
        '''
        
        crawler = mc.Web();

        cDate = datetime.now().date();

        if v_Index == None:
            v_query = "SELECT ID, ExterneID, Kurs_Datum, Kurs_Update_Intervall FROM s01_stage.Asset \
                WHERE AssetClass_ID_F = " + str(v_AssetClass) + " and Kurs_Daten = 1 and Kurs_Datum <= '" +cDate.strftime("%Y-%m-%d") + "';"

        result_q = self.db2.Select(self.CrawlerDB, v_query);
        print(result_q)
        v_Interval = 5;
        
        if result_q:
            for result in result_q:
        
                print(result)        
                nDate = result[2];
            
                print (cDate, self.Kurs_Datum_Initial, nDate)
    
                while nDate < cDate:
                    
                    print('1 while ', nDate, cDate);
                    
                    nDate = nDate + relativedelta(days=1)
                    
                    v_url = "https://www.onvista.de/onvista/boxes/historicalquote/export.csv?notationId="+ result[1] + "&dateStart="+ nDate.strftime("%d.%m.%Y") +"&interval=Y" + str(v_Interval)
        
                    crawler.Download_File(self.Portal_Name, result[1], v_url)
        
                    df = crawler.Read_File(self.Portal_Name, result[1]);
                    
                    print(df.head(5))
                    
                    if df.empty:
                        
                        nDate = nDate + relativedelta(years=v_Interval);
                        print('2 empty ', nDate, cDate)                    
                        if nDate > cDate: # Letztes Intervall endet in der Zukunft, keine Daten vorhanden
                            v_query = "UPDATE s01_stage.Asset SET Kurs_Daten = 0, Kurs_Update = NULL WHERE ID =" + str(result[0]) + ";"
                            self.db2.Update(self.CrawlerDB, v_query);
                            print('3 Break.')
                            break;
                        else:
                            print ('4 Continue.')
                            continue; # Letztes Intervall Leer, Prüfe auf nächstes Intervall
                        
                    if not df.empty:
                        
                        df['Portal_ID_F'] =self.Portal_ID;
                        df['AssetClass_ID_F'] = v_AssetClass;
                        df['Asset_ID_F'] = result[0];
                        df['Market_ID_F'] = None;
                        
                        df['Datum'] = pd.to_datetime(df['Datum'], format='%d%m%y');
                        
                        self.db2.Insert(self.CrawlerDB, 'Asset_Kursdaten', df)
                        
                        nDate = df['Datum'].max() # Setzte Next auf Max. Datum 
                        
                        v_query = "UPDATE s01_stage.Asset SET Kurs_Datum = '" + str(nDate) + "', Kurs_Update = '" + str(cDate + relativedelta(days=result[3])) +  "' WHERE ID = " + str(result[0]) + ";"
                        
                        self.db2.Update(self.CrawlerDB, v_query);
                    
        return;
    
        
    # Index
    
    def Extract_Index_Profil(self, Init=1, v_Index=None):
        '''
        x
        '''
        
#        crawler = mc.Web();
            
        v_query = "SELECT ID, Portal_ID_F, AssetClass_ID_F, URL_PAGE, Asset_Initial \
            FROM s01_stage.Asset \
            WHERE AssetCLass_ID_F = 1 AND Asset_Initial = " + str(Init) + " \
            ORDER BY ID, Portal_ID_F, AssetClass_ID_F;"
        
        result_q = self.db2.Select(self.CrawlerDB, v_query);

        if result_q:
            for result in result_q:
    
                db_Portal_ID_F = self.Portal_ID;
                db_Asset_ID_F = result[0];
                db_Assetclass_ID_F = result[2]
                
                db_URL_Page = result[3]
                                
                html_doc = self.crawler.FetchPage(db_URL_Page);
                
                df = self.__Extract_Index_Details(html_doc, db_Portal_ID_F, db_Asset_ID_F, db_Assetclass_ID_F)
                
                self.db2.Insert(self.CrawlerDB, 'P1_Asset_Stammdaten', df)
                          
                #self.__Update_Initial(result[0], result[4])

        return;
    
    def Extract_Index_Einzelwerte(self, **args):
        '''
        Args für WKN, ISIN, ...
        '''
        
        crawler = mc.Web();
        
        df = crawler.DataFrame_Asset();
        
        v_query = "SELECT a.ID, a.Portal_ID_F, a.AssetSubtyp_ID_F, a.URL_AssetClass, a.URL_Einzelwert, b.URL_Page_2, b.ID as Asset_ID \
            FROM s01_stage.Portal_AssetClass a \
            LEFT JOIN s01_stage.Asset b ON a.Portal_ID_F = b.Portal_ID_F AND a.ID = b.AssetClass_ID_F \
            WHERE URL_Einzelwert IS NOT NULL;"
        
        result_q = self.db2.Select(self.CrawlerDB, v_query)
        
        if result_q:
            for result in result_q:
                
                db_Portal_ID = self.Portal_ID;
                db_AssetSubtyp = result[2]
                print(self.Portal_URL, result[3], result[4], result[5])
                URL = self.Portal_URL + result[3] + result[4] + result[5];
    
                html_doc = crawler.FetchPage(URL);
                
                page_navi = html_doc.find_all(class_ = 'BLAETTER_NAVI');
    
                if page_navi:
                    pages = self.__Extract_Pages(page_navi)
                    URL = URL + '?page='
                else:
                    pages = 1;
                
                p = 1
                while p <= pages:    
                    # HTML Extraktion
                    try:
                        lvl_01 = html_doc.find(class_= 'alle-einzelwerte-container');
                        
                        lvl_02 = lvl_01.find(class_ = 'table_box_content_zebra');
                        
                        lvl_03 = lvl_02.find_all('tr')
                        
                        for row in lvl_03:
                            
                            lst=[]
                            
                            data = row.find_all('td');
                            
                            link = data[0].find_all('a', href=True);
                            
                            db_URL_Page = link[0]['href'];
                            
                            db_Name = data[0].find('a').contents[0];
                            
                            lst_split = db_URL_Page.split('/');
                            db_URL_Page_2 = lst_split[-1];
                            
                            db_Titel = link[0]['title'];
                            
                            notID = data[2].find_all('strong');
                            notID = notID[0]['data-push']
                            notID = notID.split(':');
                            
                            db_notationID = notID[0];
                            db_Initial = True;
                            db_Kursdaten = True;
                            db_Kurs_Datum = self.Kurs_Datum_Initial;
                            db_Top_Asset = result[6];
                            
                            lst.extend((db_Portal_ID, db_AssetSubtyp, db_Name, db_URL_Page, db_URL_Page_2, db_Titel, db_Initial, db_notationID, db_Kursdaten, db_Top_Asset, db_Kurs_Datum));
            
                            lst_series = pd.Series(lst,  index=df.columns);
                        
                            df = df.append(lst_series, ignore_index=True);
    
                        self.db2.Insert(self.CrawlerDB, 'Asset', df);
                    
                        df = df.iloc[0:0];
    
                    except BaseException as e:
                        print('Error : ' , e);
                    
                    print('PAGES ', p, pages);
                    
                    if p <= pages:
                        p +=1;
                        URL_P = URL + str(p);
                        
                        if p <= pages:
                            html_doc = crawler.FetchPage(URL_P);
                        else: 
                            break;
                
                v_query = "UPDATE s01_stage.Portal_AssetClass SET Initial = 0 WHERE ID = " + str(db_Portal_ID) + ";"
                    
                self.db2.Update(self.CrawlerDB, v_query)    
                    
        return;
    
    def Extract_Profil(self, Init = 1):
        '''
        x
        '''
        
        crawler = mc.Web();
        
#        cDate = datetime.now().date();
        
        v_query = "SELECT a.ID, a.Portal_ID_F, a.AssetClass_ID_F, b.URL_AssetClass, b.URL_Profil_1, a.URL_Page_2, a.Asset_Update_Intervall \
            FROM s01_stage.Asset a \
            left join s01_stage.Portal_AssetClass b on a.Portal_ID_F = b.Portal_ID_F and a.AssetClass_ID_F = b.Assettyp_ID_F \
            where assetclass_id_f = 2 and a.Asset_Initial = " + str(Init) + " \
            ORDER BY a.ID, Portal_ID_F, a.AssetClass_ID_F;"
        
        result_q = self.db2.Select(self.CrawlerDB, v_query);
        
        for result in result_q:
            
            db_Portal_ID = result[1]
            db_AssetClass_ID = result[2]
            db_Asset_ID = result[0]
            
            lst = []
            
            lst.extend((db_Portal_ID, db_AssetClass_ID, db_Asset_ID))
            
            URL = self.Portal_URL + result[3] + result[4] + result[5];
            
            html_doc = crawler.FetchPage(URL);
            
            # Profil
            
            df_2 = self.__Profil_Profil(html_doc, lst.copy())
            
            # Kontakt
            
            df_1 = self.__Profil_Kontakt(html_doc, lst.copy())

            # Stammndaten
            
            df_3 = self.__Profil_Stammdaten(html_doc, lst.copy())

            # Marktdaten
            
            df_4 = self.__Profil_Marktdaten(html_doc, lst.copy())
            
            # Beschäftigte
            
            df_5 = self.__Profil_Beschaeftigte(html_doc, lst.copy())
            
            # Termine
            
            df_6 = self.__Profil_Termine(html_doc, lst.copy())
            
            #self.db2.Insert(self.CrawlerDB, 'P1_Asset_Profil', df_2);
            
            #self.db2.Insert(self.CrawlerDB, 'P1_Asset_Kontakt', df_1);
    
            self.db2.Insert(self.CrawlerDB, 'P1_Asset_Stammdaten', df_3);
    
            #self.db2.Insert(self.CrawlerDB, 'P1_Asset_Marktdaten', df_4);
    
            #self.db2.Insert(self.CrawlerDB, 'P1_Asset_Beschaeftigte', df_5);     
    
            #self.db2.Insert(self.CrawlerDB, 'P1_Asset_Termine', df_6);
            
            
            #self.__Update_Initial(result[0], result[6])
            #v_query = "UPDATE Asset SET Asset_Initial = 0, Asset_Update = '" + str(cDate + relativedelta(days=result[6])) +  "' WHERE ID = " + str(result[0]) + ";"
                    
            #self.db2.Update(self.CrawlerDB, v_query);        
        
        return;    
    
    def Extract_Profil_Bechaeftigte(self, html_doc):
        '''
        x
        '''
        
        lvl_01 = html_doc.find(class_ = 'BESCHAEFTIGTE');
        
        lvl_02 = lvl_01.find('table');
        
        
        lst_j = [];
        lst_v = [];
            
        try:
            
            lvl_03 = lvl_02.find_all('th'); # Header Jahreszahl
        
            for x in lvl_03:
                lst_j.append(x.contents[0].strip())
                
            lvl_04 = lvl_02.find_all('td') # Werte
            
            for y in lvl_04:
                lst_v.append(y.contents[0].strip().replace(".", ""))
        
        except:
            lst_j = [0]
            lst_v = [0]
            
        df = pd.DataFrame(lst_v, index=lst_j, columns=['Beschaeftigt'])
        df.reset_index(level=0, inplace=True)
        df.rename(columns={'index': 'Jahr'})

        return df;
    
    def Extract_Profil_Profil(self, html_doc):
        '''
        x
        '''
        
        lvl_01 = html_doc.find(class_ = 'FIRMENPROFIL');
        
        try:
            db_Profil = lvl_01.find('p').getText();
        except:
            db_Profil = "Kein Profil vorhanden.";
            
        return db_Profil;
    
    def Extract_Profil_Kontakt(self, html_doc):
        '''
        x
        '''
        
        lvl_01 = html_doc.find(class_ = 'KONTAKT');
        
        lvl_02 = lvl_01.find('div');
        
        lvl_03 = lvl_02.find_all('li')
        
        i = 0
        
        lst = []

        db_Name = None;
        db_Strasse = None;
        db_PLZ = None;
        db_Ort = None;
        db_URL =None;
        db_Telefon = None;
        db_Fax = None;
        db_EMail = None;
                    
        for row in lvl_03:
            
            i += 1
            
            try:
                if i == 1: db_Name = row.contents[0];
                if i == 2: db_Strasse = row.contents[0];
                if i == 3: 
                    db_PLZ = row.contents[0].split(" ")[0];
                    db_Ort = row.contents[0].split(" ")[1:];
                if i == 4: 
                    db_URL = row.find('a', href=True)['href'];
                if i == 5: db_Telefon = row.contents[0].split(" ")[1];
                if i == 6: db_Fax = row.contents[0].split(" ")[1];
                if i == 7: db_EMail = row.contents[0].split(" ")[1];
            except :
                pass;
            
        lst.extend((db_Name, db_Strasse, db_PLZ, db_Ort, db_Telefon, db_Fax, db_EMail, db_URL));        
            
        return lst;
    
    def Extract_Profil_Marktdaten(self, html_doc):
        '''
        x
        '''
        
        lvl_01 = html_doc.find(class_ = 'KENNZAHLEN');
        
        lvl_02 = lvl_01.find('div');
        
        lvl_03 = lvl_02.find('tbody')

        lvl_04 = lvl_03.find_all('td')
        
        i = 0
        
        lst = []

        db_Kapitalisierung = None;
        db_Anzahl_Aktien = None;
        db_Streubesitz = None;
        
        for row in lvl_04:
            
            i += 1
            
            try:
                if i == 1: db_Kapitalisierung = row.contents[0];
                if i == 2: db_Anzahl_Aktien = row.contents[0].split(" ")[0].replace(".", "");
                if i == 3: 
                    db_Streubesitz = row.contents[0].replace("%", "");
                    db_Streubesitz = db_Streubesitz.replace(",", ".")
            except :
                pass;

        lst.extend((db_Kapitalisierung, db_Anzahl_Aktien, db_Streubesitz));

        return lst;
    
    def Extract_Profil_Stammdaten(self, html_doc):
        '''
        x
        '''
        
        lvl_01 = html_doc.find(class_ = 'STAMMDATEN');
        
        lvl_02 = lvl_01.find('div');
        
        lvl_03 = lvl_02.find_all('dd')
        
        i = 0
        
        lst = []

        db_WKN = None;
        db_ISIN = None;
        db_Symbol = None;
        db_Land = None;
        db_Branche =None;
        db_Sektor = None;
        db_Typ = None;
        db_Nennwert = None;
        db_Unternehmen = None;
        
        db_Risiko = None;
        db_Chance = None;
        db_Familie = None;
        
        for row in lvl_03:

            i += 1
            
            try:
                if i == 1: db_WKN = row.contents[0];
                if i == 2: db_ISIN = row.contents[0];
                if i == 3: db_Symbol = row.contents[0]
                if i == 4: db_Land = row.contents[1];                    
                if i == 5: db_Branche = row.contents[0];
                if i == 6: db_Sektor = row.contents[0];
                if i == 7: db_Typ = row.contents[0];
                if i == 8: 
                    db_Nennwert = row.contents[0].replace('n.a.', '0');
                    db_Nennwert = db_Nennwert.replace(",", ":");
                    db_Nennwert = db_Nennwert.replace(".", "");
                    db_Nennwert = db_Nennwert.replace(":", ".");

                if i == 9: db_Unternehmen = row.contents[0];
            except :
                pass;
            
        lvl_01_1 = html_doc.find(class_ = 'WERTPAPIER_DETAILS')
        
        lvl_02_1 = lvl_01_1.find_all('dd')
        
        i = 0
        
        for row in lvl_02_1:
            i += 1
            
            data = row.find('span')
            
            try:
                if i == 3: db_Chance = data.contents[0]
                if i == 6: db_Risiko = data.contents[0]
            except:
                pass;    
        
        lst.extend((db_WKN, db_ISIN, db_Symbol, db_Land, db_Branche, db_Sektor, db_Typ, db_Nennwert, db_Unternehmen, db_Risiko, db_Chance, db_Familie));        
        
        return lst;
    
    def Extract_Profil_Termine(self, html_doc):
        '''
        x
        '''
        
        lvl_01 = html_doc.find(class_ = 'TERMINE');
        
        lvl_02 = lvl_01.find('div');
        
        lvl_03 = lvl_02.find('tbody')

        lvl_04 = lvl_03.find_all('tr')
        
        lst = []
        
        db_Datum = None;
        db_Anlass = None;
        
        for row in lvl_04:
            lst_t = []
            
            try:
                db_Datum = row.find_all('td')[0].getText().strip()
                db_Anlass = row.find_all('td')[1].getText().strip()
            except:
                pass;
            
            lst_t.extend((db_Datum, db_Anlass));
            lst.append(lst_t);
            
        df = pd.DataFrame(lst, columns=['Datum', 'Anlass']);
        
        return df;
    
    def __Update_Initial(self, v_id, v_intervall):
        '''
        x
        '''
        
        cDate = datetime.now().date();
        
        v_query = "UPDATE s01_stage.Asset SET Asset_Initial = 0, Asset_Update = '" + str(cDate + relativedelta(days=v_intervall)) +  "' WHERE ID = " + str(v_id) + ";"
        
        self.db2.Update(self.CrawlerDB, v_query);  
        
        return;
    
    def __Extract_Index_Details(self, html_doc, db_Portal_ID_F, db_Asset_ID_F, db_Assetclass_ID_F):
        '''
        x
        '''
        crawler = mc.Web();

        df = crawler.Dataframe_P1_Asset_Stammdaten();
        
        # HTML Extraktion
                
        lvl_01 = html_doc.find(class_ = 'WERTPAPIER_DETAILS');
        
        lvl_02 = lvl_01.find_all('dd');
        
        i = 0;
        
        db_WKN = None;
        db_ISIN = None;
        db_Symbol=None;
        db_Typ = None;
        db_Familie = None;
        db_Land = None;
        
        for row in lvl_02:
            i += 1;
            
            lst = [];
            
            if (i == 1) or (i == 3): # extract label
                v_input = row.find_all('input');
                
                if i == 1:
                    try:
                        db_WKN = v_input[0]['value'];
                    except IndexError:
                        db_WKN = None;
                        
                if i == 3:
                    try:
                        db_ISIN = v_input[0]['value'];
                    except IndexError:
                        db_ISIN = None;
                        
            if (i == 2) or (i ==5) or (i == 6):
                                
                if i == 2:
                    try:
                        db_Symbol = row.contents[0];
                    except IndexError:
                        db_Symbol = None;
                        
                if i == 5:
                    try:
                        db_Typ = row.contents[0];
                    except IndexError:
                        db_Typ = None;
                        
                if i == 6:
                    try:
                        db_Familie = row.contents[0];
                    except IndexError:
                        db_Familie = None;
            if i == 4:
                try:
                    v_span = row.find_all('span');
                    db_Land = v_span[0]['title'].split(": ")[1];
    
                except IndexError:
                    db_Land = None;
                    
        db_Land_ID_F = db_Land;
        db_Branche_ID_F = None;
        db_Sektor_ID_F = None;
        db_Typ_ID_F = db_Typ;
        db_Nennwert = None;
        db_Unternehmen = None;
        db_Risiko = None;
        db_Chance = None;
        
        lst.extend((db_Portal_ID_F, db_Assetclass_ID_F, db_Asset_ID_F, db_WKN, db_ISIN, db_Symbol, db_Land_ID_F, db_Branche_ID_F, db_Sektor_ID_F, db_Typ_ID_F, db_Nennwert, db_Unternehmen, db_Risiko, db_Chance, db_Familie));
        
        lst_series = pd.Series(lst,  index=df.columns);
        
        df = df.append(lst_series, ignore_index=True);

        return df;
    
    def __Extract_Pages(self, v_page):
        '''
        x
        '''
#        print('>> __Extract_Pages : ', v_page)
        href = v_page[0].find_all('a')
        href = href[-1]['href']
        href = href.split('&')[-1]
        href = href.split('=')[-1]
#        print('>> __Extract_Pages : ', href);
        
        return int(href);
    
    def __Profil_Beschaeftigte(self, html_doc, lst):
        '''
        x
        '''
        
        crawler = mc.Web();
        
        df = crawler.Dataframe_P1_Asset_Beschaeftigte();
        
        df_r = self.Extract_Profil_Bechaeftigte(html_doc);
            
        df_r = df_r.rename(columns={'index': 'Jahr'})
        
        df_r['Portal_ID_F'] = lst[0];
        df_r['AssetClass_ID_F'] = lst[1];
        df_r['Asset_ID_F'] = lst[2];
            
        df = df.append(df_r, ignore_index=True, sort=False);
        
        return df;
    
    def __Profil_Profil(self, html_doc, lst):
        '''
        x
        '''
        
        crawler = mc.Web();
        
        df = crawler.Dataframe_P1_Asset_Profil();

        str_r = self.Extract_Profil_Profil(html_doc);            
        
        lst += [str_r];
        
        lst_s = pd.Series(lst,  index=df.columns);
        
        df = df.append(lst_s, ignore_index=True);
        
        return df;
    
    def __Profil_Kontakt(self, html_doc, lst):
        '''
        x
        '''

        crawler = mc.Web();
        
        df = crawler.Dataframe_P1_Asset_Kontakt();
        
        lst_r = self.Extract_Profil_Kontakt(html_doc);            

        lst.extend(lst_r);
        
        lst_s = pd.Series(lst,  index=df.columns);
          
        df = df.append(lst_s, ignore_index=True);
        
        return df;
    
    def __Profil_Marktdaten(self, html_doc, lst):
        '''
        x
        '''
        
        crawler = mc.Web();
        
        df = crawler.Dataframe_P1_Asset_Marktdaten();
        
        lst_r = self.Extract_Profil_Marktdaten(html_doc);            

        lst.extend(lst_r);
           
        lst_s = pd.Series(lst,  index=df.columns);
            
        df = df.append(lst_s, ignore_index=True);

        return df;
    
    def __Profil_Stammdaten(self, html_doc, lst):
        '''
        x
        '''
        
        crawler = mc.Web();
        
        df = crawler.Dataframe_P1_Asset_Stammdaten();
        
        lst_r = self.Extract_Profil_Stammdaten(html_doc);            

        lst.extend(lst_r);
            
        lst_s = pd.Series(lst,  index=df.columns);
            
        df = df.append(lst_s, ignore_index=True);
        
        return df;
    
    def __Profil_Termine(self, html_doc, lst):
        '''
        x
        '''
        
        crawler = mc.Web();
        
        df = crawler.Dataframe_P1_Asset_Termine();
            
        df_r = self.Extract_Profil_Termine(html_doc);         

        df_r['Portal_ID_F'] = lst[0];
        df_r['AssetClass_ID_F'] = lst[1];
        df_r['Asset_ID_F'] = lst[2];

        df = df.append(df_r, ignore_index=True, sort=False);
        
        return df;