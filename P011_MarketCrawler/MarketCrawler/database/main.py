'''
Created on 06.05.2021

@author: Markus
'''

import mysql.connector;

import pyodbc;

import json;

import pandas as pd;
import numpy as np;

from datetime import datetime;

class Database(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''

        
    def Connect(self, v_File):
        '''
        Connect To Database
        '''

        jf = open(v_File);
        credentials = json.load(jf);
        jf.close
        
        v_host = credentials["host"];
        v_user = credentials["user"];
        v_password = credentials["password"];
        v_database = credentials["database"];
        
#        mydb = mysql.connector.connect(host = v_host, user= v_user, password = v_password, database = v_database);
        mydb = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+v_host+';DATABASE='+v_database+';UID='+v_user+';PWD='+ v_password)
        return mydb;
    
    def Close(self, db):
        '''
        Close Database
        '''
        
        db.close();
        
        return;
        
    def Select(self, mydb, v_sql):
        '''
        Select Statement
        '''

        mycursor = mydb.cursor();
        mycursor.execute(v_sql);
        myresult = mycursor.fetchall();

        return myresult;
    
        
    def Update(self, db, v_sql):
        '''
        Update Statement
        '''
        cursor = db.cursor();
        cursor.execute(v_sql);
        
        db.commit()
        return;
    
    def Update_Measure(self, db, df):
        '''
        x
        '''
        
        cursor = db.cursor();
        
        d = datetime.now()
        
        l = 0
        
        for index, row in df.iterrows():
            
            cursor.execute("UPDATE s02_dwh.Asset_Measure SET Datum=?, SMA38=?, SMA90=?, SMA200=?, Update_Date=? WHERE ID=?", (row.Datum, row.SMA38, row.SMA90, row.SMA200, d, row.ID_F));
            
            l += 1
            
            if l % 500 == 0 and l != 0:
                
                print('Commit : ', (l) )
                
                # db.commit() # Commit je 500 Datensätze
        
        db.commit() # Rest unter 500
        print ('Datensätze hinzugefügt : ', (l))
        
        return
    
    def Insert(self, db, v_Table, df):
        '''
        x
        '''
        
        if v_Table == 'Asset':
            self.__Insert_Asset(db, df);
            self.__Insert_Asset_Top_Asset(db, df);
                
        if v_Table == 'Asset_Kursdaten':
            self.__Insert_Asset_Kursdaten(db, df);
        
        if v_Table == 'P1_Asset_Stammdaten':
            r = self.__Lookup_P1_Asset_Stammdaten(db, df);

            if r:
                self.__Update_P1_Asset_Stammdaten(db, df, r);
            else:
                self.__Insert_P1_Asset_Stammdaten(db, df);
        
        if v_Table == 'P1_Asset_Beschaeftigte':
            self.__Insert_P1_Asset_Beschaeftigte(db, df);
        
        if v_Table == 'P1_Asset_Kontakt':
            self.__Insert_P1_Asset_Kontakt(db, df);
        
        if v_Table == 'P1_Asset_Marktdaten':
            self.__Insert_P1_Asset_Marktdaten(db, df);
        
        if v_Table == 'P1_Asset_Profil':
            self.__Insert_P1_Asset_Profil(db, df);
        
        if v_Table == 'P1_Asset_Termine':
            self.__Insert_P1_Asset_Termine(db, df);
        
        
        return;
  

    def __Insert_Asset(self, db, df):
        '''
        x
        '''
        
        c = db.cursor();

        # Hole Vorhandene Datensätze
        v_query = "SELECT ID, ExterneID FROM s01_stage.Asset;"
            
        c.execute(v_query);
        r = c.fetchall();
        
        # Erstelle Dataframe
        df_r = pd.DataFrame(r, columns=['sqlID','ExterneID'])

        # Join 
        df_m = pd.merge(df, df_r, how='left', on=['ExterneID'])
        
        # Filtera auf neue Datensätze (sqlID = Null)
        df_n = df_m[df_m.sqlID.isnull()]
        
        for index, row in df_n.iterrows():
            # Asset_Initial und Kurs_Daten werden in der Datenbank per Default auf True gestellt
            c.execute("INSERT INTO s01_stage.Asset (Portal_ID_F, AssetClass_ID_F, Name, URL_Page, URL_Page_2, Titel, ExterneID, Kurs_Datum) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);", (row.Portal_ID_F, row.Assetclass_ID_F, str(row.Name), row.URL_Page, row.URL_Page_2, row.Titel, row.ExterneID, row.Kurs_Datum))
            
        db.commit();
        
        return;
    
    def __Insert_Asset_Top_Asset(self, db , df):
        '''
        x
        '''
        
        c = db.cursor();

        # Hole vorhandene Datensätze
        v_query = "SELECT ID, ExterneID FROM s01_stage.Asset;"
            
        c.execute(v_query);
        r = c.fetchall();
        
        # Erstelle Dataframe
        df_r = pd.DataFrame(r, columns=['sqlID','ExterneID'])
        
        # Join an neuen Extrakt 
        df_m = pd.merge(df, df_r, how='left', on=['ExterneID'])
        
        #
        df_c = df_m[['Portal_ID_F', 'Top_Asset_ID_F','sqlID' ]].copy()
        
        # Hole vorhandene Datensätze
        v_query = "SELECT * FROM s01_stage.Asset_TopAsset;"
        
        c.execute(v_query);
        r = c.fetchall();
        
        # Erstelle Dataframe
        df_r = pd.DataFrame(r, columns=['ID', 'Portal_ID_F', 'Asset_ID_F','Top_Asset_ID_F'])
        
        # Join 
        df_m = pd.merge(df_c, df_r, how='left', left_on=['Portal_ID_F', 'Top_Asset_ID_F', 'sqlID'], right_on=['Portal_ID_F','Top_Asset_ID_F', 'Asset_ID_F'])
        
        # Filtera auf neue Datensätze (sqlID = Null)
        df_n = df_m[df_m.ID.isnull()]
                
        for index, row in df_n.iterrows():
                                
            c.execute("INSERT INTO s01_stage.Asset_TopAsset (Portal_ID_F, Asset_ID_F, Top_Asset_ID_F) VALUES (%s,%s,%s);", (row.Portal_ID_F, row.sqlID, row.Top_Asset_ID_F))
            
        db.commit();
        
        return;
    
    def __Insert_Asset_Kursdaten(self, db, df):
        '''
        x
        '''
                
        df['Datum'] = df['Datum'].dt.date  
        
        cursor = db.cursor();
        
        for index, row in df.iterrows():
#            cursor.execute("INSERT INTO s01_stage.Asset_Kursdaten (Portal_ID_F, AssetClass_ID_F, Asset_ID_F, Datum, Market_ID_F, Open, High, Low, Close, Volume) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (row.Portal_ID_F, row.AssetClass_ID_F, row.Asset_ID_F, row.Datum, None, row.Eroeffnung, row.Hoch, row.Tief, row.Schluss, row.Volumen ))
            cursor.execute("INSERT INTO s01_stage.Asset_Kursdaten (Portal_ID_F, AssetClass_ID_F, Asset_ID_F, Datum, Market_ID_F, [Open], High, Low, [Close], Volume) VALUES (?,?,?,?,?,?,?,?,?,?)", (row.Portal_ID_F, row.AssetClass_ID_F, row.Asset_ID_F, row.Datum, None, row.Eroeffnung, row.Hoch, row.Tief, row.Schluss, row.Volumen ))
         
        db.commit();
        
        return;
    
    def __Insert_P1_Asset_Beschaeftigte(self, db, df):
        '''
        x
        '''
        
        cursor = db.cursor();
        
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO s01_stage.P1_Asset_Beschaeftigte (Portal_ID_F, AssetClass_ID_F, Asset_ID_F, Jahr, Beschaeftigt) VALUES (%s,%s,%s,%s,%s)", (row.Portal_ID_F, row.AssetClass_ID_F, row.Asset_ID_F, str(row.Jahr), str(row.Beschaeftigt)))
            
        db.commit();
        
        return;
    
    def __Insert_P1_Asset_Kontakt(self, db, df):
        '''
        x
        '''
        
        cursor = db.cursor();
        
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO s01_stage.P1_Asset_Kontakt (Portal_ID_F, AssetClass_ID_F, Asset_ID_F, Name, Strasse, PLZ, Ort, Telefon, Fax, EMail, URL) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (row.Portal_ID_F, row.AssetClass_ID_F, row.Asset_ID_F, str(row.Name), str(row.Strasse), str(row.PLZ), str(row.Ort), str(row.Telefon), str(row.Fax), str(row.EMail), str(row.Url)))
            
        db.commit();
        
        return;
    
    def __Insert_P1_Asset_Marktdaten(self, db, df):
        '''
        x
        '''
        
        cursor = db.cursor();
        
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO s01_stage.P1_Asset_Marktdaten (Portal_ID_F, AssetClass_ID_F, Asset_ID_F, Kapitalisierung, Anzahl_Aktien, Streubesitz) VALUES (%s,%s,%s,%s,%s,%s)", (row.Portal_ID_F, row.AssetClass_ID_F, row.Asset_ID_F, str(row.Kapitalisierung), int(row.Anzahl_Aktien), row.Streubesitz))
            
        db.commit();
        
        return;
    
    
    def __Insert_P1_Asset_Profil(self, db, df):
        '''
        x
        '''
        
        cursor = db.cursor();
        
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO s01_stage.P1_Asset_Profil (Portal_ID_F, AssetClass_ID_F, Asset_ID_F, Profil) VALUES (%s,%s,%s,%s)", (row.Portal_ID_F, row.AssetClass_ID_F, row.Asset_ID_F, row.Profil))
            
        db.commit();
        
        return;
    
    def __Lookup_P1_Asset_Stammdaten(self, db, df):
        '''
        x
        '''
        cursor = db.cursor()
        
        for index, row in df.iterrows():
            
            v_query = "select ID \
                FROM s01_stage.P1_Asset_Stammdaten \
                WHERE Portal_ID_F = " + str(int(row.Portal_ID_F)) + " and AssetClass_ID_F = " + str(int(row.AssetClass_ID_F)) + " and Asset_ID_F = " + str(int(row.Asset_ID_F)) + ";"
            
            cursor.execute(v_query);
            
            r = cursor.fetchone();
            
            DS_ID = r[0];
             
        return DS_ID;
    
    def __Insert_P1_Asset_Stammdaten(self, db, df):
        '''
        x
        '''
        
        cursor = db.cursor();
        
        for index, row in df.iterrows():
            if row.Familie == 'None':
                cursor.execute("INSERT INTO s01_stage.P1_Asset_Stammdaten (Portal_ID_F, AssetClass_ID_F, Asset_ID_F, WKN, ISIN, Symbol, Land_ID_F, Branche_ID_F, Sektor_ID_F, Typ_ID_F, Nennwert, Unternehmen, Risiko, Chance, Familie) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (row.Portal_ID_F, row.AssetClass_ID_F, row.Asset_ID_F, str(row.WKN), str(row.ISIN), str(row.Symbol), str(row.Land_ID_F), str(row.Branche_ID_F), str(row.Sektor_ID_F), str(row.Typ_ID_F), str(row.Nennwert), str(row.Unternehmen), row.Risiko, row.Chance, None))
            elif row.Familie != 'None':
                cursor.execute("INSERT INTO s01_stage.P1_Asset_Stammdaten (Portal_ID_F, AssetClass_ID_F, Asset_ID_F, WKN, ISIN, Symbol, Land_ID_F, Branche_ID_F, Sektor_ID_F, Typ_ID_F, Nennwert, Unternehmen, Risiko, Chance) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (row.Portal_ID_F, row.AssetClass_ID_F, row.Asset_ID_F, str(row.WKN), str(row.ISIN), str(row.Symbol), str(row.Land_ID_F), str(row.Branche_ID_F), str(row.Sektor_ID_F), str(row.Typ_ID_F), str(row.Nennwert), str(row.Unternehmen), row.Risiko, row.Chance,row.Familie))

        db.commit();
        
        return;
    
    def __Update_P1_Asset_Stammdaten(self, db, df, v_ID):
        '''
        x
        '''
        
        cursor = db.cursor();
        print (df.head(5))        
        for index, row in df.iterrows():

            if row.AssetClass_ID_F == 2:
                print ('2')
                
                cursor.execute("UPDATE s01_stage.P1_Asset_Stammdaten SET WKN=?, ISIN=?, Symbol=?, Land_ID_F=?, Branche_ID_F=?, Sektor_ID_F=?, Typ_ID_F=?, Nennwert=?, Unternehmen=?, Risiko=?, Chance=?, Familie=? WHERE ID = ?;", (str(row.WKN), str(row.ISIN), str(row.Symbol), str(row.Land_ID_F), str(row.Branche_ID_F), str(row.Sektor_ID_F), str(row.Typ_ID_F), str(row.Nennwert), str(row.Unternehmen), row.Risiko, row.Chance, None, str(v_ID)))
            elif row.AssetClass_ID_F == 1: # != 'None':
                print('1')
                cursor.execute("UPDATE s01_stage.P1_Asset_Stammdaten SET WKN=?, ISIN=?, Symbol=?, Land_ID_F=?, Branche_ID_F=?, Sektor_ID_F=?, Typ_ID_F=?, Nennwert=?, Unternehmen=?, Risiko=?, Chance=?, Familie=? WHERE ID = ?;", (str(row.WKN), str(row.ISIN), str(row.Symbol), str(row.Land_ID_F), None, None, str(row.Typ_ID_F), None, None, None, None, str(row.Familie), str(v_ID)))

        db.commit();
        
        return;
    
    def __Insert_P1_Asset_Termine(self, db, df):
        '''
        x
        '''
        
        cursor = db.cursor();
        
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO s01_stage.P1_Asset_Termine (Portal_ID_F, AssetClass_ID_F, Asset_ID_F, Datum, Anlass) VALUES (%s,%s,%s,%s,%s)", (row.Portal_ID_F, row.AssetClass_ID_F, row.Asset_ID_F, row.Datum, row.Anlass))
            
        db.commit();
        
        return;