import pyodbc
import pandas as pd
import numpy as np
import datetime

class Data:
    def __init__(self, server, database, user, password, tablename) -> None:
        self.server = server
        self.database = database
        self.user = user
        self.password = password
        self.table_name = tablename
        self.cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+ password) 
        self.cursor = self.cnxn.cursor()
        self.Narrow_df = pd.DataFrame()
        self.Wide_df = pd.DataFrame()
        self.Tag_List = pd.DataFrame()
        self.cache = dict()
    def GetTagList(self):
        if self.taglist.empty == True:
            self.taglist = pd.read_sql(f"Select distinct ModelID From [dbo].[{self.table_name}] order by ModelID",self.cnxn)
        return self.taglist



    #NarrowHistory function
    def GetNarrowHistory(self, start,end, tags):
        key = hash(start + end+ str(tags))
        df = self.cache.get(key)
        if df is None:
            if tags != '*':
                tags = ("','".join(tags))
                sql = f"""Select ModelID, Cast(T as DateTime) as DateTime, V
                                        From [dbo].[{self.table_name}] Where 
                                        Cast(T as DateTime) >='{start}'
                                        and Cast(T as DateTime) <='{end}' and ModelID in ('{tags}')"""
            else:
                sql = f"""Select ModelID, Cast(T as DateTime) as DateTime, V
                                    From [dbo].[{self.table_name}] Where 
                                    Cast(T as DateTime) >='{start}'
                                    and Cast(T as DateTime) <='{end}'"""




            self.Narrow_df = pd.read_sql(sql,self.cnxn)
            self.cache.update({key:self.Narrow_df})
        else:

            self.Narrow_df = df
        return self.Narrow_df

    #WideHistory
    def GetWideHistory(self,start,end,resolution,tags):
        narrow = self.GetNarrowHistory(start,end,tags)
        self.Wide_df = pd.pivot_table(narrow,values='V', index=['DateTime'],columns=['ModelID'],aggfunc=np.sum)
        for column in self.Wide_df.head(50):
            try:
                self.Wide_df[column] = pd.to_numeric(self.Wide_df[column])
            except:
                pass
        if resolution == 0:
            return self.Wide_df
        else:
            df = self.Wide_df.resample(f'{resolution}min').ffill()
            self.Wide_df = df
            return self.Wide_df
           

