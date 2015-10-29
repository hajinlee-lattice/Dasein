__author__ = 'nxu'

import pyodbc
import json
from Property import DanteEnvironments
from XMLLibTool import XmlToolLibrary

class DanteDataHelper(object):
    def __init__(self,server=DanteEnvironments.Dante_DB_Server_Server,DataBase=DanteEnvironments.Dante_DB_Name,User=DanteEnvironments.Dante_DB_User,Password=DanteEnvironments.Dante_DB_PWD):
        self.conn_info="DRIVER={SQL Server};Server="+server+";DATABASE="+DataBase+";UID="+User+";PWD="+Password

    def ParseLeadScore(self,Lead_ID):
        print '===start to parse Lead score From DB==='
        Dic_Lead={}
        sql_Lead_Json="Select [Value] From LeadCache Where [Account_External_ID] is null and [Salesforce_ID]='"+Lead_ID+"';"
        Dic_Lead["LeadID"]=Lead_ID
        try:
            conn=pyodbc.connect(self.conn_info)
            cursor=conn.cursor()
            Json_Value=cursor.execute(sql_Lead_Json).fetchall()
            J_dic=json.loads(Json_Value[0][0])
            Dic_Lead["Title"]=J_dic["PlayDisplayName"]
            Dic_Lead["Score"]=str(J_dic["Percentile"])+'\nScore'
            Dic_Lead["Rate"]=J_dic["LikelihoodBucketDisplayName"]
        except Exception,e:
            print "connect or query DB Fail"
            print e.message
        finally:
            cursor.close()
            conn.close()
        print '===end to parse Lead Score and return Lead dict==='
        return Dic_Lead

    def ParseAccountsPlay(self,AccountID):
        print '===start to parse accounts plays From DB==='
        Dic_Account={}
        sql_Account_Json="Select [Value] From AccountCache Where [External_ID]='"+AccountID+"';"
        sql_AccountPlays_Json="Select [Value] From LeadCache Where [Account_External_ID]='"+AccountID+"';"
        #print self.conn_info
        conn=pyodbc.connect(self.conn_info)
        cursor=conn.cursor()
        try:
            #conn=pyodbc.connect(self.conn_info)
            #cursor=conn.cursor()
            Account_json=cursor.execute(sql_Account_Json).fetchall()
            AJ_dic=json.loads(Account_json[0][0])
            Dic_Account["DisplayName"]=AJ_dic["DisplayName"]
            Account_Plays_Json=cursor.execute(sql_AccountPlays_Json).fetchall()
            Plays_list=[]
            for one_play in Account_Plays_Json:
                #print "Play start"
                play_dic={}
                J_dic=json.loads(one_play[0])
                play_dic["P_Title"]=J_dic["PlayDisplayName"]
                play_dic["P_Score"]=str(J_dic["ExternalProbability"])+'\n'+str(J_dic["LikelihoodBucketDisplayName"])
                #play_dic["P_ID"]=J_dic["PlayID"]
                sql_talkingpoints="select [Value] from TalkingPointCache where Play_External_ID='"+J_dic["PlayID"]+"';"
                TalkingPoints_json=cursor.execute(sql_talkingpoints).fetchall()
                TalkPoints_list=[]
                for one_TalkPoint in TalkingPoints_json:
                    #print "TalkPoint start"
                    TalkPoint_dic={}
                    TJ_dic=json.loads(one_TalkPoint[0])
                    TalkPoint_dic["Title"]=str(TJ_dic["Title"]).upper()
                    TalkPoints_list.append(TalkPoint_dic)
                play_dic["TalkingPoints"]=TalkPoints_list
                Plays_list.append(play_dic)
            Dic_Account["Plays"]=Plays_list
        except Exception,e:
            print "connect or query DB Fail"
            print e.message
        finally:
            cursor.close()
            conn.close()
        print '===end to parse account plays and return plays dict list==='
        return Dic_Account

    def CheckPlayTitleAndScore(self,Play_List_DB,PTitle_Page,PScore_Page):
        result=False
        for dic_item in Play_List_DB:
            if (dic_item["P_Title"]==PTitle_Page) and (dic_item["P_Score"]==PScore_Page):
                result=True
                break
        return result

    def GetTPDicListByPlayTitle(self,Dic_Play_List,PlayTitle):
        result=[]
        #print '===start to get expected talkingpoints dictionary for play: (' + str(PlayTitle)+' )==='
        for dic_item in Dic_Play_List:
            if str(dic_item["P_Title"])==PlayTitle:
                result=dic_item["TalkingPoints"]
                break
        #print '===complete to get expected talkingpoints dictionary==='
        return result

    def GetTPTitleListFromTps(self,TalkingPointsList):
        #print TalkingPointsList
        Tp_Title_list=[]
        for Tp in TalkingPointsList:
            title=Tp["Title"]
            Tp_Title_list.append(title)
        #print Tp_Title_list
        return sorted(Tp_Title_list)


    def CompareDanteDictionary(self,dict_Page,dict_DB):
        result=False
        if dict_Page.has_key("DisplayName"):
            if dict_Page["DisplayName"]==dict_DB["DisplayName"]:
                if len(dict_Page["Plays"])==len(dict_DB["Plays"]):
                    for play_item in dict_Page["Plays"]:
                        assert self.CheckPlayTitleAndScore(dict_DB["Plays"],play_item["P_Title"],play_item["P_Score"]), 'Play score for the play "'+play_item["P_Title"]+'" in page is not right, actually is: "'+str(play_item["P_Score"])
                        tp_list_page=play_item["TalkingPoints"]
                        tp_list_DB=self.GetTPDicListByPlayTitle(dict_DB["Plays"],play_item["P_Title"])
                        tp_title_list_page=self.GetTPTitleListFromTps(tp_list_page)
                        tp_title_list_DB=self.GetTPTitleListFromTps(tp_list_DB)
                        if (tp_title_list_page==tp_title_list_DB):
                            result=True
                        else:
                            print 'The talking points for play: "'+play_item["P_Title"]+'" in page is not right.'
                            print tp_title_list_page
                            print '-----'
                            print tp_title_list_DB
                            result=False
                            break
                else:
                    print 'The number of plays displayed in page is not right, actually is: '+str(len(dict_Page["Plays"]))+' expected should be: '+str(len(dict_DB["Plays"]))
                    result=False
            else:
                print 'Display Name in page is not right, actually is: '+dict_Page["DisplayName"]+' expected should be: '+dict_DB["DisplayName"]
                result=False
        return result

    def GetPRODExpectedTalkingPoints(self,filepath):
        print 'get expected talking points value from xml file'
        xmldoc=XmlToolLibrary()
        xmldoc.loadXML(filepath)
        tp_attrs_titles=xmldoc.getxmlVals('//PRODDanteUI/TalkingPoints/attrs/attr/title')
        count_attr_tp=len(tp_attrs_titles)
        tp_list=[]
        for index_ex in range(1,int(count_attr_tp)+1):
            attr_dic={}
            title=xmldoc.getxmlVal('//PRODDanteUI/TalkingPoints/attrs/attr['+str(index_ex)+']/title')
            descrition=xmldoc.getxmlVal('//PRODDanteUI/TalkingPoints/attrs/attr['+str(index_ex)+']/description')
            #print descrition
            attr_dic["Title"]=title
            attr_dic["Content"]=descrition
            #print attr_dic
            tp_list.append(attr_dic)
        return sorted(tp_list)

    def GetPRODExpectedBuyingSingal(self,filepath):
        print 'get expected buying signals value from xml file'
        xmldoc=XmlToolLibrary()
        xmldoc.loadXML(filepath)
        Ex_Header=xmldoc.getxmlAtt('//PRODDanteUI/BuyingSignals/External_attrs','title')
        In_Header=xmldoc.getxmlAtt('//PRODDanteUI/BuyingSignals/Internal_attrs','title')
        Ex_attrs_titles=xmldoc.getxmlVals('//PRODDanteUI/BuyingSignals/External_attrs/attr/title')
        In_attrs_titles=xmldoc.getxmlVals('//PRODDanteUI/BuyingSignals/Internal_attrs/attr/title')
        count_attr_Ex=len(Ex_attrs_titles)
        count_attr_In=len(In_attrs_titles)
        ex_attr_list=[]
        for index_ex in range(1,int(count_attr_Ex)+1):
            attr_dic={}
            title=xmldoc.getxmlVal('//PRODDanteUI/BuyingSignals/External_attrs/attr['+str(index_ex)+']/title')
            descrition=xmldoc.getxmlVal('//PRODDanteUI/BuyingSignals/External_attrs/attr['+str(index_ex)+']/description')
            rate=xmldoc.getxmlVal('//PRODDanteUI/BuyingSignals/External_attrs/attr['+str(index_ex)+']/rate')
            attr_dic["Title"]=title
            attr_dic["Description"]=descrition
            attr_dic["Rate"]=rate
            #print attr_dic
            ex_attr_list.append(attr_dic)
        #print sorted(ex_attr_list)
        ex_info={'Title':Ex_Header,'Attrs':sorted(ex_attr_list)}
        in_attr_list=[]
        for index_in in range(1,int(count_attr_In)+1):
            attr_dic={}
            title=xmldoc.getxmlVal('//PRODDanteUI/BuyingSignals/Internal_attrs/attr['+str(index_in)+']/title')
            descrition=xmldoc.getxmlVal('//PRODDanteUI/BuyingSignals/Internal_attrs/attr['+str(index_in)+']/description')
            rate=xmldoc.getxmlVal('//PRODDanteUI/BuyingSignals/Internal_attrs/attr['+str(index_in)+']/rate')
            attr_dic["Title"]=title
            attr_dic["Description"]=descrition
            attr_dic["Rate"]=rate
            #print attr_dic
            in_attr_list.append(attr_dic)
        #print sorted(in_attr_list)
        in_info={'Title':In_Header,'Attrs':sorted(in_attr_list)}
        #print ex_info
        #print in_info
        return ex_info,in_info



#if __name__ == '__main__':
   #ddh=DanteDataHelper()
   #dic=ddh.GetAccountsPlayJson('0018000000NW1EEAA1')
   #print dic

