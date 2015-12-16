#!/usr/local/bin/python
# coding: utf-8

# Lead creation

__author__ = "Illya Vinnichenko"
__copyright__ = "Copyright 2014"
__credits__ = ["Illya Vinnichenko"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Illya Vinnichenko"
__email__ = "ivinnichenko@lattice-engines.com"
__status__ = "Alpha"

# import modules
import requests
import json
import time
import base64
import random
import string

from Properties import PLSEnvironments
from operations.TestRunner import SessionRunner

def getMetaData(table_name,leads_number=3,conn=PLSEnvironments.SQL_BasicDataForIntegrationTest):
    dlc = SessionRunner()
    connection_string = conn;
    query = "SELECT TOP %s * FROM %s order by newid() " % (leads_number,table_name);      
    return dlc.getQuery(connection_string, query);
def recordNewAdded(sequence,marketting_app,sobjects, lead_id,email):
    dlc = SessionRunner()
    connection_string = PLSEnvironments.SQL_BasicDataForIntegrationTest;    
    query = "INSERT INTO [Results]([sequence],[maps],[s_objects],[id],[email]) VALUES(%d,'%s','%s','%s','%s')" % (sequence,marketting_app,sobjects,lead_id,email);        
    return dlc.execQuery(connection_string, query);
def recordResultSet(id,email,operation,modified_date,scored,result):
    dlc = SessionRunner()
    connection_string = PLSEnvironments.SQL_BasicDataForIntegrationTest;    
    query = "update results set operation='%s', executed_date=getdate(),modifiedDate='%s',scored=%s,result=%d where id='%s' and email='%s'" % (operation,modified_date[0:19],scored,result,id,email);

    return dlc.execQuery(connection_string, query);
def getSequence():
    dlc = SessionRunner()
    connection_string = PLSEnvironments.SQL_BasicDataForIntegrationTest;
    query = "select max(sequence) from results";      
    result = dlc.getQuery(connection_string, query);
    if None == result[0][0]:
        return 0;
    else:
        return result[0][0]+1;
    
def random_str(randomlength=8):
    fixed_str = "%s_%s" % (string.ascii_letters, string.digits);
    a = list(fixed_str);
    random.shuffle(a)
    return ''.join(a[:randomlength])
def getRandomMail(domain=None):    
    randomlength=random.randint(0,60 - len(domain));
    emailName = random_str(randomlength);  
    return "%s@%s" % (emailName,domain); 
def getDomains(marketting_app,leads_number=3,conn=PLSEnvironments.SQL_BasicDataForIntegrationTest):
    if marketting_app == PLSEnvironments.pls_marketing_app_ELQ:
        return getMetaData("domain_Eloqua",leads_number,conn);
    else:
        return getMetaData("domain_Marketo",leads_number,conn);
def getAddresses(leads_number=3,conn=PLSEnvironments.SQL_BasicDataForIntegrationTest):
    return getMetaData("meta_address", leads_number, conn);
def getStageNames(leads_number=3,conn=PLSEnvironments.SQL_BasicDataForIntegrationTest):
    return getMetaData("meta_stagename", leads_number, conn);
def getTitles(leads_number=3,conn=PLSEnvironments.SQL_BasicDataForIntegrationTest):
    return getMetaData("meta_title", leads_number, conn);
def getActivityTypes(leads_number=3,conn=PLSEnvironments.SQL_BasicDataForIntegrationTest):
    return getMetaData("meta_activityType", leads_number, conn);

def verifyResult(operation,records):
    results = []
    for r in records[1:]:
        print r;
        passed = True;
        if r["latticeforleads__Last_Score_Date__c"]==None:
            r["latticeforleads__Last_Score_Date__c"]="1900-01-01";
            passed=False;
        if r["latticeforleads__Score__c"] == None:
            r["latticeforleads__Score__c"]=0;
            passed=False;
        if float(r["latticeforleads__Score__c"]) - 0 < 0 or float(r["latticeforleads__Score__c"]) - 100 > 0:
            passed=False; 
        if not passed:
            results.append(r);

        recordResultSet(r["id"], r["email"], operation, r["latticeforleads__Last_Score_Date__c"], r["latticeforleads__Score__c"], passed);           
        
    return results;
        
class EloquaRequest():
    
    def __init__(self,base_url=PLSEnvironments.pls_ELQ_url, company=PLSEnvironments.pls_ELQ_company, user=PLSEnvironments.pls_ELQ_user, password=PLSEnvironments.pls_ELQ_pwd):
        authKey = base64.b64encode(company + "\\" + user + ":" + password)
        self.headers = {"Content-Type":"application/json", "Authorization":"Basic " + authKey};
        self.base_url=base_url;

    def get(self, url):
        request_url = self.base_url + url
#         print request_url
        request = requests.get(request_url, headers=self.headers)
        return request
    
    def put(self, url, data):
        request_url = self.base_url + url
#         print request_url
        request = requests.put(request_url, headers=self.headers, data=json.dumps(data))
        return request

    def post(self, url, data):
        request_url = self.base_url + url
#         print request_url
        request = requests.post(request_url, headers=self.headers, data=json.dumps(data))
        return request

    def delete(self, url):
        request_url = self.base_url + url
#         print request_url
        request = requests.delete(request_url, headers=self.headers)
        return request

    def createNewContact(self, email, first_name, last_name,country=None):
        
        if country == None:
            new_contact = {"emailAddress": email,
                           "firstName": first_name,
                           "lastName": last_name,
                           }
        else:
            new_contact = {"emailAddress": email,
                           "firstName": first_name,
                           "lastName": last_name,
                           "country": country,
                           }
        response = self.post("/data/contact", new_contact)
        return response;

    def updateContact(self, contact_id, email, first_name, last_name):
        new_contact = {"emailAddress": email,
                       "firstName": first_name,
                       "lastName": last_name,
                       }
        print new_contact
        response = self.put("/data/contact/%s" % contact_id, new_contact)
        if response:
            print response.status_code
            print response.text
        return response

    def getContact(self, contact_id):
        response = self.get("/data/contact/%s" % contact_id);
        return response

    def deleteContact(self, contact_id):
        response = self.delete("/data/contact/%s" % contact_id)
        if response.status_code == 200:
            return True;
        else:
            return response;

    def addEloquaContact(self,leads_number=3,country=None):
        domains = getDomains(PLSEnvironments.pls_marketing_app_ELQ, leads_number);
        contact_lists={};
        failed = 0;
        sequence = getSequence();
        
        for domain in domains:
            email = getRandomMail(domain[0]);
            response = self.createNewContact(email, 
                                                random_str(), 
                                                random_str(),
                                                country)
            if response:
                if response.status_code == 201:
                    created_id=json.loads(response.text)["id"];
                    print "==>    " + created_id + "    " + email;
                    contact_lists[created_id] = email;
                    recordNewAdded(sequence, PLSEnvironments.pls_marketing_app_ELQ,"Contact", created_id, email);
                if response.status_code == 409:
                    print "Contact already exists - pick another one:    " + email;  
            else:
                failed += 1;
                if failed>3:
                    break;
        return contact_lists    
    
    def addEloquaContactForDante(self,leads_number=3):
        domains = getDomains(PLSEnvironments.pls_marketing_app_ELQ, leads_number);
        
        contact_lists={};
        sfdc = SFDCRequest();
        sfdc_contacts={};
        sfdc_leads={};
        
        failed = 0;
        seq = getSequence();
        
        for domain in domains:
            emailAddress = getRandomMail(domain[0]);
            response = self.createNewContact(emailAddress, 
                                                random_str(), 
                                                random_str())
            if response:
                if response.status_code == 201:
                    created_id=json.loads(response.text)["id"];
                    print "==>    " + created_id + "    " + emailAddress;
                    contact_lists[created_id] = emailAddress;
                    recordNewAdded(seq, PLSEnvironments.pls_marketing_app_ELQ,"Contact", created_id, emailAddress);
                    contact_id=sfdc.addContactToSFDC(email=emailAddress);
                    if(contact_id!=None):
                        sfdc_contacts[emailAddress]=contact_id;
                        recordNewAdded(seq, PLSEnvironments.pls_marketing_app_SFDC,"Contact", contact_id, emailAddress);
                    lead_id=sfdc.addLeadToSFDC(email=emailAddress);
                    if(lead_id!=None):
                        sfdc_leads[emailAddress]=lead_id;
                        recordNewAdded(seq, PLSEnvironments.pls_marketing_app_SFDC,"Lead", lead_id, emailAddress);    
                if response.status_code == 409:
                    print "Contact already exists - pick another one:    " + emailAddress;  
            else:
                failed += 1;
                if failed>3:
                    break;
        return [contact_lists,sfdc_contacts,sfdc_leads];
    
    def addAnonymousContact(self):
        email = getRandomMail("unknownvisitor.elq")
        response = self.createNewContact(email, 
                                                random_str(), 
                                                random_str())
        if response:
                if response.status_code == 201:
                    created_id=json.loads(response.text)["id"];
                    print "==>    " + created_id + "    " + email;
                if response.status_code == 409:
                    print "Contact already exists - pick another one:    " + email;  
        else:
            print "we can't create anonymous contact for you."
        
    
    def getEloquaContact(self,contact_ids={}):
        contacts = []
        for k in contact_ids.keys():
            response = self.getContact(k);
            if len(response.text)>0:
                result = json.loads(response.text);
                print result
                results = {};
                results["id"]=k;
                results["email"]=result["emailAddress"];

                results["latticeforleads__Score__c"],results["latticeforleads__Last_Score_Date__c"] = self.getScore(result["fieldValues"])
                print "==>    %s    %s    %s    %s" % (k, results["email"], results["latticeforleads__Score__c"],results["latticeforleads__Last_Score_Date__c"])
                contacts.append(results);
            
        return contacts;

    def getScore(self, fields=[{}]):
        score=None;
        score_date=None;
        for field in fields:
            if field["id"]=="100207":
                score=field["value"]
                continue
            elif field["id"]=="100208":
                print field["value"]
                score_date=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(float(field["value"]))) ;
                continue
            if score_date != None and score != None:
                break;
        return score,score_date;

class MarketoRequest():
    def __init__(self, base_url=PLSEnvironments.pls_MKTO_Client_url, client_id=PLSEnvironments.pls_MKTO_Client_id,
                 client_secret=PLSEnvironments.pls_MKTO_client_secret):
        self.headers = {'Content-type': 'application/json', 'Accept': 'application/json'};
        self.base_url=base_url;
        self.clinet_id=client_id;
        self.client_secret=client_secret;
    
    def createOrUpdate(self,email,firstName,country=None):
        # leads_list must be a List of Dict
        """
        Like this:
        marketo_leads = [  
          {  
             "email":"kjashaedd-1@klooblept.com",
             "firstName":"Kataldar-1",
             "postalCode":"04828"
             "City": "new york"
             "Country":"usa"
          },
          {  
             "email":"kjashaedd-2@klooblept.com",
             "firstName":"Kataldar-2",
             "postalCode":"04828"
          },
          {  
             "email":"kjashaedd-3@klooblept.com",
             "firstName":"Kataldar-3",
             "postalCode":"04828"
          }
        ]
        """
        if country==None:
            leads_list=[{"email":email,
                 "firstName":firstName,
                 "postalCode":"04828"},]
        else:
            leads_list=[{"email":email,
             "firstName":firstName,
             "postalCode":"04828",
             "Country":country},]
#         print leads_list
        marketo_dict = {"action":"createOrUpdate",
                        "input": leads_list}
        
        endpoint = "/rest/v1/leads.json"
        auth_token =  "?access_token=%s" % self.getMarketoAccessToken();
        request_url = "%s%s%s" % (self.base_url, endpoint, auth_token)
#         print request_url
        response = requests.post(request_url, headers=self.headers, data=json.dumps(marketo_dict));
        return response;
     
    def getLead(self, lead_id):        
        endpoint = "/rest/v1/lead/%s.json" % lead_id;
        auth_token =  "?access_token=%s" % self.getMarketoAccessToken();
        fields = "&fields=email,latticeforleads__Score__c,latticeforleads__Last_Score_Date__c";        
        request_url = "%s%s%s%s" % (self.base_url, endpoint, auth_token,fields);
        
        response = requests.get(request_url);
        return response;
    
    def delete(self,lead_id):
        
        leads_list=[{"id":lead_id,}]
        marketo_dict = {"input": leads_list}
        
        endpoint = "/rest/v1/leads.json"
        auth_token =  "?access_token=%s" % self.getMarketoAccessToken();
        request_url = "%s%s%s" % (self.base_url, endpoint, auth_token)
#         print request_url
        response = requests.delete(request_url, headers=self.headers, data=json.dumps(marketo_dict));
        if response.status_code == 200:
            return True;
        else:
            return response;
              
    def addLeadToMarketo(self,leads_number=3,country=None):        
        domains = getDomains(PLSEnvironments.pls_marketing_app_MKTO, leads_number);
        lead_lists={};
        failed = 0;
        sequence = getSequence();
        
        for domain in domains:
            email = getRandomMail(domain[0]);
            response = self.createOrUpdate(email, random_str(),country);
            if response:
                if response.status_code == 200:
                    created = json.loads(response.text);
                    if created["result"][0]["status"]=="skipped":
                        failed += 1;
                        if failed>3:
                            continue;
                    created_id=created["result"][0]["id"];
                    print "==>    %d    %s" % (created_id, email);
                    lead_lists[created_id] = email;
                    recordNewAdded(sequence, PLSEnvironments.pls_marketing_app_MKTO,"Lead", created_id, email);
                if response.status_code == 409:
                    print "Leads already exists - pick another one:    " + email;  
            else:
                failed += 1;
                if failed>3:
                    continue;
        return lead_lists
    def addLeadToMarketoForDante(self,leads_number=3):        
        domains = getDomains(PLSEnvironments.pls_marketing_app_MKTO, leads_number);
        lead_lists={};
        sfdc = SFDCRequest();
        sfdc_contacts={};
        sfdc_leads={};
        
        failed = 0;
        seq = getSequence();
        
        for domain in domains:
            emailAddress = getRandomMail(domain[0]);
            response = self.createOrUpdate(emailAddress, random_str());
            if response:
                if response.status_code == 200:
                    print response.text;
                    created_id=json.loads(response.text)["result"][0]["id"];
                    print "==>    %d    %s" % (created_id, emailAddress);
                    lead_lists[created_id] = emailAddress;
                    recordNewAdded(seq, PLSEnvironments.pls_marketing_app_MKTO,"Lead", created_id, emailAddress);
                    
                    contact_id=sfdc.addContactToSFDC(email=emailAddress);
                    if(contact_id!=None):
                        sfdc_contacts[emailAddress]=contact_id;
                        recordNewAdded(seq, PLSEnvironments.pls_marketing_app_SFDC,"Contact", contact_id, emailAddress);
                    lead_id=sfdc.addLeadToSFDC(email=emailAddress);
                    if(lead_id!=None):
                        sfdc_leads[emailAddress]=lead_id;
                        recordNewAdded(seq, PLSEnvironments.pls_marketing_app_SFDC,"Lead", lead_id, emailAddress);    
                        
                if response.status_code == 409:
                    print "Leads already exists - pick another one:    " + emailAddress;  
            else:
                failed += 1;
                print response
                if failed>3:
                    break;
        return [lead_lists,sfdc_contacts,sfdc_leads]
    
    def getLeadFromMarketo(self,lead_ids={}):
        leads = []
        for k in lead_ids.keys():
            response=self.getLead(k);
            if response.status_code == 200:
                results = json.loads(response.text)["result"];
                print results
                if len(results)>0:
                    result = results[0];
                    print "==>    %s    %s    %d    %s" % (k, result["email"], result["latticeforleads__Score__c"], result["latticeforleads__Last_Score_Date__c"]);
                    leads.append(result);
            else:
                print response.text
            
        return leads;

    def getMarketoAccessToken(self):
        marketo_instance = "%s/identity/oauth/token?grant_type=client_credentials" % self.base_url;
        request_url = marketo_instance + "&client_id=" + self.clinet_id + "&client_secret=" + self.client_secret;
        
        #Make request
        try:
            response = requests.get(request_url);
        except:
            time.sleep(5);
            response = requests.get(request_url);
        
        if response:                
            results = json.loads(response.text);
            access_token = results["access_token"]
#             print access_token;
            return access_token;
        else:
            return None;

class SFDCRequest():
    def __init__(self,base_url=PLSEnvironments.pls_SFDC_url):
        self.headers = {'Authorization': 'Bearer %s' % self.getAuthToken(),'Content-type': 'application/json', 'Accept': 'application/json'};
        self.base_url=base_url;
    
    def createRecord(self,sobjects,sbody):
        request_url = "%s/sobjects/%s"  % (self.base_url, sobjects);
        
        response = requests.post(request_url,headers = self.headers,data=json.dumps(sbody));
        print response.text;
        if response:                
            results = json.loads(response.text);
            if True == results["success"]:
                return results["id"];
            else:
                return None;   
        else:
            return None;
    def updateRecord(self,sobjects,record_id,sbody):
        request_url = "%s/sobjects/%s/%s"  % (self.base_url, sobjects,record_id);
#         print request_url;
        response = requests.patch(request_url,headers = self.headers,data=json.dumps(sbody));
        print response.text;
        return True;
    def deleteRecord(self,sobjects,record_id):
        request_url = "%s/sobjects/%s/%s"  % (self.base_url, sobjects,record_id);
#         print request_url;
        response = requests.delete(request_url,headers = self.headers);
        print response.text;
        return True;
    def getRecord(self,sobjects,record_id):
        fields = "fields=Email,Latticeforleads__Last_Score_Date__c,Latticeforleads__Score__c"
        request_url = "%s/sobjects/%s/%s?%s"  % (self.base_url, sobjects,record_id,fields);

        response = requests.get(request_url,headers = self.headers);
        return response;
    def getRecords(self,sobjects,record_ids={}):
        records = []
        for k in record_ids.keys():
            response=self.getRecord(sobjects,k);            
            if response.status_code == 200:
                result = json.loads(response.text);
                print result
                print "==>    %s    %s    %s    %s" % (k, result["Email"], result["latticeforleads__Score__c"], result["latticeforleads__Last_Score_Date__c"]);
                result["id"]=result["Id"];
                del result["Id"];
                result["email"]=result["Email"];
                del result["Email"];                 
                records.append(result);
            else:
                print response.text
            
        return records;
    
    def addAccountsToSFDC(self, account_num=3):
        addresses = getAddresses(account_num);        
        
        account_lists={};
        failed = 0;
        sequence = getSequence();
        
        for address in addresses:
            account = {
                   "name" : random_str(16),
                   "BillingCity" : address["c_city"],
                   "BillingState" : address["c_state_Prov"],
                   "BillingCountry" : address["c_country"], 
#                    "Industry" : ,
                   "AnnualRevenue" : random.randint(1000,100000000),
                   "NumberOfEmployees":random.randint(1,3000)
                   }
            account_id = self.createRecord("Account",account);
            if account_id != None:
                account_lists[account_id] = account["name"];
                recordNewAdded(sequence, PLSEnvironments.pls_marketing_app_SFDC,"Account", account_id, account_lists[account_id]); 
            else:
                failed += 1;
                if failed>3:
                    break;
        return account_lists;
    def updateAccountToSFDC(self,record_id, name=None,BillingCity=None,BillingState=None,BillingCountry=None,Industry=None,AnnualRevenue=None,NumberOfEmployee=None):        
        account = {};
        if name != None:
            account["name"]=name;
        if BillingCity != None:
            account["BillingCity"]=BillingCity;
        if BillingState != None:
            account["BillingState"]=BillingCity;
        if BillingCountry != None:
            account["BillingCountry"]=BillingCity;
        if Industry != None:
            account["Industry"]=BillingCity;
        if AnnualRevenue != None:
            account["AnnualRevenue"]=BillingCity;
        if NumberOfEmployee != None:
            account["NumberOfEmployee"]=BillingCity;
        return self.updateRecord("Account",record_id,account);
    def deleteAccount(self,record_id):
        sobjects = "Account";
        return self.deleteRecord(sobjects, record_id);

    def addContactsToSFDC(self,account_id=None, contact_num=3,country=None):
        domains = getDomains(marketting_app=PLSEnvironments.pls_marketing_app_MKTO, leads_number=contact_num);                
        contact_lists={};
        failed = 0;
        seq = getSequence();
        
        for i in range(contact_num):
            emailAddress =  getRandomMail(domains[i][0]);
            contact_id = self.addContactToSFDC(email=emailAddress,mailingCountry=country);
            if contact_id != None:
                contact_lists[contact_id] = emailAddress;
                recordNewAdded(seq, PLSEnvironments.pls_marketing_app_SFDC, "Contact", contact_id, contact_lists[contact_id]); 
            else:
                failed += 1;
                if failed>3:
                    break;
        return contact_lists;
    def addContactToSFDC(self,account_id=None,email=None,mailingCountry=None):
        addresses = getAddresses(1);
        titles = getTitles(leads_number=1);  
        
        contact = {}
        if account_id != None:
            contact["AccountId"] = account_id;
        contact["LastName"] = random_str(6);
        contact["FirstName"] = random_str(6);
#             contact["Name"] = "%s %s" % (contact["FirstName"],contact["LastName"])
        contact["MailingCity"] = addresses[0]["c_city"];
        contact["MailingState"] = addresses[0]["c_state_Prov"];
        if mailingCountry == None:
            contact["MailingCountry"] = addresses[0]["c_country"];
        else:
            contact["MailingCountry"]=mailingCountry
#             contact["Phone"] = ;
        contact["Email"]=email;
        contact["Title"]=titles[0][0];
#             contact["LeadSource"] = ; 
        return self.createRecord("Contact",contact);
    
    def getContactsFromSFDC(self,contact_ids={}):
        return  self.getRecords("Contact",contact_ids);
    
    def addLeadsToSFDC(self, lead_num=3, country=None): 
        domains = getDomains(marketting_app=PLSEnvironments.pls_marketing_app_MKTO, leads_number=lead_num); 
        
        lead_lists={};
        failed = 0;
        seq = getSequence();
        
        for i in range(lead_num):
            emailAddress =  getRandomMail(domains[i][0]);
            lead_id = self.addLeadToSFDC(emailAddress,country);
            if lead_id != None:
                lead_lists[lead_id] = emailAddress;
                recordNewAdded(seq, PLSEnvironments.pls_marketing_app_SFDC, "Lead", lead_id, lead_lists[lead_id]); 
            else:
                failed += 1;
                if failed>3:
                    break;
        return lead_lists;
    def addLeadToSFDC(self, email=None,country=None):
        addresses = getAddresses(1);
        titles = getTitles(leads_number=1);  
        
        lead = {}
        lead["LastName"] = random_str(6);
        lead["FirstName"] = random_str(6);
        lead["City"] = addresses[0]["c_city"];
        lead["State"] = addresses[0]["c_state_Prov"];
        if country == None:
            lead["Country"] = addresses[0]["c_country"];
        else:
            lead["Country"]=country;
#             lead["Phone"] = ;
        lead["Email"]=email;
        lead["Title"]=titles[0][0];
        lead["AnnualRevenue"]=random.randint(1000,100000000);
        lead["NumberOfEmployees"]=random.randint(1,3000);
        mail=email[string.find(email,'@')+1:];
        lead["Company"]=mail[0:string.find(mail,'.')];
#         print lead
        return self.createRecord("Lead",lead);
        
    def getLeadsFromSFDC(self,lead_ids={}):
        return  self.getRecords("Lead",lead_ids);
    
    def addOpportunityToSFDC(self,account_id=None, opportunity_num=3):        
        opportunity_lists={};
        failed = 0;
        sequence = getSequence();
        
        for i in range(opportunity_num):
            opportunity = {}
            if account_id != None:
                opportunity["AccountId"] = account_id;
            opportunity["Name"] = random_str(20);
            opportunity["StageName"] = random_str(6);
            opportunity["Amount"]=random.randint(1000,100000000);
            opportunity["Probability"]=random.randint(0,100);
            opportunity["TotalOpportunityQuantity"]=random.randint(0,1000);
            nowTime = time.localtime();
            opportunity["CloseDate"]="%d-%d-%d" % (nowTime[0],(nowTime[1]+1)%12,nowTime[2]);
            opportunity["Type"]="New Business";
#     "","RecordTypeId","","","" = ,"" = ,"" = ,"",
# "","LeadSource",""
            opportunity_id = self.createRecord("Opportunity",opportunity);
            if opportunity_id != None:
                opportunity_lists[opportunity_id] = opportunity["Name"];
                recordNewAdded(sequence, PLSEnvironments.pls_marketing_app_SFDC, "Opportunity", opportunity_id, opportunity_lists[opportunity_id]); 
            else:
                failed += 1;
                if failed>3:
                    break;
        return opportunity_lists;
    
    def addOpportunityConTactRoleToSFDC(self,opportunity_id,contact_id, role_num=3):        
        role_lists={};
        failed = 0;
        sequence = getSequence();
        
        for i in range(role_num):
            role = {}
            role["OpportunityId"] = opportunity_id;
            role["ContactId"] = contact_id;
            role["Role"]="Business User";
            role_id = self.createRecord("OpportunityContactRole",role);
            if role_id != None:
                role_lists[role_id] = role["Role"];
                recordNewAdded(sequence, PLSEnvironments.pls_marketing_app_SFDC, "OpportunityContactRole", role_id, role_lists[role_id]); 
            else:
                failed += 1;
                if failed>3:
                    break;
        return role_lists;
        
    def getAuthToken(self,OAuth2_url=PLSEnvironments.pls_SFDC_OAuth2,
                     user=PLSEnvironments.pls_SFDC_OAuth2_user,
                     pwd=PLSEnvironments.pls_SFDC_OAuth2_pwd,
                     client_id=PLSEnvironments.pls_SFDC_Client_id, 
                     client_secret=PLSEnvironments.pls_SFDC_client_secret):  
        
        request_url = "%s?grant_type=password&client_id=%s&client_secret=%s&username=%s&password=%s" % (OAuth2_url,client_id,client_secret,user,pwd);
                
        try:
            response = requests.post(request_url);
        except:
            time.sleep(5);
            response = requests.post(request_url);
          
        if response:                
            results = json.loads(response.text);
            access_token = results["access_token"]
#             print access_token;
            return access_token;
        else:
            return None;
