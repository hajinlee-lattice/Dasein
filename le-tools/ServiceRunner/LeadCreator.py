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
import base64
import json

import requests


class EloquaRequest():
    headers = ''
    # Might need to be Updated
    base_url = 'https://secure.p03.eloqua.com/API/REST/1.0'
    #base_url = 'https://secure.eloqua.com/API/REST/1.0'

    def __init__(self, site, user, password):
        authKey = base64.b64encode(site + "\\" + user + ":" + password)
        self.headers = {"Content-Type":"application/json", "Authorization":"Basic " + authKey}

    def get(self, url):
        request_url = self.base_url + url
        print request_url
        request = requests.get(request_url, headers=self.headers)
        return request
    
    def put(self, url, data):
        request_url = self.base_url + url
        print request_url
        request = requests.put(request_url, headers=self.headers, data=json.dumps(data))
        return request

    def post(self, url, data):
        request_url = self.base_url + url
        print request_url
        request = requests.post(request_url, headers=self.headers, data=json.dumps(data))
        return request

    def delete(self, url):
        request_url = self.base_url + url
        print request_url
        request = requests.delete(request_url, headers=self.headers)
        return request

    def createNewContact(self, email, first_name, last_name):
        new_contact = {"emailAddress": email,
                       "firstName": first_name,
                       "lastName": last_name,
                       }
        print new_contact
        response = self.post("/data/contact", new_contact)
        if response:
            print response.status_code
            print response.text
        return response

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
        response = self.get("/data/contact/%s" % contact_id)
        if response:
            print response.status_code
            print response.text
        return response

    def deleteContact(self, contact_id):
        response = self.delete("/data/contact/%s" % contact_id)
        if response:
            print response.status_code
            print response.text
        return response

def addEloquaContact(email, first_name, last_name):
    request = EloquaRequest('TechnologyPartnerLatticeEngines', 'Matt.Sable', 'Lattice1')
    print request.headers
    response = request.createNewContact("sample@test.com", 
                                        "LatticeTest", 
                                        "TestingAtLattice")
    if response:
        if response.status_code == 201:
            print "Successfully created!"
        if response.status_code == 409:
            print "Contact already exists - pick another one"  
    return response

"""
    marketo_leads = [  
      {  
         "email":"kjashaedd-1@klooblept.com",
         "firstName":"Kataldar-1",
         "postalCode":"04828"
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

def addLeadsToMarketo(leads_list):
    # leads_list must be a List of Dict
    """
    Like this:
    marketo_leads = [  
      {  
         "email":"kjashaedd-1@klooblept.com",
         "firstName":"Kataldar-1",
         "postalCode":"04828"
      },
    ]
    """
    marketo_dict = {"action":"createOrUpdate",
                    "input": leads_list}
    print marketo_dict
    ########################################
    # This piece in general should not change
    marketo_url = "https://976-KKC-431.mktorest.com"
    marketo_token = "0e13d253-97fe-4563-abc1-abfacbfc3e31:sj" 
    endpoint = "/rest/v1/leads.json"
    #####################################
    auth_token =  "?access_token=%s" % marketo_token
    request_url = "%s%s%s" % (marketo_url, endpoint, auth_token)
    print request_url
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    
    response = requests.post(request_url, headers=headers, data=json.dumps(marketo_dict))
    if response:
        print response.text
        if response.text.find('success":true') != -1:
            print "SUCCESS: The lead was created/updated successfully!"
    return response

def main():
    print "Welcome to PLS End to End Automation!"
    """
    addEloquaContact("sample@test.com", "LatticeTest", "TestingAtLattice")
    """
    marketo_leads = [  
      {  
         "email":"kjashaedd-4@klooblept.com",
         "firstName":"Kataldar-4",
         "postalCode":"04828"
      },
    ]

    addLeadsToMarketo(marketo_leads)
    

if __name__ == '__main__':
    main()