import copy
import csv
import os
import re

from enum import Enum
from random import randrange

ACCOUNT_FILE = 'Account_All.csv'
CONTACT_FILE = 'Contact_All.csv'
ID = 'Id'
ACCOUNT_ID = 'AccountId'
CONTACT_ID = 'ContactId'
CUSTOMER_ACCOUNT_ID = 'CustomerAccountId'
CUSTOMER_CONTACT_ID = 'CustomerContactId'
REF_CUSTOMER_ACCOUNT_ID = 'CustomerAccountId_Ref'
REF_CUSTOMER_CONTACT_ID = 'CustomerContactId_Ref'
# Contact Existed Fields
FIRST_NAME = 'First_Name'
LAST_NAME = 'Last_Name'
EMAIL = 'Email'
POSTAL_CODE = 'Postal_Code'
# Contact Added Fields
PHONE_NUMBER = 'Phone_Number'
WEBSITE = 'Website'
COMPANY_NAME = 'Company_Name'
COUNTRY = 'Country'
STATE = 'State'
CITY = 'City'
TEST_SCENARIO = 'Test_Scenario'
# Account Fields (Different with Contact)
ACC_COMPANY_NAME = 'Company Name'
ACC_POSTAL_CODE = 'Postal Code'

# Don't cover name + location only case due to remote DnB is desired to be avoided in end2end test
class ContactToAccount(Enum):
    aid = 0
    aid_email = 1
    aid_email_website = 2
    aid_nl = 3
    aid_new = 4
    email = 5
    email_website = 6
    email_nl = 7
    email_new = 8
    webiste = 9
    website_nl = 10
    website_new = 11


def read_account_base(file):
    accounts = []
    schema = []
    aid_to_account = {}
    with open(file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        schema = reader.fieldnames
        for row in reader:
            standardize_account(row)
            aid_to_account[row[CUSTOMER_ACCOUNT_ID]] = row
            accounts.append(row)
    schema = standardize_account_schema(schema)
    return aid_to_account, accounts, schema


def read_contact_base(file):
    contacts = []
    schema = []
    with open(file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        schema = reader.fieldnames
        for row in reader:
            standardize_contact(row)
            contacts.append(row)
    schema = standardize_contact_schema(schema)
    return contacts, schema


def standardize_account(account):
    if ID in account:
        account[CUSTOMER_ACCOUNT_ID] = account[ID]
        account.pop(ID, None)
    account[REF_CUSTOMER_ACCOUNT_ID] = account[CUSTOMER_ACCOUNT_ID]
    account[TEST_SCENARIO] = ''


def standardize_account_schema(schema):
    schema = [CUSTOMER_ACCOUNT_ID if s == ID else s for s in schema]
    schema += [REF_CUSTOMER_ACCOUNT_ID, TEST_SCENARIO]
    return schema


def standardize_contact(contact):
    if ACCOUNT_ID in contact:
        contact[CUSTOMER_ACCOUNT_ID] = contact[ACCOUNT_ID]
        contact.pop(ACCOUNT_ID, None)
    if CONTACT_ID in contact:
        contact[CUSTOMER_CONTACT_ID] = contact[CONTACT_ID]
        contact.pop(CONTACT_ID, None)
    contact[REF_CUSTOMER_ACCOUNT_ID] = contact[CUSTOMER_ACCOUNT_ID]
    contact[REF_CUSTOMER_CONTACT_ID] = contact[CUSTOMER_CONTACT_ID]
    contact[PHONE_NUMBER] = "({}{}{}) {}{}{}-{}{}{}{}".format(*[randrange(10) for i in range(10)])
    contact[WEBSITE] = None
    contact[COMPANY_NAME] = None
    contact[COUNTRY] = None
    contact[STATE] = None
    contact[CITY] = None
    contact[TEST_SCENARIO] = ''


def standardize_contact_schema(schema):
    schema = [CUSTOMER_ACCOUNT_ID if s == ACCOUNT_ID else CUSTOMER_CONTACT_ID if s == CONTACT_ID else s for s in schema]
    schema += [PHONE_NUMBER, WEBSITE, COMPANY_NAME, COUNTRY, STATE, CITY, REF_CUSTOMER_CONTACT_ID, REF_CUSTOMER_ACCOUNT_ID, TEST_SCENARIO]
    return schema


def split(list, begin_idx, end_idx):
    return copy.deepcopy(list[begin_idx:end_idx])


def update_account(accounts):
    for account in accounts:
        account[CUSTOMER_ACCOUNT_ID] = None
        update_scenario(account, 'No AID;')


def update_account_in_contact(contacts, aid_to_account):
    for k in range(len(contacts)):
        contact = contacts[k]
        i = k % len(list(ContactToAccount))
        acct_website = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][WEBSITE]
        acct_company_name = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][ACC_COMPANY_NAME]
        acct_company_country = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][COUNTRY]
        acct_company_state = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][STATE]
        acct_company_city = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][CITY]
        acct_postal_code = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][ACC_POSTAL_CODE]
        # MatchKey = AID
        if ContactToAccount(i) == ContactToAccount.aid:
            contact[EMAIL] = None
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: AID only;')
        # MatchKey = AID + Email
        elif ContactToAccount(i) == ContactToAccount.aid_email:
            contact[EMAIL] = '%s@%s' % (contact[EMAIL][:contact[EMAIL].index('@')], acct_website[acct_website.find('@')+1:])
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: AID + Email;')
        # MatchKey = AID + Email (Public domain) + Website
        elif ContactToAccount(i) == ContactToAccount.aid_email_website:
            contact[EMAIL] = '%s@gmail.com' % (contact[EMAIL][:contact[EMAIL].index('@')])
            contact[WEBSITE] = acct_website
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: AID + Email (with public domain) + Website;')
        # MatchKey = AID + Name + Location
        elif ContactToAccount(i) == ContactToAccount.aid_nl:
            contact[EMAIL] = None
            contact[COMPANY_NAME] = acct_company_name
            contact[COUNTRY] = acct_company_country
            contact[STATE] = acct_company_state
            contact[CITY] = acct_company_city
            contact[POSTAL_CODE] = acct_postal_code
            update_scenario(contact, 'Contact to Account: AID + Name + Location;')
        # MatchKey = AID (New) -- Also covers case that multiple contacts match to same account
        elif ContactToAccount(i) == ContactToAccount.aid_new:
            contact[CUSTOMER_ACCOUNT_ID] = 'AID_NEW_1'
            contact[REF_CUSTOMER_ACCOUNT_ID] = contact[CUSTOMER_ACCOUNT_ID]
            contact[EMAIL] = '%s@new.domain1.com' % (contact[EMAIL][:contact[EMAIL].index('@')])
            contact[COMPANY_NAME] = 'New Testing Company 1'
            contact[COUNTRY] = 'USA'
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: new AID (multiple Contacts with same Account info);')
        # MatchKey = Email
        elif ContactToAccount(i) == ContactToAccount.email:
            contact[CUSTOMER_ACCOUNT_ID] = None
            contact[EMAIL] = '%s@%s' % (contact[EMAIL][:contact[EMAIL].index('@')], acct_website[acct_website.find('@')+1:])
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: Email only;')
        # MatchKey = Email + Website
        elif ContactToAccount(i) == ContactToAccount.email_website:
            contact[CUSTOMER_ACCOUNT_ID] = None
            contact[EMAIL] = '%s@%s' % (contact[EMAIL][:contact[EMAIL].index('@')], acct_website[acct_website.find('@')+1:])
            contact[WEBSITE] = 'google.com' # Email is higher priority than Website, so google.com is not honored
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: Email + Website;')
        # MatchKey = Email + Name + Location
        elif ContactToAccount(i) == ContactToAccount.email_nl:
            contact[CUSTOMER_ACCOUNT_ID] = None
            contact[EMAIL] = '%s@%s' % (contact[EMAIL][:contact[EMAIL].index('@')], acct_website[acct_website.find('@')+1:])
            contact[COMPANY_NAME] = acct_company_name
            contact[COUNTRY] = acct_company_country
            contact[STATE] = acct_company_state
            contact[CITY] = acct_company_city
            contact[POSTAL_CODE] = acct_postal_code
            update_scenario(contact, 'Contact to Account: Email + Name + Location;')
        # MatchKey = Email (New) -- Also covers case that multiple contacts match to same account
        elif ContactToAccount(i) == ContactToAccount.email_new:
            contact[CUSTOMER_ACCOUNT_ID] = None
            contact[EMAIL] = '%s@new.domain2.com' % (contact[EMAIL][:contact[EMAIL].index('@')])
            contact[COMPANY_NAME] = 'New Testing Company 2'
            contact[COUNTRY] = 'USA'
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: new Email (multiple Contacts with same Account info);')
        # MatchKey = Website
        elif ContactToAccount(i) == ContactToAccount.webiste:
            contact[CUSTOMER_ACCOUNT_ID] = None
            contact[EMAIL] = None
            contact[WEBSITE] = acct_website
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: Website only;')
        # MatchKey = Website + Name + Location
        elif ContactToAccount(i) == ContactToAccount.website_nl:
            contact[CUSTOMER_ACCOUNT_ID] = None
            contact[EMAIL] = None
            contact[WEBSITE] = acct_website
            contact[COMPANY_NAME] = acct_company_name
            contact[COUNTRY] = acct_company_country
            contact[STATE] = acct_company_state
            contact[CITY] = acct_company_city
            contact[POSTAL_CODE] = acct_postal_code
            update_scenario(contact, 'Contact to Account: Website + Name + Location;')
        # MatchKey = Website (New) -- Also covers case that multiple contacts match to same account
        elif ContactToAccount(i) == ContactToAccount.website_new:
            contact[CUSTOMER_ACCOUNT_ID] = None
            contact[EMAIL] = None
            contact[WEBSITE] = 'new.domain3.com'
            contact[COMPANY_NAME] = 'New Testing Company 3'
            contact[COUNTRY] = 'USA'
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: new Website (multiple Contacts with same Account info);')
        else:
            raise Exception('Unknown Contact to Account match scenario: %s' % ContactToAccount(i))


def update_contact(contacts):
    for contact in contacts:
        contact[CUSTOMER_CONTACT_ID] = None
        if contact[EMAIL] is not None:
            contact[FIRST_NAME] = None
            contact[LAST_NAME] = None
            contact[PHONE_NUMBER] = None
            update_scenario(contact, 'Contact match: Email;')
        else:
            update_scenario(contact, 'Contact match: First Name + Last Name + Phone Number;')


def update_scenario(contact, msg):
    contact[TEST_SCENARIO] = ' '.join([contact[TEST_SCENARIO], msg])


def output(rows, file, schema):
    if os.path.isfile(file):
        os.remove(file)
    with open(file, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = schema)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


if __name__ == '__main__':
    # Account
    # dict: AID -> Account, list: Account, list: Account field names
    aid_to_account, accounts, account_schema = read_account_base(ACCOUNT_FILE)
    # For ProcessAccount test
    accounts1 = split(accounts, 0, 900)
    # 1st import for UpdateAccount test
    accounts2 = split(accounts, 400, 500)
    update_account(accounts2)
    # 2nd import for UpdateAccount test
    accounts3 = split(accounts, 900, 1000)
    output(accounts1, 'EntityMatch_Account_1_900.csv', account_schema)
    output(accounts2, 'EntityMatch_Account_401_500.csv', account_schema)
    output(accounts3, 'EntityMatch_Account_901_1000.csv', account_schema)

    # Contact
    # list: Contact, list: Contact field names
    contacts, contact_schema = read_contact_base(CONTACT_FILE)
    # For ProcessAccount test
    contacts1 = split(contacts, 0, 900)
    # 1st import for UpdateAccount test
    contacts3 = split(contacts, 900, 1000)
    update_account_in_contact(contacts1, aid_to_account)
    update_account_in_contact(contacts3, aid_to_account)
    # 2nd import for UpdateAccount test
    contacts2 = split(contacts1, 400, 500)
    update_contact(contacts2)
    output(contacts1, 'EntityMatch_Contact_1_900.csv', contact_schema)
    output(contacts2, 'EntityMatch_Contact_401_500.csv', contact_schema)
    output(contacts3, 'EntityMatch_Contact_901_1000.csv', contact_schema)