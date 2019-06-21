import copy
import csv
import os
import re
import uuid

from enum import Enum
from random import randrange

ACCOUNT_FILE = 'Account_All.csv'
CONTACT_FILE = 'Contact_All.csv'
TXN_FILE = 'Transaction_All.csv'
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

UUID_STR = str(uuid.uuid1())


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

class AccountInfo:
    def __init__(self, website, name, country, state, city, postal_code):
        self.website = website
        self.name = name
        self.country = country
        self.state = state
        self.city = city
        self.postal_code = postal_code


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


def read_txn_base(file):
    txns = []
    schema = []
    with open(file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        schema = reader.fieldnames
        for row in reader:
            standardize_txn(row)
            txns.append(row)
    schema = standardize_txn_schema(schema)
    return txns, schema


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


def standardize_txn(txn):
    if ACCOUNT_ID in txn:
        txn[CUSTOMER_ACCOUNT_ID] = txn[ACCOUNT_ID]
        txn.pop(ACCOUNT_ID, None)
        
        
def standardize_txn_schema(schema):
    schema = [CUSTOMER_ACCOUNT_ID if s == ACCOUNT_ID else s for s in schema]
    return schema


def split(list, begin_idx, end_idx):
    return copy.deepcopy(list[begin_idx:end_idx])


def update_account(accounts):
    for account in accounts:
        account[CUSTOMER_ACCOUNT_ID] = None
        update_scenario(account, 'No AID;')


def update_account_in_contact(contacts, aid_to_account, update_mode):
    for k in range(len(contacts)):
        contact = contacts[k]
        i = k % len(list(ContactToAccount))
        acct_website = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][WEBSITE]
        acct_company_name = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][ACC_COMPANY_NAME]
        acct_company_country = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][COUNTRY]
        acct_company_state = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][STATE]
        acct_company_city = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][CITY]
        acct_postal_code = aid_to_account[contact[CUSTOMER_ACCOUNT_ID]][ACC_POSTAL_CODE]
        account = AccountInfo(acct_website, acct_company_name, acct_company_country, acct_company_state, acct_company_city, acct_postal_code)
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
            contact[CUSTOMER_ACCOUNT_ID] = 'AID_NEW_1' if not update_mode else 'AID_NEW_2'
            contact[REF_CUSTOMER_ACCOUNT_ID] = contact[CUSTOMER_ACCOUNT_ID]
            contact[EMAIL] = ('%s@new.domain1.com' if not update_mode else '%s@new.domain2.com') % (contact[EMAIL][:contact[EMAIL].index('@')])
            contact[COMPANY_NAME] = ('FakeName_%s_1' if not update_mode else 'FakeName_%s_2') % (UUID_STR)
            contact[COUNTRY] = 'USA'
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: new AID (multiple Contacts with same Account info);')
        # MatchKey = Email
        elif ContactToAccount(i) == ContactToAccount.email:
            if acct_company_country == 'United States':
                contact[CUSTOMER_ACCOUNT_ID] = None
                contact[EMAIL] = '%s@%s' % (contact[EMAIL][:contact[EMAIL].index('@')], acct_website[acct_website.find('@')+1:])
                contact[POSTAL_CODE] = None
                update_scenario(contact, 'Contact to Account: Email only;')
            else:
                update_account_in_contact_case_email_nl(contact, account)
        # MatchKey = Email + Website
        elif ContactToAccount(i) == ContactToAccount.email_website:
            if acct_company_country == 'United States':
                contact[CUSTOMER_ACCOUNT_ID] = None
                contact[EMAIL] = '%s@%s' % (contact[EMAIL][:contact[EMAIL].index('@')], acct_website[acct_website.find('@')+1:])
                contact[WEBSITE] = 'google.com' # Email is higher priority than Website, so google.com is not honored
                contact[POSTAL_CODE] = None
                update_scenario(contact, 'Contact to Account: Email + Website;')
            else:
                update_account_in_contact_case_email_nl(contact, account)
        # MatchKey = Email + Name + Location
        elif ContactToAccount(i) == ContactToAccount.email_nl:
            update_account_in_contact_case_email_nl(contact, account)
        # MatchKey = Email (New) -- Also covers case that multiple contacts match to same account
        elif ContactToAccount(i) == ContactToAccount.email_new:
            contact[CUSTOMER_ACCOUNT_ID] = None
            contact[REF_CUSTOMER_ACCOUNT_ID] = contact[CUSTOMER_ACCOUNT_ID]
            contact[EMAIL] = ('%s@new.domain3.com' if not update_mode else '%s@new.domain4.com') % (contact[EMAIL][:contact[EMAIL].index('@')])
            contact[COMPANY_NAME] = ('FakeName_%s_3' if not update_mode else 'FakeName_%s_4') % (UUID_STR)
            contact[COUNTRY] = 'USA'
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: new Email (multiple Contacts with same Account info);')
        # MatchKey = Website
        elif ContactToAccount(i) == ContactToAccount.webiste:
            if acct_company_country == 'United States':
                contact[CUSTOMER_ACCOUNT_ID] = None
                contact[EMAIL] = None
                contact[WEBSITE] = acct_website
                contact[POSTAL_CODE] = None
                update_scenario(contact, 'Contact to Account: Website only;')
            else:
                update_account_in_contact_case_website_nl(contact, account)
        # MatchKey = Website + Name + Location
        elif ContactToAccount(i) == ContactToAccount.website_nl:
            update_account_in_contact_case_website_nl(contact, account)
        # MatchKey = Website (New) -- Also covers case that multiple contacts match to same account
        elif ContactToAccount(i) == ContactToAccount.website_new:
            contact[CUSTOMER_ACCOUNT_ID] = None
            contact[REF_CUSTOMER_ACCOUNT_ID] = contact[CUSTOMER_ACCOUNT_ID]
            contact[EMAIL] = None
            contact[WEBSITE] = 'new.domain5.com' if not update_mode else 'new.domain6.com'
            contact[COMPANY_NAME] = ('FakeName_%s_5' if not update_mode else 'FakeName_%s_6') % (UUID_STR)
            contact[COUNTRY] = 'USA'
            contact[POSTAL_CODE] = None
            update_scenario(contact, 'Contact to Account: new Website (multiple Contacts with same Account info);')
        else:
            raise Exception('Unknown Contact to Account match scenario: %s' % ContactToAccount(i))


def update_account_in_contact_case_email_nl(contact, account):
    contact[CUSTOMER_ACCOUNT_ID] = None
    contact[EMAIL] = '%s@%s' % (contact[EMAIL][:contact[EMAIL].index('@')], account.website[account.website.find('@')+1:])
    contact[COMPANY_NAME] = account.name
    contact[COUNTRY] = account.country
    contact[STATE] = account.state
    contact[CITY] = account.city
    contact[POSTAL_CODE] = account.postal_code
    update_scenario(contact, 'Contact to Account: Email + Name + Location;')


def update_account_in_contact_case_website_nl(contact, account):
    contact[CUSTOMER_ACCOUNT_ID] = None
    contact[EMAIL] = None
    contact[WEBSITE] = account.website
    contact[COMPANY_NAME] = account.name
    contact[COUNTRY] = account.country
    contact[STATE] = account.state
    contact[CITY] = account.city
    contact[POSTAL_CODE] = account.postal_code
    update_scenario(contact, 'Contact to Account: Website + Name + Location;')


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
            

# For ProcessTxn test:
# 1. Update CustomerAccountId in range [901, 1000] to be [10901, 11000] by adding 10000
#    Reason: Expect CustomerAccountId in range [10901, 11000] becomes NEW account created by ProcessTxn test
#            Expect CustomerAccountId in range [901, 1000] becomes NEW account created by UpdateAccount test which runs after ProcessTxn
# 2. Update CustomerAccountId in range [1001, ) to be [0, 899] by mod 900
#    Reason: To avoid trigger profiling account in ProcessTxn test, new CustomerAccountId cannot be more than 30% of accounts created by ProcessAccount test
def update_account_in_txn_for_process(txns):
    for txn in txns:
        if (int(txn[CUSTOMER_ACCOUNT_ID]) >= 901 and int(txn[CUSTOMER_ACCOUNT_ID]) <= 1000):
            txn[CUSTOMER_ACCOUNT_ID] = int(txn[CUSTOMER_ACCOUNT_ID]) + 10000
        elif (int(txn[CUSTOMER_ACCOUNT_ID]) > 1000):
            txn[CUSTOMER_ACCOUNT_ID] = int(txn[CUSTOMER_ACCOUNT_ID]) % 900


# For UpdateTxn test:
# 1. Update CustomerAccountId in range [1001, ) to be [20000, 20899] by mod 900 and adding 20000
#    Reason: Expect CustomerAccountId originally in range [20000, 20899] become NEW account created by UpdateTxn test
#            Also to avoid trigger profiling account in UpdateTxn test, new CustomerAccountId cannot be more than 30% of accounts created by all the previous end2end tests
def update_account_in_txn_for_update(txns):
    for txn in txns:
        if (int(txn[CUSTOMER_ACCOUNT_ID]) > 1000):
            txn[CUSTOMER_ACCOUNT_ID] = int(txn[CUSTOMER_ACCOUNT_ID]) % 900 + 20000


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
    update_account_in_contact(contacts1, aid_to_account, False)
    update_account_in_contact(contacts3, aid_to_account, True)
    # 2nd import for UpdateAccount test
    contacts2 = split(contacts1, 400, 500)
    update_contact(contacts2)
    output(contacts1, 'EntityMatch_Contact_1_900.csv', contact_schema)
    output(contacts2, 'EntityMatch_Contact_401_500.csv', contact_schema)
    output(contacts3, 'EntityMatch_Contact_901_1000.csv', contact_schema)
    
    # Transaction
    # list: Transaction, list: Transaction field names
    txns, txn_schema = read_txn_base(TXN_FILE)
    # For ProcessTxn test
    txn1 = split(txns, 0, 25000)
    txn2 = split(txns, 25000, 50000)
    # For UpdateTxn test
    txn3 = split(txns, 46000, 60000)
    update_account_in_txn_for_process(txn1)
    update_account_in_txn_for_process(txn2)
    update_account_in_txn_for_update(txn3)
    output(txn1, 'EntityMatch_Transaction_1_25K.csv', txn_schema)
    output(txn2, 'EntityMatch_Transaction_25K_50K.csv', txn_schema)
    output(txn3, 'EntityMatch_Transaction_46K_60K.csv', txn_schema)