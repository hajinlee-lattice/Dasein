
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
from std_visidb_ds_firstname_sameas_lastname import std_visidb_ds_firstname_sameas_lastname
from std_visidb_ds_companyname_entropy import std_visidb_ds_companyname_entropy
from std_entropy import std_entropy

def metadata():
    return {  'ApprovedUsage'   : 'ModelAndModelInsights'
            , 'Description'     : 'Indicator for spam leads'
            , 'DisplayName'     : 'Spam Lead'
            , 'Category'        : 'Lead Information'
            , 'StatisticalType' : 'nominal'
            , 'Tags'            : 'Internal' }


def ds_company_isunusual(n):
    n = n.lower()

    if re.search('["#$%+:<=>?@\^_`{}~]', n):
        return 1

    if re.search('(^|\s+)[\[]*(none|no|not|delete|asd|sdf|unknown|undisclosed|null|dont|don\'t|n\/a|n\.a|abc|xyz|noname|nocompany)($|\s+)', n):
        return 1

    try:
        test_var = float(n)
        return 1
    except:
        pass

    return 0


def std_visidb_ds_spamindicator(firstName, lastName, title, phone, company):
    if firstName is None or lastName is None or title is None or phone is None or company is None:
        return None

    if std_visidb_ds_firstname_sameas_lastname(firstName, lastName) == 1:
        return 1

    score = 0
    score += ds_company_isunusual(company)

    if len(company) < 5:
        score += 1

    companyNameEntropy = std_visidb_ds_companyname_entropy(company)

    if companyNameEntropy is not None and companyNameEntropy <= 0.03:
        score += 1

    if len(title) <= 2:
        score += 1

    phoneEntropy = std_entropy(phone)

    if phoneEntropy is not None and phoneEntropy <= 0.03:
        score += 1

    if score >= 2:
        return 1

    return 0


def transform(args, record):
    column1 = args["column1"]
    column2 = args["column2"]
    column3 = args["column3"]
    column4 = args["column4"]
    column5 = args["column5"]
    firstName = record[column1]
    lastName = record[column2]
    title = record[column3]
    phone = record[column4]
    company = record[column5]
    return std_visidb_ds_spamindicator(firstName, lastName, title, phone, company)
