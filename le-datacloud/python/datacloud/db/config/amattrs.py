from __future__ import absolute_import

import MySQLdb
import logging
import csv
from datacloud.common.cipher import decrypt

logger = logging.getLogger(__name__)

SOURCE_MAP = {
    'HGData_Pivoted_Source': 'HGDataPivoted',
    'BuiltWith_Pivoted_Source': 'BuiltWithPivoted',
    'Feature_Pivoted_Source': 'FeaturePivoted',
    'HPA_New_Pivoted_Source': 'HPANewPivoted',
    'Orb': 'OrbIntelligenceMostRecent',
    'Alexa': 'AlexaMostRecent',
    'MadisonLogic_30Day_Aggregated': 'Bombora30DayAgg',
    'Semrush': 'SemrushMostRecent'
}


DNB_CODE_COLUMNS = (
    'DOMESTIC_ULTIMATE_DnB_CITY_CODE',
    'STATUS_CODE',
    'DnB_CONTINENT_CODE',
    'US_1987_SIC_1',
    'COMPOSITE_RISK_SCORE',
    'FULL_REPORT_DATE',
    'HIERARCHY_CODE',
    'SUBSIDIARY_INDICATOR',
    'LOCAL_ACTIVITY_TYPE_CODE',
    'DnB_STATE_PROVINCE_CODE',
    'LEGAL_STATUS_CODE',
    'PREMIUM_MARKETING_PRESCREEN',
    'DnB_COUNTY_CODE',
    'LE_SIC_CODE',
    'LE_NAICS_CODE',
    'YEAR_STARTED',
    'DnB_COUNTRY_CODE',
    'TRIPLE_PLAY_SEGMENT'
)

def str_to_value(s):
    return ('\'%s\'' % s.replace("'", "''")) if s is not None else 'NULL'

def create_table(conn):
    logger.info('Recreating table [AccountMaster_Attributes]')
    with conn.cursor() as cursor:
        sql = """
        DROP TABLE IF EXISTS `AccountMaster_Attributes`;
        """
        cursor.execute(sql)
        sql = """
        CREATE TABLE `AccountMaster_Attributes` (
            PID BIGINT NOT NULL AUTO_INCREMENT,
            InternalName VARCHAR(64) NOT NULL,
            Source VARCHAR(200),
            SourceJoinKey VARCHAR(200),
            SourceColumn VARCHAR(100),
            DisplayName VARCHAR(250),
            Description VARCHAR(1000),
            JavaClass VARCHAR(20),
            Category VARCHAR(100),
            SubCategory VARCHAR(100),
            ExportRestriction VARCHAR(20) NOT NULL,
            Deprecated TINYINT NOT NULL DEFAULT 0,
            FundamentalType VARCHAR(100),
            StatisticalType VARCHAR(100),
            DisplayDiscretizationStrategy VARCHAR(1000),
            DnbAvailability VARCHAR(250),
            DnbCodeBook LONGTEXT,
            Approved_Model TINYINT NOT NULL,
            Approved_Insights TINYINT NOT NULL,
            Approved_BIS TINYINT NOT NULL,
            Approved_InternalEnrichment TINYINT NOT NULL,
            Approved_ExternalEnrichment TINYINT NOT NULL,
            Approved_Segment TINYINT NOT NULL,
            PRIMARY KEY (`PID`)
        ) ENGINE = InnoDB;
        """
        cursor.execute(sql)
        conn.commit()

def read_dnb_attributes(conn, file_name):
    ex_attrs = []
    with open(file_name,'rb') as csvFile:
        reader = csv.DictReader(csvFile, delimiter=',', quotechar = '"')
        for row in reader: 
            if row["Add To New Account Master"].strip().replace("\t","") == "Y":
                attr = []
                attr.append(row["Internal Name"].strip().replace("\t",""))
                attr.append(row["Display Name"].strip().replace("\t",""))
                attr.append(row["Description"].strip().replace("\t",""))
                attr.append(row["Data Type"].strip().replace("\t",""))
                attr.append(row["Category"].strip().replace("\t",""))
                attr.append('Other')
                attr.append(row["DnB Export Restriction SFDC"].strip().replace("\t",""))
                attr.append(row["DnB Export Restriction - All"].strip().replace("\t",""))
                attr.append(row["Model Tag"].strip().replace("\t",""))
                attr.append(row["Insights Tag"].strip().replace("\t",""))
                attr.append(row["Internal Enrichment Tag"].strip().replace("\t",""))
                attr.append(row["External Enchrichment Tag"].strip().replace("\t",""))
                attr.append(row["FundamentalType"].strip().replace("\t",""))
                attr.append(row["Statistical Type"].strip().replace("\t",""))
                attr.append(row["DisplayDiscretizationStrategy"].strip().replace("\t",""))
                attr.append(row["Availability"].strip().replace("\t",""))
                attr.append(row["Possible Values Code Table"].strip().replace("\t",""))
                attr.append(row["Segmentation Tag"].strip().replace("\t",""))
                ex_attrs.append(attr)
    ex_attrs = sorted(ex_attrs,key=lambda x: x[0])

    stmts = []
    for row in ex_attrs:
        item = dnb_row_to_item(row)
        sql = """
                INSERT INTO AccountMaster_Attributes (
                    InternalName,
                    Source,
                    SourceJoinKey,
                    SourceColumn,
                    DisplayName,
                    Description,
                    JavaClass,
                    Category,
                    SubCategory,
                    ExportRestriction,
                    FundamentalType,
                    StatisticalType,
                    DisplayDiscretizationStrategy,
                    DnbAvailability,
                    DnbCodeBook,
                    Approved_Model,
                    Approved_Insights,
                    Approved_BIS,
                    Approved_InternalEnrichment,
                    Approved_ExternalEnrichment,
                    Approved_Segment
                ) VALUES (%s)
            """ % ',\n'.join([
                    str_to_value(item['InternalName']),
                    '\'DnBCacheSeed\'',
                    '\'DUNS_NUMBER\'',
                    str_to_value(item['InternalName']),
                    str_to_value(item['DisplayName']),
                    str_to_value(item['Description']),
                    str_to_value(item['JavaClass']),
                    str_to_value(item['Category']),
                    str_to_value(item['SubCategory']),
                    str_to_value(item['ExportRestriction']),
                    str_to_value(item['FundamentalType']),
                    str_to_value(item['StatisticalType']),
                    str_to_value(item['DisplayDiscretizationStrategy']),
                    str_to_value(item['DnbAvailability']),
                    str_to_value(item['DnbCodeBook']),
                    '1' if item['Approved_Model'] else '0',
                    '1' if item['Approved_Insights'] else '0',
                    '0',
                    '1' if item['Approved_InternalEnrichment'] else '0',
                    '1' if item['Approved_ExternalEnrichment'] else '0',
                    '1' if item['Approved_Segment'] else '0'
                ])
        stmts.append((item['InternalName'], sql))

    with conn.cursor() as cursor:
        for n, s in stmts:
            print "Adding DnB attribute [%s]" % n
            cursor.execute(s)

        conn.commit()


def dnb_row_to_item(row):
    item = {
        'InternalName': row[0] if row[0] != '' else None,
        'DisplayName': row[1] if row[1] != '' else None,
        'Description': row[2].replace('\n', '').replace('\r', '') if row[2]  != '' else None,
        'JavaClass': None,
        'Category': row[4] if row[4]  != '' else None,
        'SubCategory': row[5] if row[5]  != '' else None,
        'ExportRestriction': 'NONE',
        'Approved_Model': row[8] == 'Y' if row[8] is not None else 'N',
        'Approved_Insights': row[9] == 'Y' if row[9] is not None else 'N',
        'Approved_InternalEnrichment': row[10] == 'Y' if row[10] is not None else 'N',
        'Approved_ExternalEnrichment': row[11] == 'Y' if row[11] is not None else 'N',
        'FundamentalType': row[12] if row[12] != '' else None,
        'StatisticalType': row[13] if row[13] != '' else None,
        'DisplayDiscretizationStrategy': row[14] if row[14] != '' else None,
        'DnbAvailability': row[15] if row[15] != '' else None,
        'DnbCodeBook': row[16] if row[16] != '' else None,
        'Approved_Segment': row[17] == 'Y' if row[17] is not None else 'N'
    }

    export_sfdc = row[6] if row[6]  != '' else None
    export_all = row[7] if row[7]  != '' else None

    if export_sfdc == 'Y':
        item['ExportRestriction'] = 'SFDC'
    if export_all == 'Y':
        item['ExportRestriction'] = 'ALL'


    if row[3].lower() == 'alpha-numeric' or row[3].lower() == 'alpha':
        item['JavaClass'] = 'String'
    elif row[3].lower() == 'numeric':
        item['JavaClass'] = 'Integer'
    elif row[3] == '':
        item['JavaClass'] = None
    elif row[3] == 'Boolean':
        item['JavaClass'] = 'Boolean'
    else:
        raise ValueError("Unknown DataType " + row[3])

    if item['InternalName'] in DNB_CODE_COLUMNS:
        item['JavaClass'] = 'String'
    elif item['InternalName'] == 'SALES_VOLUME_US_DOLLARS' or item['InternalName'] == 'SALES_VOLUME_LOCAL_CURRENCY':
        item['JavaClass'] = 'Long'

    return item


def read_existing_attributes(conn, file_name):
    ex_attrs = []
    with open(file_name,'rb') as csvFile:
        reader = csv.DictReader(csvFile, delimiter=',', quotechar = '"')
        for row in reader: 
            if row["Add To New Account Master"].strip().replace("\t","") == "Y":
                attr = []
                attr.append(row["ExternalColumnID"].strip().replace("\t",""))
                attr.append(row["Display Name"].strip().replace("\t",""))
                attr.append(row["Description"].strip().replace("\t",""))
                attr.append(row["DataType"].strip().replace("\t",""))
                attr.append(row["Category"].strip().replace("\t",""))
                attr.append(row["SubCategory"].strip().replace("\t",""))
                attr.append(row["Model Tag"].strip().replace("\t",""))
                attr.append(row["Insights Tag"].strip().replace("\t",""))
                attr.append(row["BIS Tag"].strip().replace("\t",""))
                attr.append(row["Internal Enrichment Tag"].strip().replace("\t",""))
                attr.append(row["External Enrichment Tag"].strip().replace("\t",""))
                attr.append(row["EOL Tag"].strip().replace("\t",""))
                attr.append(row["FundamentalType"].strip().replace("\t",""))
                attr.append(row["StatisticalType"].strip().replace("\t",""))
                attr.append(row["DisplayDiscretizationStrategy"].strip().replace("\t",""))
                attr.append(row["Source"].strip().replace("\t",""))
                attr.append(row["SourceColumnName"].strip().replace("\t",""))
                attr.append(row["Segmentation Tag"].strip().replace("\t",""))
                ex_attrs.append(attr)
    ex_attrs = sorted(ex_attrs,key=lambda x: x[15])

    stmts = []
    for row in ex_attrs:
        item = existing_row_to_item(row)
        sql = """
                INSERT INTO AccountMaster_Attributes (
                    InternalName,
                    Source,
                    SourceJoinKey,
                    SourceColumn,
                    DisplayName,
                    Description,
                    JavaClass,
                    Category,
                    SubCategory,
                    FundamentalType,
                    StatisticalType,
                    DisplayDiscretizationStrategy,
                    Deprecated,
                    Approved_Model,
                    Approved_Insights,
                    Approved_BIS,
                    Approved_InternalEnrichment,
                    Approved_ExternalEnrichment,
                    ExportRestriction,
                    Approved_Segment
                ) VALUES (%s)
                """ % ',\n'.join([
                        str_to_value(item['InternalName']),
                        str_to_value(item['Source']),
                        str_to_value(item['SourceJoinKey']),
                        str_to_value(item['SourceColumn']),
                        str_to_value(item['DisplayName']),
                        str_to_value(item['Description']),
                        str_to_value(item['JavaClass']),
                        str_to_value(item['Category']),
                        str_to_value(item['SubCategory']),
                        str_to_value(item['FundamentalType']),
                        str_to_value(item['StatisticalType']),
                        str_to_value(item['DisplayDiscretizationStrategy']),
                        '1' if item['Deprecated'] else '0',
                        '1' if item['Approved_Model'] else '0',
                        '1' if item['Approved_Insights'] else '0',
                        '1' if item['Approved_BIS'] else '0',
                        '1' if item['Approved_InternalEnrichment'] else '0',
                        '1' if item['Approved_ExternalEnrichment'] else '0',
                        "'NONE'",
                        '1' if item['Approved_Segment'] else '0'
                    ])

        stmts.append((item['InternalName'], item['Source'], sql))

    with conn.cursor() as cursor:
        for n, s, q in stmts:
            print "Adding %s attribute [%s]" % (s, n)
            cursor.execute(q)
        conn.commit()


def existing_row_to_item(row):
    item = {
        'InternalName': row[0] if row[0] != '' else None,
        'DisplayName': row[1] if row[1] != '' else None,
        'Description': row[2].replace('\n', '').replace('\r', '') if row[2] != '' else None,
        'JavaClass': None,
        'Category': row[4] if row[4]  != '' else None,
        'SubCategory': row[5] if row[5]  != '' else 'Other',
        'ExportRestriction': 'NONE',
        'Approved_Model': row[6] == 'Y' if row[6] is not None else 'N',
        'Approved_Insights': row[7] == 'Y' if row[7] is not None else 'N',
        'Approved_BIS': row[8] == 'Y' if row[8] is not None else 'N',
        'Approved_InternalEnrichment': row[9] == 'Y' if row[9] is not None else 'N',
        'Approved_ExternalEnrichment': row[10] == 'Y' if row[10] is not None else 'N',
        'Deprecated': row[11] == 'Y' if row[11] is not None else 'N',
        'FundamentalType': row[12] if row[12] != '' else None,
        'StatisticalType': row[13] if row[13] != '' else None,
        'DisplayDiscretizationStrategy': row[14] if row[14] != '' else None,
        'Source' : row[15] if row[15] != '' else None,
        'SourceColumnName' : row[16] if row[16] != '' else None,
        'SourceJoinKey': 'Domain',
        'Approved_Segment': row[17] == 'Y' if row[17] is not None else 'N'
    }


    if 'VARCHAR' in row[3].upper():
        item['JavaClass'] = 'String'
    elif row[3].upper() == 'INT':
        item['JavaClass'] = 'Integer'
    elif row[3].upper() == 'BIGINT' or row[3].upper() == 'DATETIME':
        item['JavaClass'] = 'Long'
    elif row[3].upper() == 'BIT':
        item['JavaClass'] = 'Boolean'
    elif row[3].upper() == 'REAL':
        item['JavaClass'] = 'Float'
    elif row[3].upper() == 'FLOAT':
        item['JavaClass'] = 'Double'
    else:
        raise ValueError("Unknown DataType " + row[3])

    if item['Source'] is not None and 'Orb' in item['Source'].split(','):
        item['Source'] = SOURCE_MAP['Orb']

    if item['Source'] in SOURCE_MAP:
        item['Source'] = SOURCE_MAP[item['Source']]

    if item['Source'] in ('HPA', 'Jobs', 'Twitter'):
        item['SourceJoinKey'] = 'DUNS_NUMBER'

    if item['SourceColumnName'] is not None and item['SourceColumnName'] != '':
        item['SourceColumn'] = item['SourceColumnName']
    else:
        item['SourceColumn'] = item['InternalName']

    # source specific treatment:

    if item['Source'] == 'FeaturePivoted':
        item['SourceJoinKey'] = 'URL'
    elif item['Source'] == 'AlexaTimeseries':
        item['SourceColumn'] = item['InternalName'].replace('Alexa_', '')
    elif item['Source'] is None or item['Source'] == '':
        item['SourceColumn'] = item['InternalName']

    if item['InternalName'] == 'LE_DOMAIN':
        item['InternalName'] = 'Domain'

    return item


def verify_am_attrs(conn):
    logger.info("Verify generated table.")
    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT
           `InternalName`
          ,`StatisticalType`
          ,`FundamentalType`
          ,`DisplayDiscretizationStrategy`
          FROM `AccountMaster_Attributes`
          WHERE `Approved_Model` = 1 AND NULLIF(`StatisticalType`, '') IS NULL
          ORDER BY `InternalName`
        """)

        title_printed = False
        for row in cursor:
            if not title_printed:
                print '\nThe following columns are approved to Model but do not have StatisticalType:'
                title_printed = True
            print row[0]

        if title_printed:
            print '\n'

        cursor.execute("""
        SELECT
           `InternalName`
          ,`StatisticalType`
          ,`FundamentalType`
          ,`DisplayDiscretizationStrategy`
          FROM `AccountMaster_Attributes`
          WHERE `Approved_Insights` = 1 AND NULLIF(`FundamentalType`, '') IS NULL
          ORDER BY `InternalName`
        """)

        title_printed = False
        for row in cursor:
            if not title_printed:
                print '\nThe following columns are approved to Insights but do not have FundamentalType:'
                title_printed = True
            print row[0]
        if title_printed:
            print '\n'

def execute():
    pwd = decrypt(b'1AZy8-CiCvVE81AL66tHuqT6G5qwbD0zIOY1hBs45Po=')
    conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd=pwd,db="LDC_ConfigDB")

    create_table(conn)
    read_dnb_attributes(conn, 'Account Master Attribute Metadata - DnB Attributes.csv')
    read_existing_attributes(conn, 'Account Master Attribute Metadata - Existing Attributes.csv')
    read_existing_attributes(conn, 'Account Master Attribute Metadata - Derived Attributes.csv')
    verify_am_attrs(conn)
    conn.close()

if __name__ == '__main__':
    execute()
