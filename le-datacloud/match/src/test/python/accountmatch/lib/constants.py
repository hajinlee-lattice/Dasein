# match keys
ACCOUNT_ID = 'CustomerAccountId'
SFDC_ID = 'SfdcId'
MKTO_ID = 'MktoId'
NAME = 'Name'
DOMAIN = 'Domain'
DUNS = 'DUNS'
COUNTRY = 'Country'
STATE = 'State'
CITY = 'City'
MATCH_KEY_COLS = [ ACCOUNT_ID, SFDC_ID, MKTO_ID, NAME, DOMAIN, DUNS, COUNTRY, STATE, CITY ]
SYS_ID_COLS = [ ACCOUNT_ID, SFDC_ID, MKTO_ID ]

# test related columns
TEST_GRP_TYPE = 'TestGroupType'
TEST_GRP_ID = 'TestGroupId'
TEST_RECORD_ID = 'TestRecordId'
TEST_DATA_EXISTS_IN_UNIVERSE = 'ExistsInUniverse'
TEST_COLS = [ TEST_GRP_TYPE, TEST_GRP_ID, TEST_RECORD_ID, ACCOUNT_ID,
SFDC_ID, MKTO_ID, NAME, DOMAIN, DUNS, COUNTRY, STATE, CITY, TEST_DATA_EXISTS_IN_UNIVERSE ]

# avro
NULLABLE_STRING_TYPE = [ 'string', 'null' ]
# use string for all fields for now (even though one flag is boolean)
TEST_AVRO_SCHEMA = {
    'namespace': 'entity.match.account',
    'type': 'record',
    'name': 'Account',
    'fields': [ { 'name': col, 'type': NULLABLE_STRING_TYPE } for col in TEST_COLS ]
}
