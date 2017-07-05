import argparse
import httplib2
import logging
import os
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

logger = logging.getLogger(__name__)

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
_READONLY_SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
_CLIENT_SECRET_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'conf', 'client_secret.json')
_APPLICATION_NAME = 'Lattice Data Cloud Google Sheets API'
_SERVICE = None
_ACCOUNT_MASTER_SHEET_ID = '13ZmSmtzP5C3X-F3JrL7GJa40TT2_EVV5ApOTc93Cjes'

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, 'sheets.googleapis.datacloud.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        logger.debug('Obtaining credentials...')
        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        client_file = os.path.join(project_dir, 'conf', 'client_secret.json')
        flow = client.flow_from_clientsecrets(client_file, _READONLY_SCOPES)
        flow.user_agent = _APPLICATION_NAME
        flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args(args=[])
        credentials = tools.run_flow(flow, store, flags)
        logger.info('Storing credentials to ' + credential_path)
    return credentials


def get_service():
    global _SERVICE
    if _SERVICE is None:
        credentials = get_credentials()
        http = credentials.authorize(httplib2.Http())
        discovery_url = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
        _SERVICE = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discovery_url, cache_discovery=False)
    return _SERVICE

def get_range(rangeName):
    logger.debug("Requesting range %s" % rangeName)
    result = get_service().spreadsheets().values().get(spreadsheetId=_ACCOUNT_MASTER_SHEET_ID, range=rangeName).execute()
    return result.get('values', [])

def read_sheet(sheet_name, min_col='A', max_col='Z'):
    logger.info("Reading google sheet [%s]" % sheet_name)
    titleRow = '%s!%s1:%s1' % (sheet_name, min_col, max_col)
    titles = get_range(titleRow)[0]
    valid_titles = [t for t in titles if t is not None and t != '']
    max_col = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'[len(valid_titles) - 1]
    logger.debug("Determine the right most column to be %s" % max_col)

    dataRange = '%s!%s2:%s' % (sheet_name, min_col, max_col)
    rows = get_range(dataRange)
    data = []
    for row in rows:
        row_dict = {}
        for i in xrange(len(valid_titles)):
            if i > len(row) - 1:
                value = u''
            else:
                value = row[i]
            row_dict[valid_titles[i]] = value
        data.append(row_dict)
    logger.info("Got %d rows x %d cols from sheet [%s]" % (len(data), len(data[0]), sheet_name))
    return data


if __name__ == '__main__':
    from log import init_logging
    init_logging()
    data = read_sheet('DnB Attributes')
    print data
