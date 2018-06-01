import os
import random
import uuid
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from csv import DictReader


def generateAmounts(howMany, lo=100, hi=10000, stepSize=10, zeroValueSize=0.025, nullValueSize=0.025, randomState=None):
    assert howMany >= 0
    assert lo <= hi

    if randomState is not None:
        random.seed(randomState)

    result = [random.choice(range(lo, hi + stepSize, stepSize)) for _ in range(howMany)]

    i = 0
    while i < int(zeroValueSize * howMany):
        idx = random.choice(range(howMany))
        if result[idx] == 0:
            continue
        else:
            result[idx] = 0
            i += 1

    i = 0
    while i < int(nullValueSize * howMany):
        idx = random.choice(range(howMany))
        if result[idx] == 0 or result[idx] is np.NaN:
            continue
        else:
            result[idx] = np.NaN
            i += 1

    return result


def splitList(inputList, split1Size=0.95, randomState=None) -> '(splitted1, splitted2)':
    assert split1Size <= 1.0

    if randomState is not None:
        random.seed(randomState)

    split1 = random.sample(inputList, int(split1Size * len(inputList)))
    split2 = list(set(inputList) - set(split1))
    assert len(split1) + len(split2) == len(inputList)

    return split1, split2


def zipLists(listX, listY) -> '[(x1, y1), (x2, y2), (x3, y1), ...]':
    assert len(listX) >= len(listY)

    return [(x, random.choice(listY)) for x in listX]


ap = argparse.ArgumentParser()
ap.add_argument('-p', '--path', required=True, help='path to working directory')
argv = vars(ap.parse_args())


accountIds = set()
for accountFile in filter(lambda f: f.endswith('.csv') and f.startswith('Account_'), os.listdir(argv['path'])):
    file = argv['path'] + os.path.sep + accountFile
    print('[INFO] reading {}'.format(file))
    with open(file, 'r', encoding='ISO-8859-1') as fp:
        ids = [row['Id'] for row in DictReader(fp)]
        accountIds |= set(ids)
print('[INFO] {} unique account Ids are read.\n'.format(str(len(accountIds))))
accountIds = list(accountIds)

contactIds = set()
for contactFile in filter(lambda f: f.endswith('.csv') and f.startswith('Contact_'), os.listdir(argv['path'])):
    file = argv['path'] + os.path.sep + contactFile
    print('[INFO] reading {}'.format(file))
    with open(file, 'r', encoding='ISO-8859-1') as fp:
        ids = [row['ContactId'] for row in DictReader(fp)]
        contactIds |= set(ids)
print('[INFO] {} unique contact Ids are read.\n'.format(str(len(contactIds))))
contactIds = list(contactIds)

productBundleIds = set()
productHierarchyIds = set()
productVdbIds = set()
for productFile in filter(lambda f: f.endswith('.csv') and f.startswith('Product'), os.listdir(argv['path'])):
    file = argv['path'] + os.path.sep + productFile
    print('[INFO] reading {}'.format(file))
    with open(file, 'r', encoding='ISO-8859-1') as fp:
        ids = [row['Id'] for row in DictReader(fp)]
        if 'bundles' in productFile.lower():
            productBundleIds |= set(ids)
        elif 'hierarchies' in productFile.lower():
            productHierarchyIds |= set(ids)
        elif 'vdb' in productFile.lower():
            productVdbIds |= set(ids)
        else:
            print('[WARN] unknown product file {}'.format(productFile))
productIds = set() | productBundleIds | productHierarchyIds | productVdbIds
print('[INFO] {} unique product bundles are read.'.format(str(len(productBundleIds))))
print('[INFO] {} unique product hierarchies are read.'.format(str(len(productHierarchyIds))))
print('[INFO] {} unique VDB products are read.\n'.format(str(len(productVdbIds - productBundleIds))))
print('[INFO] {} unique products in total.\n'.format(str(len(productIds))))
productBundleIds = list(productBundleIds)
productHierarchyIds = list(productHierarchyIds)
productVdbIds = list(productVdbIds)
productIds = list(productIds)

# requirements 1, 2, 3
# datetimeFormatStr = '%Y-%m-%dT%H:%M:%S %z'
datetimeFormatStr = '%Y-%m-%d'

# 2016 quarters
trxsPerQuarter = 2000
quarters2016 = [datetime.strptime('2016-03-15', datetimeFormatStr)] * trxsPerQuarter +                [datetime.strptime('2016-06-15', datetimeFormatStr)] * trxsPerQuarter +                [datetime.strptime('2016-09-15', datetimeFormatStr)] * trxsPerQuarter +                [datetime.strptime('2016-12-15', datetimeFormatStr)] * trxsPerQuarter
print('[INFO] {} transactions per quarter, {} dates generated for years 2016.'.format(
    trxsPerQuarter, len(quarters2016)))

# 2017 weeks
trxsPerWeek = 1000
weeks2017 = []
weekStarts = [datetime.strptime('2017-01-02', datetimeFormatStr)]
while weekStarts[-1].year == 2017:
    start = weekStarts[-1] + timedelta(weeks=1)
    weekStarts.append(start)
del weekStarts[-1]  # delete 2018-01-01
for start in weekStarts:
    for i in range(trxsPerWeek):
        if start.strftime('%Y-%m-%d') != '2017-12-25':
            trxDate = start + timedelta(days=random.randrange(0, 5))
        else:
            trxDate = date(2017, 12, 31)
        weeks2017.append(trxDate)
print('[INFO] {} transactions per week, {} dates generated for years 2017.'.format(trxsPerWeek, len(weeks2017)))

# transaction IDs
df1 = pd.DataFrame({
    'TransactionId': pd.Series([uuid.uuid4().hex.upper() for _ in range(len(quarters2016) + len(weeks2017))]),
    'TransactionTime': pd.Series(quarters2016 + weeks2017, dtype=np.datetime64)})

# requirement 4
prod1 = productIds[:10]
acct1 = random.sample(accountIds, 900) + random.sample(range(2000, 3000), 100)
random.shuffle(acct1)
acctProd1 = zipLists(acct1, prod1)
trxsPerQuarter2016 = 1500
trxsPerQuarter2017 = 10000
dates2016 = [(row['TransactionId'], row['TransactionDate'].date())
            for index, row in df1.iterrows() if row['TransactionDate'].year == 2016]
quarterTrxDate = random.sample(dates2016, trxsPerQuarter2016 * 4)
dates2017q1, dates2017q2, dates2017q3, dates2017q4 = [], [], [], []
for index, row in df1.iterrows():
    if row['TransactionDate'].year == 2017 and row['TransactionDate'].month in (1, 2, 3):
        dates2017q1.append(row)
    elif row['TransactionDate'].year == 2017 and row['TransactionDate'].month in (4, 5, 6):
        dates2017q2.append(row)
    elif row['TransactionDate'].year == 2017 and row['TransactionDate'].month in (7, 8, 9):
        dates2017q3.append(row)
    elif row['TransactionDate'].year == 2017 and row['TransactionDate'].month in (10, 11, 12):
        dates2017q4.append(row)
quarterTrxDate += (random.sample(dates2017q1, trxsPerQuarter2017) +                    random.sample(dates2017q2, trxsPerQuarter2017) +                    random.sample(dates2017q3, trxsPerQuarter2017) +                    random.sample(dates2017q4, trxsPerQuarter2017))
trxAcctProd1 = zipLists(quarterTrxDate, acctProd1)
trxAcctProdDF = pd.DataFrame({
    'TransactionId': pd.Series([x[0][0] for x in trxAcctProd1]),
    'TransactionDate': pd.Series([x[0][1] for x in trxAcctProd1]),
    'AccountId': pd.Series([x[1][0] for x in trxAcctProd1]),
    'ProductId': pd.Series([x[1][1] for x in trxAcctProd1])})
assert len(set(trxAcctProdDF['AccountId'])) == len(acct1)
assert len(set(trxAcctProdDF['ProductId'])) == len(prod1)
df2 = pd.merge(left=df1, right=trxAcctProdDF, on='TransactionId', how='left', suffixes=('', '_y'))
assert len(df2[(df2['TransactionDate_y'].notnull()) & (df2['TransactionDate'] != df2['TransactionDate_y'])]) == 0
df2 = df2.drop('TransactionDate_y', axis=1)
print('[INFO] {} productIds {} have transactions for ALL accounts (len={}) in every quarter.'.format(
    len(prod1), str(prod1), len(acct1)))

# requirement 5
prod2 = productIds[10:15]
acct2 = accountIds[:50]
acctProd2 = zipLists(acct2, prod2)
print('[INFO] {} productIds {} have NO transactions for {} accounts {}.'.format(
    len(prod2), str(prod2), len(acct2), str(acct2)))

# requirement 6
prod3 = productIds[15:20]
acct3 = accountIds[50:100]
acctProd3 = zipLists(acct3, prod3)
quarterDates = []
for index, row in df2[df2['AccountId'].isnull()].iterrows():
    trxDate = row['TransactionDate']
    if trxDate.year == 2016 or (trxDate.year == 2017 and trxDate.month in (1, 2, 3)):
        quarterDates.append((row['TransactionId'], trxDate))
trxAcctProd3 = zipLists(quarterDates, acctProd3)
# trxAcctProd3
mask = df2['TransactionId'].isin([x[0][0] for x in trxAcctProd3])
df2.loc[mask, 'AccountId'] = [x[1][0] for x in trxAcctProd3]
df2.loc[mask, 'ProductId'] = [x[1][1] for x in trxAcctProd3]
print('[INFO] {} productIds {} for {} accounts {} have transactions earlier than 1st quarter of 2017 (included).'.format(
    len(prod3), str(prod3), len(acct3), str(acct3)))

# requirement 7, 8
acct4 = acct1
prod4 = productIds[20:50] + [uuid.uuid4().hex.upper() for _ in range(10)]
random.shuffle(prod4)
acctProd4 = zipLists(acct4, prod4)
trxId4 = df2[df2['AccountId'].isnull()]['TransactionId'].tolist()
trxAcctProd4 = zipLists(trxId4, acctProd4)
mask4 = df2['TransactionId'].isin(x[0] for x in trxAcctProd4)
df2.loc[mask4, 'AccountId'] = [x[1][0] for x in trxAcctProd4]
df2.loc[mask4, 'ProductId'] = [x[1][1] for x in trxAcctProd4]
assert df2[df2['AccountId'].isnull()].shape == df2[df2['ProductId'].isnull()].shape

acctSet1 = set(accountIds)
acctSet2 = set(df2['AccountId'])
acctSet1And2 = acctSet1 & acctSet2
acctDiffSet1 = acctSet1 - acctSet1And2
acctDiffSet2 = acctSet2 - acctSet1And2
print('[INFO] {} accountIds: {} in Account but not in Transaction.'.format(len(acctDiffSet1), str(acctDiffSet1)))
print('[INFO] {} accountIds: {} in Transaction but not in Account.'.format(len(acctDiffSet2), str(acctDiffSet2)))

prodSet1 = set(productIds)
prodSet2 = set(df2['ProductId'])
prodSet1And2 = prodSet1 & prodSet2
prodDiffSet1 = prodSet1 - prodSet1And2
prodDiffSet2 = prodSet2 - prodSet1And2
print('[INFO] {} productIds: {} in Product but not in Transaction.'.format(len(prodDiffSet1), str(prodDiffSet1)))
print('[INFO] {} productIds: {} in Transaction but not in Product.'.format(len(prodDiffSet2), str(prodDiffSet2)))

# requirement 9
amounts = generateAmounts(df2.shape[0], randomState=42)
df2['Amount'] = pd.Series(amounts, dtype=np.float32, index=df2.index)

# requirement 10
df2['Cost'] = pd.Series([np.NaN for _ in range(df2.shape[0])])
lo = 0.3
hi = 0.7
sizeRatio = 0.5
mask = (df2['Amount'].notnull()) & (df2['Amount'] > 0)
idAmount = df2[['TransactionId', 'Amount']][mask]
trxIds = random.sample(idAmount['TransactionId'].tolist(), int(idAmount.shape[0] * sizeRatio))
r = []
for _ in trxIds:
    r.append(random.uniform(lo, hi))
df2.loc[df2['TransactionId'].isin(trxIds), 'Cost'] = r * df2.loc[df2['TransactionId'].isin(trxIds), 'Amount']

df2['Quantity'] = pd.Series([10 for _ in range(df2.shape[0])])
df3 = df2.fillna('')
indexHalf = df3.shape[0] // 2
df3_1 = df3.iloc[:indexHalf]  # 0 ~ 30K
df3_2 = df3.iloc[indexHalf - 10000:]  # split 3, 20K ~ 60K
df3_11 = df3_1.iloc[:df3_1.shape[0] // 2]  # split 1, 0 ~ 15K
df3_12 = df3_1.iloc[df3_1.shape[0] // 2:]  # split 2, 15K ~ 30K
assert df3_1.shape[0] == 30000
assert df3_11.shape[0] == 15000
assert df3_12.shape[0] == 15000
assert df3_2.shape[0] == 40000
df3.to_csv(argv['path'] + os.path.sep + 'Transaction_0_60K.csv', index=False)
df3_11.to_csv(argv['path'] + os.path.sep + 'Transaction_0_15K.csv', index=False)
df3_12.to_csv(argv['path'] + os.path.sep + 'Transaction_15K_30K.csv', index=False)
df3_2.to_csv(argv['path'] + os.path.sep + 'Transaction_20K_60K.csv', index=False)
print('[INFO] done!')
