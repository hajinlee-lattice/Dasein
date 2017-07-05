from __future__ import absolute_import

import logging
from datacloud.db.config.utils import get_config_db, str_to_value, bool_to_value

_logger = logging.getLogger(__name__)
_AM_ATTRS = None

def create_table():
    _logger.info('Recreating table [AccountMaster_Attributes]')
    conn = get_config_db()
    with conn.cursor() as cursor:
        sql = """
        DROP TABLE IF EXISTS `AccountMasterColumn`;
        """
        cursor.execute(sql)
        sql = """
        CREATE TABLE `AccountMasterColumn` (
          `AMColumnID`                    VARCHAR(64)  NOT NULL,
          `ApprovedUsage`                 VARCHAR(255) DEFAULT 'None',
          `Category`                      VARCHAR(50)  NOT NULL,
          `DecodeStrategy`                VARCHAR(1000),
          `Description`                   VARCHAR(1000),
          `DisplayDiscretizationStrategy` VARCHAR(1000),
          `DisplayName`                   VARCHAR(255) NOT NULL,
          `FundamentalType`               VARCHAR(50),
          `Groups`                        VARCHAR(700) NOT NULL DEFAULT '',
          `IsInternalEnrichment`          BOOLEAN      NOT NULL DEFAULT FALSE,
          `JavaClass`                     VARCHAR(50)  NOT NULL,
          `IsPremium`                     BOOLEAN      NOT NULL DEFAULT FALSE,
          `StatisticalType`               VARCHAR(50),
          `Subcategory`                   VARCHAR(200),
          PRIMARY KEY (`AMColumnID`)
        )
          ENGINE = InnoDB;
        """
        cursor.execute(sql)
        conn.commit()

def add_static_attrs():
    sql = """
    INSERT INTO `AccountMasterColumn` (
          `AMColumnID`,
          `DisplayName`,
          `Description`,
          `JavaClass`,
          `Category`,
          `Subcategory`,
          `StatisticalType`,
          `FundamentalType`,
          `ApprovedUsage`,
          `IsPremium`,
          `Groups`
    ) VALUES (
        'LatticeAccountId',
        'Lattice Account ID',
        'The identifier of an account in Lattice Data Cloud.',
        'String',
        'DEFAULT',
        'Other',
        NULL,
        'ALPHA',
        'None',
        FALSE,
        'ID'
    ), (
        'IsPublicDomain',
        'Is Public Domain',
        'Indicates whether email represents a public domain, e.g. gmail.com',
        'Boolean',
        'LEAD_INFORMATION',
        'Other',
        'NOMINAL',
        'BOOLEAN',
        'Model',
        FALSE,
        'RTS'
    )
    """
    conn = get_config_db()
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
        _logger.info("Inserted 2 static attrs: LatticeAccountId and IsPublicDomain")

def add_am_attrs():
    conn = get_config_db()
    sql = [ """
    INSERT INTO `AccountMasterColumn` (
          `AMColumnID`,
          `Category`,
          `Description`,
          `DisplayDiscretizationStrategy`,
          `DisplayName`,
          `FundamentalType`,
          `JavaClass`,
          `StatisticalType`,
          `Subcategory`,
          `IsInternalEnrichment`
    ) VALUES
    """ ]
    sql = [l[4:] for l in sql]
    with conn.cursor() as cursor:
        am_attrs = get_am_attrs()
        values = []
        for attr in am_attrs.values():
            item = convert_amattr_to_amcol(attr)
            value = [
                str_to_value(item['AMColumnID']),
                str_to_value(item['Category']),
                str_to_value(item['Description']),
                str_to_value(item['DisplayDiscretizationStrategy']),
                str_to_value(item['DisplayName']),
                str_to_value(item['FundamentalType']),
                str_to_value(item['JavaClass']),
                str_to_value(item['StatisticalType']),
                str_to_value(item['Subcategory']),
                bool_to_value(item['IsInternalEnrichment'])
            ]
            values.append("(" + ", ".join(value) + ")")
        sql.append(",\n".join(values))
        sql = "\n".join(sql) + ";"
        cursor.execute(sql)
        _logger.info("Inserted %d rows to AccountMasterColumn" %  len(values))
        conn.commit()

def convert_amattr_to_amcol(am_attr):
    am_col = {
        'AMColumnID': am_attr['InternalName'],
        'DisplayName': am_attr['DisplayName'],
        'Description': am_attr['Description'],
        'JavaClass': am_attr['JavaClass'],
        'Category': am_attr['Category'],
        'Subcategory': am_attr['InternalName'],
        'DisplayDiscretizationStrategy': am_attr['DisplayDiscretizationStrategy'],
        'FundamentalType': am_attr['FundamentalType'],
        'StatisticalType': am_attr['StatisticalType'],
        'IsInternalEnrichment': am_attr['Approved_InternalEnrichment'] == 1
    }
    return am_col

def update_approved_usages():
    conn = get_config_db()
    update_count = 0
    with conn.cursor() as cursor:
        am_attrs = get_am_attrs()
        for attr in am_attrs.values():
            au = None
            if attr['Approved_Model'] == 1:
                au = 'Model'
                if attr['Approved_Insights'] == 1:
                    au = 'ModelAndModelInsights'
                    if attr['Approved_BIS'] == 1:
                        au = 'ModelAndAllInsights'
            if au is not None:
                sql = 'UPDATE `AccountMasterColumn` SET `ApprovedUsage` = %s WHERE `AMColumnID` = %s' % (str_to_value(au), str_to_value(attr['InternalName']))
                cursor.execute(sql)
                update_count += 1
        conn.commit()
        _logger.info("Updated ApprovedUsage for %d attributes." % update_count)


def update_groups():
    conn = get_config_db()
    update_count = 0
    with conn.cursor() as cursor:
        am_attrs = get_am_attrs()
        for attr in am_attrs.values():
            groups = []
            if attr['Approved_Model'] == 1:
                groups.append('RTS')
            if attr['Approved_Segment'] == 1:
                groups.append('Segment')
            if attr['Approved_ExternalEnrichment'] == 1 or attr['Approved_InternalEnrichment'] == 1:
                groups.append('Enrichment')
            if len(groups) > 0:
                sql = 'UPDATE `AccountMasterColumn` SET `Groups` = %s WHERE `AMColumnID` = %s' % (str_to_value(",".join(groups)), str_to_value(attr['InternalName']))
                cursor.execute(sql)
                update_count += 1
        conn.commit()
        _logger.info("Updated Groups for %d attributes." % update_count)


def rename_ldc_cols():
    ldc_cols = [
        ('LE_DOMAIN', 'LDC_Domain'),
        ('DUNS_NUMBER', 'LDC_DUNS'),
        ('BUSINESS_NAME', 'LDC_Name'),
        ('CITY_NAME', 'LDC_City'),
        ('STATE_PROVINCE_NAME', 'LDC_State'),
        ('COUNTRY_NAME', 'LDC_Country'),
        ('STREET_ADDRESS', 'LDC_Street'),
        ('POSTAL_CODE', 'LDC_ZipCode')
    ]
    conn = get_config_db()
    with conn.cursor() as cursor:
        for old_name, new_name in ldc_cols:
            sql = "UPDATE `AccountMasterColumn` SET `AMColumnID` = %s WHERE `AMColumnID` = %s" \
                  % (str_to_value(new_name), str_to_value(old_name))
            _logger.info("Rename %s to %s" % (old_name, new_name))
            cursor.execute(sql)
        conn.commit()

def get_am_attrs():
    global _AM_ATTRS
    if _AM_ATTRS is None:
        _AM_ATTRS = {}
        conn = get_config_db()
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM `AccountMaster_Attributes`")
            for attr in cursor:
                _AM_ATTRS[attr['InternalName']] = attr
    return _AM_ATTRS


def execute():
    create_table()
    add_static_attrs()
    add_am_attrs()
    update_approved_usages()
    update_groups()
    rename_ldc_cols()
    get_config_db().close()


if __name__ == '__main__':
    execute()
