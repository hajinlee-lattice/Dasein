from __future__ import absolute_import

import logging
from datacloud.db.config.bomborasurge import to_decode_strategy as to_bmbr_decode_strategy
from datacloud.db.config.techindicator import to_decode_strategy as to_tech_decode_strategy
from datacloud.db.config.utils import get_config_db, str_to_value, bool_to_value
from jinja2 import Template

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
    with conn.cursor() as cursor:
        am_attrs = get_am_attrs_cache()
        values = [convert_amattr_to_sql_value(attr) for attr in am_attrs.values()]
        sql = insert_values_sql(values)
        cursor.execute(sql)
        _logger.info("Inserted %d rows to AccountMasterColumn" % len(values))
        conn.commit()


def insert_values_sql(values):
    sqls = ["""
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
          `IsInternalEnrichment`,
          `IsPremium`,
          `DecodeStrategy`
    ) VALUES
    """]
    sqls = [l[4:] for l in sqls]
    sqls.append(",\n".join(values))
    sql = "\n".join(sqls) + ";"
    return sql


def convert_amattr_to_sql_value(am_attr):
    item = convert_amattr_to_amcol(am_attr)
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
        bool_to_value(item['IsInternalEnrichment']),
        bool_to_value(item['IsPremium']),
        str_to_value(item['DecodeStrategy']),
    ]
    return "(" + ", ".join(value) + ")"


def convert_amattr_to_amcol(am_attr):
    am_col = {
        'AMColumnID': am_attr['InternalName'],
        'DisplayName': am_attr['DisplayName'],
        'Description': am_attr['Description'],
        'JavaClass': am_attr['JavaClass'],
        'Category': am_attr['Category'],
        'Subcategory': am_attr['SubCategory'],
        'DisplayDiscretizationStrategy': am_attr['DisplayDiscretizationStrategy'],
        'FundamentalType': am_attr['FundamentalType'],
        'StatisticalType': am_attr['StatisticalType'],
        'IsInternalEnrichment': am_attr['Approved_InternalEnrichment'] == 1,
        'IsPremium': am_attr['IsPremium'] if 'IsPremium' in am_attr else False,
        'DecodeStrategy': am_attr['DecodeStrategy'] if 'DecodeStrategy' in am_attr else None,
    }
    return am_col


def add_tech_indicators():
    grp_tpls = {}
    conn = get_config_db()
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM `AccountMaster_Attributes` WHERE `ColumnGroup` IS NOT NULL")
        for attr in cursor:
            grp_tpls[attr['ColumnGroup']] = {}
            for k, v in attr.items():
                if isinstance(v, basestring):
                    grp_tpls[attr['ColumnGroup']][k] = Template(v)
                else:
                    grp_tpls[attr['ColumnGroup']][k] = v
            _logger.info("Convert column group [%s] to jinja2 template" % attr['ColumnGroup'])

    seen_techs = set()
    render_grp_attrs(grp_tpls['HGData_SupplierTechIndicators'], 'HGData_SupplierTechIndicators', 'TechName',
                     "SELECT * FROM `TechIndicator` WHERE `Source` = 'HG' AND `SearchKey` = 'Supplier_Name'",
                     to_tech_decode_strategy, seen_cols=seen_techs)
    render_grp_attrs(grp_tpls['HGData_SegmentTechIndicators'], 'HGData_SegmentTechIndicators', 'TechName',
                     "SELECT * FROM `TechIndicator` WHERE `Source` = 'HG' AND `SearchKey` = 'Segment_Name'",
                     to_tech_decode_strategy, is_premium=True, seen_cols=seen_techs)
    render_grp_attrs(grp_tpls['BuiltWith_TechIndicators'], 'BuiltWith_TechIndicators', 'TechName',
                     "SELECT * FROM `TechIndicator` WHERE `Source` = 'BW'", to_tech_decode_strategy, is_premium=True,
                     seen_cols=seen_techs)
    render_grp_attrs(grp_tpls['BmbrSurge_BucketCode'], 'BmbrSurge_BucketCode', 'Name',
                     "SELECT * FROM `BomboraSurge` WHERE `ValueColumn` = 'CompositeScore'", to_bmbr_decode_strategy,
                     is_premium=True)
    render_grp_attrs(grp_tpls['BmbrSurge_CompositeScore'], 'BmbrSurge_CompositeScore', 'Name',
                     "SELECT * FROM `BomboraSurge` WHERE `ValueColumn` = 'BucketCode'", to_bmbr_decode_strategy,
                     is_premium=True)
    render_grp_attrs(grp_tpls['BmbrSurge_Intent'], 'BmbrSurge_Intent', 'Name',
                     "SELECT * FROM `BomboraSurge` WHERE `ValueColumn` = 'Intent'", to_bmbr_decode_strategy,
                     is_premium=True)


def render_grp_attrs(grp_tpl, grp_name, id_col, sql, to_decode_func, is_premium=False, seen_cols=set()):
    conn = get_config_db()
    with conn.cursor() as cursor:
        cursor.execute(sql)
        values = []
        for config in cursor:
            attr = {}
            for k, v in grp_tpl.items():
                if isinstance(v, Template):
                    attr[k] = v.render(**config)
                else:
                    attr[k] = v
            attr['InternalName'] = config[id_col]
            attr['DecodeStrategy'] = to_decode_func(config)
            attr['IsPremium'] = is_premium
            if len(attr['InternalName']) > 64:
                attr['InternalName'] = attr['InternalName'][:64]
            if attr['InternalName'].lower() not in seen_cols:
                values.append(convert_amattr_to_sql_value(attr))
                seen_cols.add(attr['InternalName'].lower())
                add_to_am_attrs_cache(attr)
            else:
                _logger.info(
                    "Skip [%s] in [%s], since already seen it in other column groups." % (
                    attr['InternalName'], grp_name))
        sql = insert_values_sql(values)
        cursor.execute(sql)
        _logger.info("Inserted %d attrs in [%s] to AccountMasterColumn" % (len(values), grp_name))
        conn.commit()
    return seen_cols


def update_approved_usages():
    conn = get_config_db()
    update_count = 0
    with conn.cursor() as cursor:
        am_attrs = get_am_attrs_cache()
        for attr in am_attrs.values():
            au = None
            if attr['Approved_Model'] == 1:
                au = 'Model'
                if attr['Approved_Insights'] == 1:
                    au = 'ModelAndModelInsights'
                    if attr['Approved_BIS'] == 1:
                        au = 'ModelAndAllInsights'
            if au is not None:
                sql = 'UPDATE `AccountMasterColumn` SET `ApprovedUsage` = %s WHERE `AMColumnID` = %s' % (
                    str_to_value(au), str_to_value(attr['InternalName']))
                cursor.execute(sql)
                update_count += 1
        conn.commit()
        _logger.info("Updated ApprovedUsage for %d attributes." % update_count)


def update_groups():
    conn = get_config_db()
    update_count = 0
    with conn.cursor() as cursor:
        am_attrs = get_am_attrs_cache()
        for attr in am_attrs.values():
            groups = []
            if attr['Approved_Model'] == 1:
                groups.append('RTS')
            if attr['Approved_Segment'] == 1:
                groups.append('Segment')
            if attr['Approved_ExternalEnrichment'] == 1 or attr['Approved_InternalEnrichment'] == 1:
                groups.append('Enrichment')
            if len(groups) > 0:
                sql = 'UPDATE `AccountMasterColumn` SET `Groups` = %s WHERE `AMColumnID` = %s' % (
                    str_to_value(",".join(groups)), str_to_value(attr['InternalName']))
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


def get_am_attrs_cache():
    global _AM_ATTRS
    if _AM_ATTRS is None:
        _AM_ATTRS = {}
        conn = get_config_db()
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM `AccountMaster_Attributes`")
            for attr in cursor:
                if 'ColumnGroup' not in attr or attr['ColumnGroup'] is None:
                    _AM_ATTRS[attr['InternalName']] = attr
    return _AM_ATTRS


def add_to_am_attrs_cache(attr):
    global _AM_ATTRS
    _AM_ATTRS[attr['InternalName']] = attr


def execute():
    create_table()
    add_static_attrs()
    add_am_attrs()
    add_tech_indicators()
    update_approved_usages()
    update_groups()
    rename_ldc_cols()
    get_config_db().close()


if __name__ == '__main__':
    from datacloud.common.log import init_logging

    init_logging()
    execute()
