from __future__ import absolute_import

import json
import logging
from datacloud.configdb.utils import get_sql231_manage_db, get_config_db, str_to_value, bool_to_value, utf8_to_latin1

_logger = logging.getLogger(__name__)


def create_table():
    _logger.info('Recreating table [TechIndicator]')
    conn = get_config_db()
    with conn.cursor() as cursor:
        sql = """
        DROP TABLE IF EXISTS `TechIndicator`;
        """
        cursor.execute(sql)
        sql = """
        CREATE TABLE `TechIndicator` (
          `Name`     VARCHAR(100),
          `Source`    VARCHAR(5),
          `SearchKey`  VARCHAR(20),
          `SearchValue`  VARCHAR(1000),
          `BitPosition`  BIGINT,
          `SubCategory` VARCHAR(100),
          `InAM`      BOOLEAN,
          `Description`  VARCHAR(1000),
          `TechName`  VARCHAR(64),
          PRIMARY KEY (`Name`, `Source`)
        )
          ENGINE = InnoDB;
        """
        cursor.execute(sql)
        conn.commit()

def generate_config():
    """
    Currently we download data from SQL231 and upload to mysql
    In future, we should be able to generate this config table independent of SQL Server
    """

    sql231 = get_sql231_manage_db()
    items = []
    with sql231.cursor() as cursor:
        cursor.execute("SELECT * FROM [Config_TechIndicator_2.0.4]")
        for tech in cursor:
            item = convert_to_mysql_item(tech)
            items.append(item)
            if len(items) % 1000 == 0:
                _logger.info("Read %d rows from SQL231" % len(items))
        _logger.info("Read %d rows from SQL231" % len(items))

    dump_items(items)

def dump_items(items):
    sql0 = [ """
    INSERT INTO `TechIndicator` (
          `Name`,
          `Source`,
          `SearchKey`,
          `SearchValue`,
          `BitPosition`,
          `SubCategory`,
          `InAM`,
          `Description`,
          `TechName`
    ) VALUES (
    """ ]
    sql0 = "\n".join([l[4:] for l in sql0])
    conn = get_config_db()
    with conn.cursor() as cursor:
        for item in items:
            value = [
                str_to_value(item['Name']),
                str_to_value(item['Source']),
                str_to_value(item['SearchKey']),
                str_to_value(item['SearchValue']),
                str(item['BitPosition']),
                str_to_value(item['SubCategory']),
                bool_to_value(item['InAM']),
                str_to_value(item['Description']),
                str_to_value(item['TechName'])
            ]
            sql = sql0 +  ",".join(value) + ");"
            try:
                cursor.execute(sql)
            except Exception:
                _logger.error("Failed to insert config %s" % json.dumps(item))
                raise
        _logger.info("Inserted %d rows to TechIndicatorConfig" %  len(items))
        conn.commit()

def convert_to_mysql_item(tech):
    try:
        item = {
            'Name': tech['Name'],
            'Source': tech['Source'],
            'SearchKey': tech['SearchKey'],
            'SearchValue': tech['SearchValue'],
            'BitPosition': tech['BitPosition'],
            'SubCategory': tech['SubCategory'],
            'InAM': tech['InAM'] == 1,
            'Description': utf8_to_latin1(tech['Description']),
            'TechName': tech['TechName'],
        }
    except Exception:
        _logger.error("Failed to convert tech config from SQL Server:" + json.dumps(tech))
        raise
    return item

def to_decode_strategy(config):
    strategy = {
        "BitPosition": config['BitPosition'],
        "BitInterpretation": "BOOLEAN_YESNO"
    }
    if config['Source'] == 'BW':
        strategy['EncodedColumn'] = 'BuiltWith_TechIndicators'
    elif config['SearchKey'] == 'Segment_Name':
        strategy['EncodedColumn'] = 'HGData_SegmentTechIndicators'
    elif config['SearchKey'] == 'Supplier_Name':
        strategy['EncodedColumn'] = 'HGData_SupplierTechIndicators'
    return json.dumps(strategy)


def execute():
    create_table()
    generate_config()
    get_sql231_manage_db().close()
    get_config_db().close()


if __name__ == '__main__':
    from datacloud.common.log import init_logging
    init_logging()
    execute()