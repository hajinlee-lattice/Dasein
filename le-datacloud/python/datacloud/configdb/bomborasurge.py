from __future__ import absolute_import

import json
import logging
from datacloud.configdb.utils import get_sql231_manage_db, get_config_db, str_to_value

_logger = logging.getLogger(__name__)



def create_table():
    _logger.info('Recreating table [BomboraSurge]')
    conn = get_config_db()
    with conn.cursor() as cursor:
        sql = """
        DROP TABLE IF EXISTS `BomboraSurge`;
        """
        cursor.execute(sql)
        sql = """
        CREATE TABLE `BomboraSurge` (
          `Name`     VARCHAR(64),
          `KeyColumn`    VARCHAR(100),
          `ValueColumn`  VARCHAR(100),
          `SearchKey`  VARCHAR(500),
          `BitPosition`  INT,
          `DecodeStrategy` VARCHAR(50),
          `ValueDict`    VARCHAR(500),
          `BitUnit`      INT,
          `SubCategory`  VARCHAR(100),
          PRIMARY KEY (`Name`)
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
        cursor.execute("SELECT * FROM [Config_BomboraSurge_2.0.5]")
        for tech in cursor:
            item = convert_to_mysql_item(tech)
            items.append(item)
            if len(items) % 1000 == 0:
                _logger.info("Read %d rows from SQL231" % len(items))
        _logger.info("Read %d rows from SQL231" % len(items))

    dump_items(items)

def dump_items(items):
    sql0 = [ """
    INSERT INTO `BomboraSurge` (
          `Name`,
          `KeyColumn`,
          `ValueColumn`,
          `SearchKey`,
          `BitPosition`,
          `DecodeStrategy`,
          `ValueDict`,
          `BitUnit`,
          `SubCategory`
    ) VALUES (
    """ ]
    sql0 = "\n".join([l[4:] for l in sql0])
    conn = get_config_db()
    with conn.cursor() as cursor:
        for item in items:
            value = [
                str_to_value(item['Name']),
                str_to_value(item['KeyColumn']),
                str_to_value(item['ValueColumn']),
                str_to_value(item['SearchKey']),
                str(item['BitPosition']),
                str_to_value(item['DecodeStrategy']),
                str_to_value(item['ValueDict']),
                str(item['BitUnit']),
                str_to_value(item['SubCategory'])
            ]
            sql = sql0 +  ",".join(value) + ");"
            try:
                cursor.execute(sql)
            except Exception:
                _logger.error("Failed to insert config %s" % json.dumps(item))
                raise
        _logger.info("Inserted %d rows to TechIndicatorConfig" %  len(items))
        conn.commit()

def convert_to_mysql_item(row):
    try:
        item = {
            'Name': row['Name'],
            'KeyColumn': row['KeyColumn'],
            'ValueColumn': row['ValueColumn'],
            'SearchKey': row['SearchKey'],
            'BitPosition': row['BitPosition'],
            'DecodeStrategy': row['DecodeStrategy'],
            'ValueDict': row['ValueDict'],
            'BitUnit': row['BitUnit'],
            'SubCategory': row['SubCategory'],
        }
    except Exception:
        _logger.error("Failed to convert bombora surge config from SQL Server:" + json.dumps(row))
        raise
    return item


def to_decode_strategy(config):
    strategy = {
        "BitPosition": config['BitPosition'],
        "BitInterpretation": config['DecodeStrategy']
    }
    if config['ValueColumn'] == 'BucketCode':
        strategy['EncodedColumn'] = 'BmbrSurge_BucketCode'
        strategy['ValueDict'] = 'A||B||C'
        strategy['BitUnit'] = 2
    elif config['ValueColumn'] == 'CompositeScore':
        strategy['EncodedColumn'] = 'BmbrSurge_CompositeScore'
        strategy['BitUnit'] = 8
    elif config['ValueColumn'] == 'Intent':
        strategy['EncodedColumn'] = 'BmbrSurge_Intent'
        strategy['ValueDict'] = "Very Low||Low||Medium||High||Very High"
        strategy['BitUnit'] = 3
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