from __future__ import absolute_import

import MySQLdb
import logging
from datacloud.common.cipher import decrypt

logger = logging.getLogger(__name__)

def create_table(conn):
    logger.info('Recreating table [SourceAttribute]')
    with conn.cursor() as cursor:
        sql = """
        DROP TABLE IF EXISTS LDC_ConfigDB.SourceAttribute;
        """
        cursor.execute(sql)
        sql = """
            CREATE TABLE LDC_ConfigDB.SourceAttribute (
              `SourceAttributeID` bigint(20) NOT NULL AUTO_INCREMENT,
              `Arguments` varchar(1024) DEFAULT NULL,
              `Attribute` varchar(128) NOT NULL,
              `Source` varchar(128) NOT NULL,
              `Stage` varchar(32) NOT NULL,
              `Transformer` varchar(32) NOT NULL,
              PRIMARY KEY (`SourceAttributeID`),
              UNIQUE KEY `SourceAttributeID` (`SourceAttributeID`),
              UNIQUE KEY `Source` (`Source`,`Stage`,`Transformer`,`Attribute`),
              KEY `IX_SOURCE_STAGE_TRANSFORMER` (`Source`,`Stage`,`Transformer`)
            ) ENGINE=InnoDB;
        """
        cursor.execute(sql)
        conn.commit()

def register_amprofile(conn, version):
    logger.info('Registering attributes for profiling pipeline')
    with conn.cursor() as cursor:
        sql = """
            INSERT INTO LDC_ConfigDB.SourceAttribute
            (`Arguments`,
             `Attribute`,
             `Source`,
             `Stage`,
             `Transformer`)
            SELECT NULL,
                   AMColumnID,
                   'AccountMasterBucketed',
                   'Profiling',
                   'SourceProfiler'
              FROM LDC_ManageDB.AccountMasterColumn
             WHERE DataCloudVersion = '%s'
        """ % (version)
        cursor.execute(sql)
        conn.commit()

        sql = """
            UPDATE LDC_ConfigDB.SourceAttribute lhs
            INNER JOIN LDC_ManageDB.AccountMasterColumn rhs
			ON lhs.Attribute = rhs.AMColumnID AND rhs.DataCloudVersion = '%s'
            SET lhs.Arguments = '{"IsSegment":false}'
			WHERE rhs.Groups NOT LIKE '%%Segment%%';
        """ % (version)
        cursor.execute(sql)
        conn.commit()

        sql = """
			UPDATE LDC_ConfigDB.SourceAttribute lhs
            INNER JOIN LDC_ManageDB.AccountMasterColumn rhs
			ON lhs.Attribute = rhs.AMColumnID AND rhs.DataCloudVersion = '%s'
            SET lhs.Arguments = CONCAT('{"IsSegment":true,"DecodeStrategy":',rhs.DecodeStrategy,',' ,
				  CASE WHEN rhs.DecodeStrategy LIKE '%%BOOLEAN_YESNO%%' THEN '"NumBits":2,"BktAlgo":"BooleanBucket"}'
				  ELSE '}' 
			      END)
			WHERE rhs.Groups LIKE '%%Segment%%' AND rhs.DecodeStrategy IS NOT NULL;
        """ % (version)
        cursor.execute(sql)
        conn.commit()

        sql = """
			UPDATE LDC_ConfigDB.SourceAttribute lhs
            INNER JOIN LDC_ManageDB.AccountMasterColumn rhs
			ON lhs.Attribute = rhs.AMColumnID AND rhs.DataCloudVersion = '%s'
			SET lhs.Arguments = '{"IsSegment":true}'
			WHERE rhs.Groups LIKE '%%Segment%%' AND lhs.Arguments IS NULL;
        """ % (version)
        cursor.execute(sql)
        conn.commit()

def execute():
    datacloud_version = '2.0.4'
    pwd = decrypt(b'1AZy8-CiCvVE81AL66tHuqT6G5qwbD0zIOY1hBs45Po=')
    conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd=pwd)
    create_table(conn)
    register_amprofile(conn, datacloud_version)

if __name__ == '__main__':
    execute()
