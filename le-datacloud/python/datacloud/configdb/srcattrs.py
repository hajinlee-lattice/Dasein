from __future__ import absolute_import

import logging
from datacloud.configdb.utils import get_config_db

_logger = logging.getLogger(__name__)

def create_table(conn):
    _logger.info('Recreating table [SourceAttribute]')
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
    _logger.info('Registering attributes for profiling pipeline')
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
                   'AMProfile',
                   '%s',
                   'SourceProfiler'
              FROM LDC_ManageDB.AccountMasterColumn
             WHERE DataCloudVersion = '%s'
        """
        cursor.execute(sql % ('SEGMENT', version))
        conn.commit()
        cursor.execute(sql % ('ENRICH', version))
        conn.commit()

        sql = """
            UPDATE LDC_ConfigDB.SourceAttribute lhs
            INNER JOIN LDC_ManageDB.AccountMasterColumn rhs
			ON lhs.Attribute = rhs.AMColumnID AND rhs.DataCloudVersion = '%s'
            SET lhs.Arguments = CONCAT('{"IsProfile":false',
                                         CASE WHEN rhs.DecodeStrategy IS NOT NULL THEN CONCAT(',"DecodeStrategy":', rhs.DecodeStrategy)
                                         ELSE ''
                                         END,
                                       '}')
			WHERE rhs.Groups NOT LIKE '%%%s%%'
              AND lhs.Source = 'AMProfile'
              AND lhs.Stage = '%s'
              AND lhs.Transformer = 'SourceProfiler';
        """
        cursor.execute(sql % (version, 'Segment', 'SEGMENT'))
        conn.commit()
        cursor.execute(sql % (version, 'Enrichment', 'ENRICH'))
        conn.commit()

        sql = """
			UPDATE LDC_ConfigDB.SourceAttribute lhs
            INNER JOIN LDC_ManageDB.AccountMasterColumn rhs
			ON lhs.Attribute = rhs.AMColumnID AND rhs.DataCloudVersion = '%s'
            SET lhs.Arguments = CONCAT('{"IsProfile":true,"DecodeStrategy":',rhs.DecodeStrategy,
				  CASE WHEN rhs.DecodeStrategy LIKE '%%BOOLEAN_YESNO%%' THEN ',"NumBits":2,"BktAlgo":"BooleanBucket"}'
                       WHEN rhs.DecodeStrategy LIKE '%%ENUM_STRING%%' THEN ',"BktAlgo":"CategoricalBucket"}'
                       WHEN rhs.DecodeStrategy LIKE '%%NUMERIC_UNSIGNED_INT%%' OR rhs.DecodeStrategy LIKE '%%NUMERIC_INT%%' THEN ',"BktAlgo":"IntervalBucket"}'
				  ELSE '}' 
			      END)
			WHERE rhs.Groups LIKE '%%%s%%' AND rhs.DecodeStrategy IS NOT NULL
              AND lhs.Source = 'AMProfile'
              AND lhs.Stage = '%s'
              AND lhs.Transformer = 'SourceProfiler';
        """
        cursor.execute(sql % (version, 'Segment', 'SEGMENT'))
        conn.commit()
        cursor.execute(sql % (version, 'Enrichment', 'ENRICH'))
        conn.commit()

        sql = """
			UPDATE LDC_ConfigDB.SourceAttribute lhs
            INNER JOIN LDC_ManageDB.AccountMasterColumn rhs
			ON lhs.Attribute = rhs.AMColumnID AND rhs.DataCloudVersion = '%s'
			SET lhs.Arguments = '{"IsProfile":true}'
			WHERE rhs.Groups LIKE '%%%s%%' AND lhs.Arguments IS NULL
              AND lhs.Source = 'AMProfile'
              AND lhs.Stage = '%s'
              AND lhs.Transformer = 'SourceProfiler';;
        """
        cursor.execute(sql % (version, 'Segment', 'SEGMENT'))
        conn.commit()
        cursor.execute(sql % (version, 'Enrichment', 'ENRICH'))
        conn.commit()


def register_am(conn):
    _logger.info('Registering attributes for account master rebuild')
    with conn.cursor() as cursor:
        sql = """
            INSERT INTO LDC_ConfigDB.SourceAttribute (
                Source,
                Stage,
                Transformer,
                Attribute,
                Arguments
            )
            SELECT 'AccountMaster',
                   'MapStage',
                   'mapAttribute',
                   InternalName,
                   CONCAT('{"target":null,"attribute":"', SourceColumn, '","Source":"', Source, '"}')
              FROM LDC_ConfigDB.AccountMaster_Attributes
             WHERE (ColumnGroup IS NULL OR ColumnGroup NOT IN ('BuiltWith_TechIndicators', 'HGData_SegmentTechIndicators', 'HGData_SupplierTechIndicators', 'BmbrSurge_BucketCode', 'BmbrSurge_CompositeScore', 'BmbrSurge_Intent'))
               AND InternalName NOT IN ('DUNS_NUMBER', 'LE_DOMAIN', 'BUSINESS_NAME', 'STREET_ADDRESS', 'CITY_NAME', 'STATE_PROVINCE_NAME', 'COUNTRY_NAME', 'POSTAL_CODE', 'LE_IS_PRIMARY_DOMAIN', 'LE_IS_PRIMARY_LOCATION', 'LE_NUMBER_OF_LOCATIONS', 'LE_COMPANY_PHONE', 'LE_REVENUE_RANGE', 'LE_EMPLOYEE_RANGE', 'EMPLOYEES_TOTAL', 'CHIEF_EXECUTIVE_OFFICER_NAME')
               AND Source IS NOT NULL
        """
        cursor.execute(sql)
        conn.commit()

        sql = """
            INSERT INTO LDC_ConfigDB.SourceAttribute (
                Source,
                Stage,
                Transformer,
                Attribute,
                Arguments
            ) VALUES (
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'BuiltWith_TechIndicators',
                '{"target":null,"attribute":"TechIndicators","Source":"BuiltWithTechIndicators"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'HGData_SegmentTechIndicators',
                '{"target":null,"attribute":"SegmentTechIndicators","Source":"HGDataTechIndicators"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'HGData_SupplierTechIndicators',
                '{"target":null,"attribute":"SupplierTechIndicators","Source":"HGDataTechIndicators"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'BmbrSurge_BucketCode',
                '{"target":null,"attribute":"BmbrSurge_BucketCode","Source":"BomboraSurgePivoted"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'BmbrSurge_CompositeScore',
                '{"target":null,"attribute":"BmbrSurge_CompositeScore","Source":"BomboraSurgePivoted"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'BmbrSurge_Intent',
                '{"target":null,"attribute":"BmbrSurge_Intent","Source":"BomboraSurgePivoted"}'
            )
        """
        cursor.execute(sql)
        conn.commit()

        sql = """
            INSERT INTO LDC_ConfigDB.SourceAttribute (
                Source,
                Stage,
                Transformer,
                Attribute,
                Arguments
            ) VALUES (
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'LatticeID',
                '{"target":null,"attribute":"LatticeID","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'DUNS',
                '{"target":null,"attribute":"DUNS","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'Domain',
                '{"target":null,"attribute":"Domain","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'Name',
                '{"target":null,"attribute":"Name","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'Street',
                '{"target":null,"attribute":"Street","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'City',
                '{"target":null,"attribute":"City","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'State',
                '{"target":null,"attribute":"State","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'Country',
                '{"target":null,"attribute":"Country","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'ZipCode',
                '{"target":null,"attribute":"ZipCode","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'LE_IS_PRIMARY_DOMAIN',
                '{"target":null,"attribute":"LE_IS_PRIMARY_DOMAIN","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'LE_IS_PRIMARY_LOCATION',
                '{"target":null,"attribute":"LE_IS_PRIMARY_LOCATION","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'LE_NUMBER_OF_LOCATIONS',
                '{"target":null,"attribute":"LE_NUMBER_OF_LOCATIONS","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'PrimaryIndustry',
                '{"target":null,"attribute":"PrimaryIndustry","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'LE_COMPANY_PHONE',
                '{"target":null,"attribute":"LE_COMPANY_PHONE","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'LE_REVENUE_RANGE',
                '{"target":null,"attribute":"LE_REVENUE_RANGE","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'LE_EMPLOYEE_RANGE',
                '{"target":null,"attribute":"LE_EMPLOYEE_RANGE","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'DomainSource',
                '{"target":null,"attribute":"DomainSource","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'EMPLOYEES_TOTAL',
                '{"target":null,"attribute":"EMPLOYEES_TOTAL","Source":"AccountMasterSeed"}'
            ),(
                'AccountMaster',
                'MapStage',
                'mapAttribute',
                'CHIEF_EXECUTIVE_OFFICER_NAME',
                '{"target":null,"attribute":"CHIEF_EXECUTIVE_OFFICER_NAME","Source":"AccountMasterSeed"}'
            )
        """
        cursor.execute(sql)
        conn.commit()


def execute(version):
    conn = get_config_db()
    create_table(conn)
    register_amprofile(conn, version)
    register_am(conn)
    conn.close()

