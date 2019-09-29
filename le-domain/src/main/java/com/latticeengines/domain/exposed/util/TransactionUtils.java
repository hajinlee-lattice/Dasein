package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class TransactionUtils {

    private static final Logger log = LoggerFactory.getLogger(TransactionUtils.class);

    public static List<Table> createTxnTables(TableRoleInCollection role,
            List<PeriodStrategy> periodStrategies, Configuration yarnConfiguration,
            String tableBasePath) {
        SchemaInterpretation schema;
        List<String> tablePrefixes = new ArrayList<>();
        switch (role) {
            case ConsolidatedRawTransaction:
                schema = SchemaInterpretation.TransactionRaw;
                tablePrefixes.add("");
                break;
            case ConsolidatedDailyTransaction:
                schema = SchemaInterpretation.TransactionDailyAggregation;
                tablePrefixes.add("");
                break;
            case ConsolidatedPeriodTransaction:
                schema = SchemaInterpretation.TransactionPeriodAggregation;
                periodStrategies.forEach(strategy -> {
                    tablePrefixes
                            .add(PeriodStrategyUtils.getTablePrefixFromPeriodStrategy(strategy));
                });
                break;
            default:
                throw new UnsupportedOperationException(role + " is not a supported period store.");
        }

        List<Table> txnTables = new ArrayList<>();
        for (String tablePrefix : tablePrefixes) {
            Table table = SchemaRepository.instance().getSchema(schema);
            String tableName = tablePrefix + NamingUtils.timestamp(role.name());
            table.setName(tableName);

            try {
                log.info("Initialize transaction store " + tableBasePath + "/" + tableName);
                if (HdfsUtils.isDirectory(yarnConfiguration, tableBasePath + "/" + tableName)) {
                    HdfsUtils.rmdir(yarnConfiguration, tableBasePath + "/" + tableName);
                }
                HdfsUtils.mkdir(yarnConfiguration, tableBasePath + "/" + tableName);
            } catch (Exception e) {
                log.error("Failed to initialize transaction store " + tableBasePath + "/"
                        + tableName);
                throw new RuntimeException("Failed to create transaction store " + role);
            }

            Extract extract = new Extract();
            extract.setName("extract_target");
            extract.setExtractionTimestamp(DateTime.now().getMillis());
            extract.setProcessedRecords(1L);
            extract.setPath(tableBasePath + "/" + tableName + "/*.avro");
            table.setExtracts(Collections.singletonList(extract));
            txnTables.add(table);
        }

        return txnTables;
    }

    public static boolean hasAnalyticProduct(Configuration yarnConfiguration, String filePath) {
        filePath = getPath(filePath);
        String avroPath = filePath + "/*.avro";
        log.info("Load transactions from " + avroPath);
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, avroPath);
        while (records.hasNext()) {
            GenericRecord record = records.next();
            if (ProductType.Analytic.name()
                    .equals(String.valueOf(record.get(InterfaceName.ProductType.name())))) {
                return true;
            }
        }
        return false;
    }

    private static String getPath(String avroDir) {
        return ProductUtils.getPath(avroDir);
    }
}
