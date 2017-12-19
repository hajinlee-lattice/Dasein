package com.latticeengines.cdl.workflow.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EventProxy;

@Component("createCdlTableUtils")
public class CreateCdlTableHelper {

    private static final Logger log = LoggerFactory.getLogger(CreateCdlTableHelper.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private EventProxy eventProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    public Table getFilterTable(CustomerSpace customerSpace, String recordType, String tableSuffix, String tableName,
                                EventFrontEndQuery query, InterfaceName type, String targetTableName) {
        Table filterTable = null;
        log.info("Table Name:" + tableName);
        if (StringUtils.isNotBlank(tableName)) {
            filterTable = metadataProxy.getTable(customerSpace.toString(), tableName);
            if (filterTable != null) {
                log.info("Filter table is null.");
                return filterTable;
            }
        }
        Schema schema = getSchema(recordType);
        String filePath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString();
        tableName = targetTableName + tableSuffix;
        filePath += "/" + tableName + "/" + "/part-00000.avro";
        filterTable = runQueryToTable(customerSpace, schema, tableName, filePath, query, type);
        metadataProxy.updateTable(customerSpace.toString(), filterTable.getName(), filterTable);
        return filterTable;
    }

    private Table runQueryToTable(CustomerSpace customerSpace, Schema schema, String tableName, String filePath,
                                  EventFrontEndQuery query, InterfaceName type) {

        String accountIdKey = InterfaceName.AccountId.name().toLowerCase();
        String periodIdKey = InterfaceName.PeriodId.name().toLowerCase();
        int rowNumber = 0, pageSize = 10000;
        long total = 0;
        while (true) {
            query.setPageFilter(new PageFilter(rowNumber, pageSize));
            DataPage dataPage = null;
            switch (type) {
            case Train:
                dataPage = eventProxy.getTrainingTuples(customerSpace.toString(), query);
                break;
            case Event:
                dataPage = eventProxy.getEventTuples(customerSpace.toString(), query);
                break;
            default:
                dataPage = eventProxy.getScoringTuples(customerSpace.toString(), query);
            }

            List<Map<String, Object>> rows = dataPage.getData();
            if (CollectionUtils.isEmpty(rows)) {
                break;
            }
            List<GenericRecord> records = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                GenericRecord record = new GenericData.Record(schema);
                record.put(InterfaceName.AccountId.name(), row.get(accountIdKey));
                record.put(InterfaceName.PeriodId.name(), Long.valueOf(row.get(periodIdKey).toString()));
                records.add(record);
            }
            writeRecords(schema, filePath, records);
            total += rows.size();
            rowNumber += pageSize;
        }
        log.info(type + "total filter rows=" + total);
        Table table = MetaDataTableUtils.createTable(yarnConfiguration, tableName, filePath);
        table.getExtracts().get(0).setExtractionTimestamp(System.currentTimeMillis());
        return table;
    }

    private void writeRecords(Schema schema, String targetFile, List<GenericRecord> data) {
        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, targetFile)) {
                AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetFile, data);
            } else {
                AvroUtils.appendToHdfsFile(yarnConfiguration, targetFile, data);
            }
            log.info("Write a buffer of " + data.size() + " rows to " + targetFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Schema getSchema(String recordName) {
        String schemaString = "{\"namespace\": \"RatingEngineModel\", \"type\": \"record\", " + "\"name\": \"%s\","
                + "\"fields\": ["
                + "{\"name\": \"AccountId\", \"type\": [\"string\", \"null\"]}, {\"name\": \"PeriodId\", \"type\": [\"long\", \"null\"]}"
                + "]}";
        schemaString = String.format(schemaString, recordName);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

}
