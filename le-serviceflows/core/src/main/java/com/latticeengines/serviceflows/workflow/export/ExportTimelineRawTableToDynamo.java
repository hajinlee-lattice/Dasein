package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportTimelineRawTableToDynamoStepConfiguration;

@Component("exportTimelineRawTableToDynamo")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportTimelineRawTableToDynamo extends BaseExportToDynamo<ExportTimelineRawTableToDynamoStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportTimelineRawTableToDynamo.class);

    @Override
    protected List<Exporter> getExporters(List<DynamoExportConfig> configs) {
        List<Exporter> exporters = new ArrayList<>();
        configs.forEach(config -> {
            if (!Boolean.TRUE.equals(config.getRelink()) || !relinkDynamo(config)) {
                if (skipPublication) {
                    log.info("Skip exporting {} to DynamoDB, due to property flag.", config.getTableName());
                } else {
                    Exporter exporter = new TimelineTableExporter(config);
                    exporters.add(exporter);
                }
            }
        });
        return exporters;
    }

    @Override
    protected List<DynamoExportConfig> getExportConfigs() {
        List<DynamoExportConfig> tables = getListObjectFromContext(TIMELINE_RAWTABLES_GOING_TO_DYNAMO, DynamoExportConfig.class);
        if (CollectionUtils.isEmpty(tables)) {
            throw new IllegalStateException("Cannot find timeline raw tables to be published to dynamo.");
        }
        return tables;
    }

    protected class TimelineTableExporter extends Exporter {

        TimelineTableExporter(DynamoExportConfig configuration) {
            super(configuration);
        }

        protected HdfsToDynamoConfiguration generateEaiConfig() {
            String tableName = config.getTableName();
            String inputPath = getInputPath();
            log.info("Found input path for table " + tableName + ": " + inputPath);

            HdfsToDynamoConfiguration eaiConfig = new HdfsToDynamoConfiguration();
            eaiConfig.setName("ExportDynamo_" + tableName);
            eaiConfig.setCustomerSpace(configuration.getCustomerSpace());
            eaiConfig.setExportDestination(ExportDestination.DYNAMO);
            eaiConfig.setExportFormat(ExportFormat.AVRO);
            eaiConfig.setExportInputPath(inputPath);
            eaiConfig.setUsingDisplayName(false);
            eaiConfig.setExportTargetPath("/tmp/path");

            String recordClass = configuration.getEntityClass().getCanonicalName();
            String recordType = configuration.getEntityClass().getSimpleName() + "_" + configuration.getDynamoSignature();

            Map<String, String> properties = new HashMap<>();
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED,
                    CipherUtils.encrypt(awsAccessKey));
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED,
                    CipherUtils.encrypt(awsSecretKey));
            properties.put(HdfsToDynamoConfiguration.CONFIG_ENTITY_CLASS_NAME, recordClass);
            properties.put(HdfsToDynamoConfiguration.CONFIG_REPOSITORY, configuration.getRepoName());
            properties.put(HdfsToDynamoConfiguration.CONFIG_RECORD_TYPE, recordType);
            properties.put(HdfsToDynamoConfiguration.CONFIG_PARTITION_KEY, config.getPartitionKey());
            properties.put(HdfsToDynamoConfiguration.CONFIG_SORT_KEY, config.getSortKey());
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_REGION, awsRegion);
            properties.put(ExportProperty.NUM_MAPPERS, String.valueOf(numMappers));
            eaiConfig.setProperties(properties);

            return eaiConfig;
        }

        protected void registerDataUnit() {
            String customerSpace = configuration.getCustomerSpace().toString();
            DynamoDataUnit unit = new DynamoDataUnit();
            unit.setTenant(CustomerSpace.shortenCustomerSpace(customerSpace));
            String srcTbl = StringUtils.isNotBlank(config.getSrcTableName()) ? config.getSrcTableName()
                    : config.getTableName();
            unit.setName(srcTbl);
            if (!unit.getName().equals(config.getTableName())) {
                unit.setLinkedTable(config.getTableName());
            }
            unit.setPartitionKey(config.getPartitionKey());
            if (StringUtils.isNotBlank(config.getSortKey())) {
                unit.setSortKey(config.getSortKey());
            }
            unit.setSignature(configuration.getDynamoSignature());
            DataUnit created = dataUnitProxy.create(customerSpace, unit);
            log.info("Registered DataUnit: " + JsonUtils.pprint(created));
        }
    }
}
