package com.latticeengines.cdl.workflow.steps;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.UpdateStatsObjectsConfiguration;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("updateStatsObjects")
public class UpdateStatsObjects extends BaseWorkflowStep<UpdateStatsObjectsConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(UpdateStatsObjects.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private Map<BusinessEntity, Table> statsTableMap = new HashMap<>();

    @Override
    public void execute() {
        log.info("Inside UpdateStatsObjects execute()");
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        Map<BusinessEntity, String> entityTableNames = getMapObjectFromContext(SERVING_STORE_IN_STATS,
                BusinessEntity.class, String.class);
        Map<BusinessEntity, Table> entityTableMap = new HashMap<>();
        entityTableNames.forEach((entity, tableName) -> {
            Table table = metadataProxy.getTable(customerSpaceStr, tableName);
            entityTableMap.put(entity, table);
        });
        Table masterTable = entityTableMap.get(BusinessEntity.Account);
        if (masterTable == null) {
            throw new NullPointerException("Master table for stats object calculation is not found.");
        }
        DataCollection.Version inactiveVersion = dataCollectionProxy.getInactiveVersion(customerSpaceStr);
        Table profileTable = dataCollectionProxy.getTable(customerSpaceStr, TableRoleInCollection.Profile, inactiveVersion);
        if (profileTable == null) {
            throw new NullPointerException("Profile table for stats object calculation is not found.");
        }

        Map<BusinessEntity, String> statsTableNames = getMapObjectFromContext(STATS_TABLE_NAMES, BusinessEntity.class,
                String.class);
        if (statsTableNames != null) {
            statsTableNames.forEach((entity, tableName) -> {
                log.info(String.format("statsTableName for entity %s and customer %s is %s", entity, customerSpaceStr,
                        tableName));
                Table statsTable = metadataProxy.getTable(customerSpaceStr, tableName);
                if (statsTable == null) {
                    throw new NullPointerException("Target table " + tableName + " for Stats Object is not found.");
                }
                statsTableMap.put(entity, statsTable);
            });
        }
        StatisticsContainer statsContainer = constructStatsContainer(entityTableMap, statsTableMap);
        statsContainer.setVersion(inactiveVersion);
        dataCollectionProxy.upsertStats(customerSpaceStr, statsContainer);
    }

    @Override
    public void onExecutionCompleted() {
        statsTableMap.forEach((entity, table) -> {
            log.info("Drop stats table " + table.getName() + " for entity " + entity);
            metadataProxy.deleteTable(getConfiguration().getCustomerSpace().toString(), table.getName());
        });
    }

    private StatsCube getStatsCube(Table targetTable) {
        List<Extract> extracts = targetTable.getExtracts();
        List<String> paths = new ArrayList<>();
        for (Extract extract : extracts) {
            paths.add(extract.getPath());
        }
        log.info("Checking for result file: " + StringUtils.join(paths, ", "));
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, paths);
        return StatsCubeUtils.parseAvro(records);
    }

    private StatisticsContainer constructStatsContainer(Map<BusinessEntity, Table> entityTableMap,
            Map<BusinessEntity, Table> statsTableMap) {
        log.info("Converting stats cube to statistics container.");
        // hard code entity
        Map<BusinessEntity, List<ColumnMetadata>> mdMap = new HashMap<>();
        entityTableMap.forEach((entity, table) -> mdMap.put(entity, table.getColumnMetadata()));
        // get StatsCube from statsTable
        Map<BusinessEntity, StatsCube> cubeMap = new HashMap<>();
        statsTableMap.forEach((entity, table) -> cubeMap.put(entity, getStatsCube(table)));
        try {
            ObjectMapper om = new ObjectMapper();
            om.writeValue(new File("/tmp/cubeMap.json"), cubeMap);
            om.writeValue(new File("/tmp/mdMap.json"), mdMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Statistics statistics = StatsCubeUtils.constructStatistics(cubeMap, mdMap);
        StatisticsContainer statsContainer = new StatisticsContainer();
        statsContainer.setStatistics(statistics);
        statsContainer.setName(NamingUtils.timestamp("Stats"));
        return statsContainer;
    }

}
