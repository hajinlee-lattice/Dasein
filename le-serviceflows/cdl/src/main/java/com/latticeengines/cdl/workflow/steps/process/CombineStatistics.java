package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("combineStatistics")
public class CombineStatistics extends BaseWorkflowStep<CombineStatisticsConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CombineStatistics.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private Map<BusinessEntity, Table> statsTableMap = new HashMap<>();
    private String customerSpaceStr;
    private DataCollection.Version activeVersion;
    private DataCollection.Version inactiveVersion;

    @Override
    public void execute() {
        log.info("Inside CombineStatistics execute()");
        customerSpaceStr = configuration.getCustomerSpace().toString();
        activeVersion = dataCollectionProxy.getActiveVersion(customerSpaceStr);
        inactiveVersion = activeVersion.complement();

        Map<String, StatsCube> cubeMap = new HashMap<>();
        StatisticsContainer latestStatsContainer = dataCollectionProxy.getStats(customerSpaceStr, inactiveVersion);
        DataCollection.Version latestStatsVersion = null;
        if (latestStatsContainer == null) {
            latestStatsContainer = dataCollectionProxy.getStats(customerSpaceStr, activeVersion);
            if (latestStatsContainer != null) {
                latestStatsVersion = activeVersion;
            }
        } else {
            latestStatsVersion = inactiveVersion;
        }
        if (latestStatsContainer != null && MapUtils.isNotEmpty(latestStatsContainer.getStatsCubes())) {
            cubeMap.putAll(latestStatsContainer.getStatsCubes());
        }
        log.info("Found " + cubeMap.size() + " cubes in the stats in " + latestStatsVersion + " : " //
                + StringUtils.join(cubeMap.keySet(), ", "));

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
        if (MapUtils.isNotEmpty(statsTableMap)) {
            log.info("Upserting " + statsTableMap.size() + " cubes into stats.");
            statsTableMap.forEach((entity, table) -> cubeMap.put(entity.name(), getStatsCube(table, entity)));
        } else if (MapUtils.isEmpty(statsTableMap) && inactiveVersion.equals(latestStatsVersion)) {
            log.info("Nothing changes to inactive stats, finish the workflow step.");
            return;
        }

        StatisticsContainer statsContainer = new StatisticsContainer();
        statsContainer.setName(NamingUtils.timestamp("Stats"));
        statsContainer.setStatsCubes(cubeMap);
        statsContainer.setVersion(inactiveVersion);
        log.info("Saving stats with " + cubeMap.size() + " cubes.");
        dataCollectionProxy.upsertStats(customerSpaceStr, statsContainer);
    }

    @Override
    public void onExecutionCompleted() {
        statsTableMap.forEach((entity, table) -> {
            log.info("Drop stats table " + table.getName() + " for entity " + entity);
            metadataProxy.deleteTable(getConfiguration().getCustomerSpace().toString(), table.getName());
        });
    }

    private StatsCube getStatsCube(Table targetTable, BusinessEntity entity) {
        List<Extract> extracts = targetTable.getExtracts();
        List<String> paths = new ArrayList<>();
        for (Extract extract : extracts) {
            paths.add(extract.getPath());
        }
        log.info("Checking for result file: " + StringUtils.join(paths, ", "));
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, paths);
        StatsCube statsCube = StatsCubeUtils.parseAvro(records);
        if (BusinessEntity.PurchaseHistory.equals(entity)) {
            Map<String, AttributeStats> newStatsMap = new HashMap<>();
            statsCube.getStatistics().forEach((attrName, attrStats) -> {
                AttributeStats newStats = StatsCubeUtils.convertPurchaseHistoryStats(attrName, attrStats);
                if (newStats != null) {
                    newStatsMap.put(attrName, attrStats);
                }
            });
            statsCube.setStatistics(newStatsMap);
        }
        return statsCube;
    }

}
