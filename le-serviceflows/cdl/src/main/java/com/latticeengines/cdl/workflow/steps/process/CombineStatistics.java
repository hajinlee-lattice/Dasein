package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.AvroUtils.AvroFilesIterator;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.CombineStatisticsConfiguration;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("combineStatistics")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CombineStatistics extends BaseWorkflowStep<CombineStatisticsConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CombineStatistics.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private Map<BusinessEntity, Table> statsTableMap;
    private String customerSpaceStr;

    @Override
    public void execute() {
        log.info("Inside CombineStatistics execute()");
        customerSpaceStr = configuration.getCustomerSpace().toString();
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpaceStr);
        DataCollection.Version inactiveVersion = activeVersion.complement();

        Set<BusinessEntity> resetEntities = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        if (resetEntities == null) {
            resetEntities = Collections.emptySet();
        }

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

        int originalCubes = cubeMap.size();
        String msg = "Found " + originalCubes + " cubes in the stats in " + latestStatsVersion;
        if (MapUtils.isNotEmpty(cubeMap)) {
            msg += " : " + StringUtils.join(cubeMap.keySet(), ", ");
        }
        log.info(msg);

        resetEntities.forEach(entity -> {
            String key = entity.name();
            if (cubeMap.containsKey(key)) {
                cubeMap.remove(key);
            }
        });
        if (cubeMap.size() != originalCubes) {
            log.info("Removed " + (originalCubes - cubeMap.size()) + " cubes due to entity reset.");
        }

        Map<BusinessEntity, String> statsTableNames = getMapObjectFromContext(STATS_TABLE_NAMES, BusinessEntity.class,
                String.class);
        statsTableMap = new HashMap<>();
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
            int originalStatsTables = statsTableMap.size();
            resetEntities.forEach(entity -> {
                if (statsTableMap.containsKey(entity)) {
                    statsTableMap.remove(entity);
                }
            });
            if (statsTableMap.size() != originalStatsTables) {
                log.info("Removed " + (originalStatsTables - statsTableMap.size())
                        + " stats tables due to entity reset.");
            }
        }

        if (MapUtils.isNotEmpty(statsTableMap)) {
            log.info("Upserting " + statsTableMap.size() + " cubes into stats.");
            statsTableMap.forEach((entity, table) -> cubeMap.put(entity.name(), getStatsCube(table, entity)));
        } else if (MapUtils.isEmpty(statsTableMap) && inactiveVersion.equals(latestStatsVersion)) {
            log.info("Nothing changes to inactive stats, finish the workflow step.");
            return;
        }

        if (MapUtils.isNotEmpty(cubeMap)) {
            StatisticsContainer statsContainer = new StatisticsContainer();
            statsContainer.setName(NamingUtils.timestamp("Stats"));
            statsContainer.setStatsCubes(cubeMap);
            statsContainer.setVersion(inactiveVersion);
            log.info("Saving stats with " + cubeMap.size() + " cubes.");
            dataCollectionProxy.upsertStats(customerSpaceStr, statsContainer);
        } else {
            log.info("Skip saving an empty stats.");
        }
    }

    @Override
    public void onExecutionCompleted() {
        statsTableMap.forEach((entity, table) -> {
            log.info("Drop stats table " + table.getName() + " for entity " + entity);
            addToListInContext(TEMPORARY_CDL_TABLES, table.getName(), String.class);
        });
    }

    private StatsCube getStatsCube(Table targetTable, BusinessEntity entity) {
        List<Extract> extracts = targetTable.getExtracts();
        List<String> paths = new ArrayList<>();
        for (Extract extract : extracts) {
            paths.add(extract.getPath());
        }
        log.info("Checking for result file: " + StringUtils.join(paths, ", "));
        try (AvroFilesIterator records = AvroUtils.iterateAvroFiles(yarnConfiguration, paths.toArray(new String[]{}))) {
            StatsCube statsCube = StatsCubeUtils.parseAvro(records);
            if (BusinessEntity.PurchaseHistory.equals(entity)) {
                processPHCube(statsCube);
            }
            if (BusinessEntity.Rating.equals(entity)) {
                processRatingCube(statsCube);
            }
            return statsCube;
        }
    }

    private void processPHCube(StatsCube statsCube) {
        Map<String, AttributeStats> newStatsMap = new HashMap<>();
        statsCube.getStatistics().forEach((attrName, attrStats) -> {
            AttributeStats newStats = StatsCubeUtils.convertPurchaseHistoryStats(attrName, attrStats);
            if (newStats != null) {
                newStatsMap.put(attrName, attrStats);
            }
        });
        statsCube.setStatistics(newStatsMap);
    }

    private void processRatingCube(StatsCube statsCube) {
        @SuppressWarnings("rawtypes")
        Map<String, Map> liftMap = getMapObjectFromContext(RATING_LIFTS, String.class, Map.class);
        if (MapUtils.isNotEmpty(liftMap)) {
            statsCube.getStatistics().forEach((attrName, attrStats) -> {
                if (isRatingAttr(attrName)) {
                    StatsCubeUtils.sortRatingBuckets(attrStats);
                    log.info("Sorted rating buckets for " + attrName + " : " + JsonUtils.serialize(attrStats));
                }
                if (liftMap.containsKey(attrName)) {
                    Map<String, Double> lifts = JsonUtils.convertMap(liftMap.get(attrName), String.class, Double.class);
                    StatsCubeUtils.addLift(attrStats, lifts);
                }
            });
        }
    }

    private boolean isRatingAttr(String attrName) {
        return attrName.startsWith(RatingEngine.RATING_ENGINE_PREFIX + "_") && !hasRatingAttrSuffix(attrName);
    }

    private boolean hasRatingAttrSuffix(String attrName) {
        return RatingEngine.SCORE_ATTR_SUFFIX.values().stream().map(s -> "_" + s).anyMatch(attrName::endsWith);
    }

}
