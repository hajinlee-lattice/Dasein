package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Collection;
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
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.query.AttributeLookup;
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

    @Override
    public void execute() {
        log.info("Inside CombineStatistics execute()");
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        Map<BusinessEntity, String> entityTableNames = getMapObjectFromContext(SERVING_STORE_IN_STATS,
                BusinessEntity.class, String.class);
        Map<BusinessEntity, Table> entityTableMap = new HashMap<>();
        entityTableNames.forEach((entity, tableName) -> {
            Table table = metadataProxy.getTable(customerSpaceStr, tableName);
            if (table == null) {
                throw new IllegalStateException(
                        "Serving store " + tableName + " for entity " + entity + " cannot be found.");
            }
            entityTableMap.put(entity, table);
        });
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpaceStr);
        DataCollection.Version inactiveVersion = activeVersion.complement();
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
        Map<BusinessEntity, StatsCube> cubeMap = new HashMap<>();
        StatisticsContainer activeStatsContainer = dataCollectionProxy.getStats(customerSpaceStr);
        Statistics activeStats = null;
        if (activeStatsContainer != null) {
            activeStats = removeEntities(activeStatsContainer.getStatistics(), entityTableMap.keySet());
        }
        statsTableMap.forEach((entity, table) -> cubeMap.put(entity, getStatsCube(table)));
        StatisticsContainer statsContainer = constructStatsContainer(entityTableMap, cubeMap, activeStats);
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
            Map<BusinessEntity, StatsCube> cubeMap, Statistics activeStats) {
        log.info("Converting stats cube to statistics container.");
        // hard code entity
        Map<BusinessEntity, List<ColumnMetadata>> mdMap = new HashMap<>();
        entityTableMap.forEach((entity, table) -> mdMap.put(entity, table.getColumnMetadata()));
        Statistics statistics = StatsCubeUtils.constructStatistics(cubeMap, mdMap);
        if (activeStats != null) {
            statistics = merge(statistics, activeStats);
        }
        StatisticsContainer statsContainer = new StatisticsContainer();
        statsContainer.setStatistics(statistics);
        statsContainer.setName(NamingUtils.timestamp("Stats"));
        return statsContainer;
    }

    private Statistics merge(Statistics stats1, Statistics stats2) {
        Map<Category, CategoryStatistics> catStatsMap = new HashMap<>();
        stats1.getCategories().forEach((cat, catStats1) -> {
            CategoryStatistics catStats2 = stats2.getCategory(cat);
            if (catStats2 != null) {
                CategoryStatistics catStats = merge(catStats1, catStats2);
                catStatsMap.put(cat, catStats);
            } else {
                catStatsMap.put(cat, catStats1);
            }
        });
        stats2.getCategories().forEach((cat, catStats2) -> {
            if (!catStatsMap.containsKey(cat)) {
                catStatsMap.put(cat, catStats2);
            }
        });
        Statistics stats = new Statistics();
        stats.setCategories(catStatsMap);
        return stats;
    }

    private CategoryStatistics merge(CategoryStatistics catStats1, CategoryStatistics catStats2) {
        Map<String, SubcategoryStatistics> subcatStatsMap = new HashMap<>();
        catStats1.getSubcategories().forEach((subcat1, subcatStats1) ->{
            SubcategoryStatistics subcatStats2 = catStats2.getSubcategory(subcat1);
            if (subcatStats2 != null) {
                SubcategoryStatistics subcatStats = merge(subcatStats1, subcatStats2);
                subcatStatsMap.put(subcat1, subcatStats);
            } else {
                subcatStatsMap.put(subcat1, subcatStats1);
            }
        });
        catStats2.getSubcategories().forEach((subcat2, subcatStats2) -> {
            if (!subcatStatsMap.containsKey(subcat2)) {
                subcatStatsMap.put(subcat2, subcatStats2);
            }
        });
        CategoryStatistics catStats = new CategoryStatistics();
        catStats.setSubcategories(subcatStatsMap);
        return catStats;
    }

    private SubcategoryStatistics merge(SubcategoryStatistics subcatStats1, SubcategoryStatistics subcatStats2) {
        Map<AttributeLookup, AttributeStats> attributeStatsMap = new HashMap<>();
        attributeStatsMap.putAll(subcatStats1.getAttributes());
        attributeStatsMap.putAll(subcatStats2.getAttributes());
        SubcategoryStatistics subcatStats = new SubcategoryStatistics();
        subcatStats.setAttributes(attributeStatsMap);
        return subcatStats;
    }

    private Statistics removeEntities(Statistics statistics, Collection<BusinessEntity> entities) {
        Map<Category, CategoryStatistics> catStatsMap = new HashMap<>();
        statistics.getCategories().forEach((cat, catStats) -> {
            CategoryStatistics newStats = removeEntities(catStats, entities);
            if (newStats != null) {
                catStatsMap.put(cat, newStats);
            }
        });
        if (MapUtils.isNotEmpty(catStatsMap)) {
            statistics.setCategories(catStatsMap);
            return statistics;
        } else {
            return null;
        }
    }

    private CategoryStatistics removeEntities(CategoryStatistics catStats, Collection<BusinessEntity> entities) {
        Map<String, SubcategoryStatistics> subcatStatsMap = new HashMap<>();
        catStats.getSubcategories().forEach((subcat, subcatStats) -> {
            SubcategoryStatistics newStats = removeEntities(subcatStats, entities);
            if (newStats != null) {
                subcatStatsMap.put(subcat, newStats);
            }
        });
        if (MapUtils.isNotEmpty(subcatStatsMap)) {
            catStats.setSubcategories(subcatStatsMap);
            return catStats;
        } else {
            return null;
        }
    }

    private SubcategoryStatistics removeEntities(SubcategoryStatistics subcatStats, Collection<BusinessEntity> entities) {
        Map<AttributeLookup, AttributeStats> attributeStatsMap = new HashMap<>();
        subcatStats.getAttributes().forEach((lookup, stats) -> {
            if (!entities.contains(lookup.getEntity())) {
                attributeStatsMap.put(lookup, stats);
            }
        });
        if (MapUtils.isNotEmpty(attributeStatsMap)) {
            subcatStats.setAttributes(attributeStatsMap);
            return subcatStats;
        } else {
            return null;
        }
    }

}
