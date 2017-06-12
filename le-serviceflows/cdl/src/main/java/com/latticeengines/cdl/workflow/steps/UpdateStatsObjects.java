package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.metadata.StatisticsContainerProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

import edu.emory.mathcs.backport.java.util.Arrays;

@Component("updateStatsObjects")
public class UpdateStatsObjects extends BaseWorkflowStep<UpdateStatsObjectsConfiguration> {

    private static final Log log = LogFactory.getLog(UpdateStatsObjects.class);

    private static final String TARGET_TABLE_SOURCE_PATTERN = "%s_%s";

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private StatisticsContainerProxy statisticsContainerProxy;

    @Override
    public void execute() {
        log.info("Inside UpdateStatsObjects execute()");
        String masterTableName = configuration.getMasterTableName();
        log.info(String.format("masterTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                masterTableName));
        Table masterTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), masterTableName);

        String targetTableName = getStringValueFromContext(CALCULATE_STATS_TARGET_TABLE);
        String version = getStringValueFromContext(CALCULATE_STATS_TRANSFORM_VERSION);
        String targetTableSourceName = String.format(TARGET_TABLE_SOURCE_PATTERN, targetTableName, version);
        log.info(String.format("targetTableSourceName for customer %s is %s",
                configuration.getCustomerSpace().toString(), targetTableSourceName));

        Table targetTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), targetTableSourceName);
        if (targetTable == null) {
            throw new NullPointerException("Target table for Stats Object Calculation is not found.");
        }

        StatisticsContainer statsContainer = constructStatsContainer(targetTable);
        statisticsContainerProxy.createOrUpdateStatistics(configuration.getCustomerSpace().toString(), statsContainer);
    }

    private StatsCube getStatsCube(Table targetTable) {
        List<Extract> extracts = targetTable.getExtracts();
        List<String> paths = new ArrayList<String>();
        for (Extract extract : extracts) {
            paths.add(extract.getPath());
        }
        log.info("Checking for result file: " + Arrays.toString(paths.toArray()));
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, paths.get(0));
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record is " + record);
        }
        StatsCube statsCube = StatsCubeUtils.parseAvro(records);
        return statsCube;
    }

    @VisibleForTesting
    StatisticsContainer constructStatsContainer(Table targetTable) {
        StatsCube statsCube = getStatsCube(targetTable);

        StatisticsContainer statsContainer = new StatisticsContainer();
        String schemaStr = targetTable.getInterpretation();
        log.info(String.format("schema Str for targetTable %s is %s", targetTable.getName(), schemaStr));
        SchemaInterpretation schemaInterpretation = SchemaInterpretation.valueOf(targetTable.getInterpretation());
        Map<String, AttributeStats> attributeStatsMap = statsCube.getStatistics();

        Statistics statistics = new Statistics();
        for (String name : attributeStatsMap.keySet()) {
            ColumnLookup columnLookup = new ColumnLookup(schemaInterpretation, name);
            Attribute attributeInMetadataTable = targetTable.getAttribute(name);
            if (attributeInMetadataTable == null) {
                log.warn(String.format("Attribute %s in StatsCube does not exist in the metadata table %s", name,
                        targetTable.getName()));
                continue;
            }
            String category = attributeInMetadataTable.getCategory();
            String subCategory = attributeInMetadataTable.getSubcategory();
            if (category == null || subCategory == null) {
                log.warn(String.format("Category is % and SubCategory is %s", category, subCategory));
                continue;
            }
            AttributeStatistics attributeStatistics = new AttributeStatistics();
            attributeStatistics.getBuckets().addAll(attributeStatsMap.get(name).getBuckets().getBucketList());
            if (statistics.getCategories().containsKey(category)) {
                CategoryStatistics categoryStatistics = statistics.getCategories().get(category);
                if (categoryStatistics.getSubcategories().containsKey(subCategory)) {
                    categoryStatistics.getSubcategories().get(subCategory).getAttributes().put(columnLookup,
                            attributeStatistics);
                } else {
                    SubcategoryStatistics subCategoryStatistics = new SubcategoryStatistics();
                    subCategoryStatistics.getAttributes().put(columnLookup, attributeStatistics);
                    categoryStatistics.getSubcategories().put(subCategory, subCategoryStatistics);
                }
            } else {
                CategoryStatistics categoryStatistics = new CategoryStatistics();
                SubcategoryStatistics subCategoryStatistics = new SubcategoryStatistics();
                subCategoryStatistics.getAttributes().put(columnLookup, attributeStatistics);
                categoryStatistics.getSubcategories().put(subCategory, subCategoryStatistics);
                statistics.getCategories().put(category, categoryStatistics);
            }
        }
        statsContainer.setStatistics(statistics);

        return statsContainer;
    }

}
