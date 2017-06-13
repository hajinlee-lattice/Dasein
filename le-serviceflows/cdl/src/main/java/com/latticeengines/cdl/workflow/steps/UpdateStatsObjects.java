package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.UpdateStatsObjectsConfiguration;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.metadata.StatisticsContainerProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

import edu.emory.mathcs.backport.java.util.Arrays;

@Component("updateStatsObjects")
public class UpdateStatsObjects extends BaseWorkflowStep<UpdateStatsObjectsConfiguration> {

    private static final Log log = LogFactory.getLog(UpdateStatsObjects.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private StatisticsContainerProxy statisticsContainerProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Override
    public void execute() {
        log.info("Inside UpdateStatsObjects execute()");
        DataCollectionType dataCollectionType = configuration.getDataCollectionType();
        DataCollection dataCollection = dataCollectionProxy
                .getDataCollectionByType(configuration.getCustomerSpace().toString(), dataCollectionType);
        Table masterTable = CDLWorkflowStepUtils.getMasterTable(dataCollection);
        if (masterTable == null) {
            throw new NullPointerException("Master table for Stats Object Calculation is not found.");
        }
        Table profileTable = CDLWorkflowStepUtils.getProfileTable(dataCollection);
        if (profileTable == null) {
            throw new NullPointerException("Profile table for Stats Object Calculation is not found.");
        }

        String statsTableName = getStringValueFromContext(CALCULATE_STATS_TARGET_TABLE);
        log.info(String.format("statsTableName for customer %s is %s", configuration.getCustomerSpace().toString(),
                statsTableName));
        Table statsTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), statsTableName);
        if (statsTable == null) {
            throw new NullPointerException("Target table for Stats Object Calculation is not found.");
        }

        StatisticsContainer statsContainer = constructStatsContainer(masterTable, statsTable);
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
        return StatsCubeUtils.parseAvro(records);
    }

    private StatisticsContainer constructStatsContainer(Table masterTable, Table statsTable) {
        StatisticsContainer statsContainer = new StatisticsContainer();

        // get StatsCube from statsTable
        StatsCube statsCube = getStatsCube(statsTable);

        // get metadata for account master
        String latestVersion = columnMetadataProxy.latestVersion("").getVersion();
        List<ColumnMetadata> cols = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Segment,
                latestVersion);
        Map<String, ColumnMetadata> colLookup = new HashMap<>();
        cols.forEach(c -> colLookup.put(c.getColumnId(), c));

        // get all other metadata from master table (and matchapi if necessary)
        String schemaIntStr = masterTable.getInterpretation();
        SchemaInterpretation masterTableType = null;
        if (StringUtils.isNotBlank(schemaIntStr)) {
            masterTableType = SchemaInterpretation.valueOf(masterTable.getInterpretation());
            log.info("SchemaInterpretation of master table is: " + masterTableType);
        }
        Map<String, AttributeStats> attributeStatsMap = statsCube.getStatistics();

        Statistics statistics = new Statistics();
        for (String name : attributeStatsMap.keySet()) {
            ColumnLookup columnLookup = new ColumnLookup(name);
            String category;
            String subCategory;

            Attribute attrInMasterTable = masterTable.getAttribute(name);
            if (attrInMasterTable != null) {
                // an attribute from master table
                if (masterTableType != null) {
                    columnLookup = new ColumnLookup(masterTableType, name);
                }
                category = attrInMasterTable.getCategory();
                subCategory = attrInMasterTable.getSubcategory();
            } else if (colLookup.containsKey(name)) {
                ColumnMetadata metadata = colLookup.get(name);
                category = metadata.getCategory().getName();
                subCategory = metadata.getSubcategory();
            } else {
                log.warn(String.format(
                        "Attribute %s in StatsCube does not exist in account master or the customer master table %s",
                        name, statsTable.getName()));
                continue;
            }

            if (category == null || subCategory == null) {
                log.warn(String.format("Category is %s and SubCategory is %s", category, subCategory));
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
