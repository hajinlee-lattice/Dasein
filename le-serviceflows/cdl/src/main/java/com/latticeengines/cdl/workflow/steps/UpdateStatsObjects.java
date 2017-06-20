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
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.CategoryStatistics;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.SubcategoryStatistics;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.UpdateStatsObjectsConfiguration;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("updateStatsObjects")
public class UpdateStatsObjects extends BaseWorkflowStep<UpdateStatsObjectsConfiguration> {

    private static final Log log = LogFactory.getLog(UpdateStatsObjects.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Override
    public void execute() {
        log.info("Inside UpdateStatsObjects execute()");
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        String collectionName = configuration.getDataCollectionName();
        Table masterTable = dataCollectionProxy.getTable(customerSpaceStr, collectionName,
                TableRoleInCollection.ConsolidatedAccount);
        if (masterTable == null) {
            throw new NullPointerException("Master table for Stats Object Calculation is not found.");
        }
        Table profileTable = dataCollectionProxy.getTable(customerSpaceStr, collectionName,
                TableRoleInCollection.Profile);
        if (profileTable == null) {
            throw new NullPointerException("Profile table for Stats Object Calculation is not found.");
        }

        String statsTableName = getStringValueFromContext(CALCULATE_STATS_TARGET_TABLE);
        log.info(String.format("statsTableName for customer %s is %s", customerSpaceStr, statsTableName));
        Table statsTable = metadataProxy.getTable(customerSpaceStr, statsTableName);
        if (statsTable == null) {
            throw new NullPointerException("Target table for Stats Object Calculation is not found.");
        }

        StatisticsContainer statsContainer = constructStatsContainer(masterTable, statsTable);
        dataCollectionProxy.upsertStats(customerSpaceStr, collectionName, statsContainer);
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

    private StatisticsContainer constructStatsContainer(Table masterTable, Table statsTable) {
        // hard code entity
        BusinessEntity master = BusinessEntity.Account;
        BusinessEntity am = BusinessEntity.LatticeAccount;

        StatisticsContainer statsContainer = new StatisticsContainer();
        statsContainer.setName(statsTable.getName());

        // get StatsCube from statsTable
        StatsCube statsCube = getStatsCube(statsTable);

        log.info("Converting stats cube to statistics container.");
        // get metadata for account master
        String latestVersion = columnMetadataProxy.latestVersion("").getVersion();
        List<ColumnMetadata> cols = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Segment,
                latestVersion);
        Map<String, ColumnMetadata> colLookup = new HashMap<>();
        cols.forEach(c -> colLookup.put(c.getColumnId(), c));

        // get all other metadata from master table and matchapi
        Map<String, AttributeStats> attributeStatsMap = statsCube.getStatistics();
        Statistics statistics = new Statistics();
        statistics.setCount(statsCube.getCount());
        for (String name : attributeStatsMap.keySet()) {
            AttributeLookup attrLookup;
            Category category;
            String subCategory;
            Attribute attrInMasterTable = masterTable.getAttribute(name);
            if (attrInMasterTable != null) {
                // an attribute from master table
                attrLookup = new AttributeLookup(master, name);
                category = StringUtils.isBlank(attrInMasterTable.getCategory()) ? null
                        : Category.fromName(attrInMasterTable.getCategory());
                subCategory = attrInMasterTable.getSubcategory();
            } else if (colLookup.containsKey(name)) {
                attrLookup = new AttributeLookup(am, name);
                ColumnMetadata metadata = colLookup.get(name);
                category = metadata.getCategory();
                subCategory = metadata.getSubcategory();
            } else {
                log.warn(String.format(
                        "Attribute %s in StatsCube does not exist in account master or the customer master table %s",
                        name, statsTable.getName()));
                continue;
            }
            if (category == null) {
                category = Category.DEFAULT;
            }
            if (subCategory == null) {
                subCategory = "Other";
            }

            AttributeStats statsInCube = attributeStatsMap.get(name);
            // create map entries if not there
            if (!statistics.hasCategory(category)) {
                statistics.putCategory(category, new CategoryStatistics());
            }
            CategoryStatistics categoryStatistics = statistics.getCategory(category);
            if (!categoryStatistics.hasSubcategory(subCategory)) {
                categoryStatistics.putSubcategory(subCategory, new SubcategoryStatistics());
            }
            // update the corresponding map entry
            SubcategoryStatistics subcategoryStatistics = statistics.getCategory(category).getSubcategory(subCategory);
            subcategoryStatistics.putAttrStats(attrLookup, statsInCube);
        }
        statsContainer.setStatistics(statistics);

        return statsContainer;
    }

}
