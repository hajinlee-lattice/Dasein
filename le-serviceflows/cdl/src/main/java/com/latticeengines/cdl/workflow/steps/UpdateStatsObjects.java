package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
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

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    private String statsTableName;

    @Override
    public void execute() {
        log.info("Inside UpdateStatsObjects execute()");
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        Map<BusinessEntity, Table> entityTableMap = getMapObjectFromContext(TABLE_GOING_TO_REDSHIFT, BusinessEntity.class, Table.class);
        Table masterTable = entityTableMap.get(BusinessEntity.Account);
        if (masterTable == null) {
            throw new NullPointerException("Master table for stats object calculation is not found.");
        }
        Table profileTable = dataCollectionProxy.getTable(customerSpaceStr, TableRoleInCollection.Profile);
        if (profileTable == null) {
            throw new NullPointerException("Profile table for stats object calculation is not found.");
        }

        statsTableName = getStringValueFromContext(CALCULATE_STATS_TARGET_TABLE);
        log.info(String.format("statsTableName for customer %s is %s", customerSpaceStr, statsTableName));
        Table statsTable = metadataProxy.getTable(customerSpaceStr, statsTableName);
        if (statsTable == null) {
            throw new NullPointerException("Target table for Stats Object Calculation is not found.");
        }

        StatisticsContainer statsContainer = constructStatsContainer(masterTable, statsTable);
        dataCollectionProxy.upsertStats(customerSpaceStr, statsContainer);
    }

    @Override
    public void onExecutionCompleted() {
        log.info("Drop stats table " + statsTableName);
        metadataProxy.deleteTable(getConfiguration().getCustomerSpace().toString(), statsTableName);
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
        log.info("Converting stats cube to statistics container.");
        List<ColumnMetadata> masterCols = masterTable.getColumnMetadata();
        // hard code entity
        List<Pair<BusinessEntity, List<ColumnMetadata>>> mdPairs = Collections.singletonList(
                ImmutablePair.of(BusinessEntity.Account, masterCols)
        );
        // get StatsCube from statsTable
        StatsCube statsCube = getStatsCube(statsTable);
        Statistics statistics = StatsCubeUtils.constructStatistics(statsCube, mdPairs);
        StatisticsContainer statsContainer = new StatisticsContainer();
        statsContainer.setStatistics(statistics);
        statsContainer.setName(statsTable.getName());
        return statsContainer;
    }

}
