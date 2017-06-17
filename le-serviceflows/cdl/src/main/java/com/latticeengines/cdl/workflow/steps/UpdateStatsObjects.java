package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
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

        log.info("Converting stats cube to statistics container.");
        // get StatsCube from statsTable
        StatsCube statsCube = getStatsCube(statsTable);
        String name = statsTable.getName();

        // get metadata for account master
        String latestVersion = columnMetadataProxy.latestVersion("").getVersion();
        List<ColumnMetadata> cols = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Segment,
                latestVersion);

        // get all other metadata from master table and matchapi
        String schemaIntStr = masterTable.getInterpretation();
        SchemaInterpretation masterTableType = null;
        if (StringUtils.isNotBlank(schemaIntStr)) {
            masterTableType = SchemaInterpretation.valueOf(masterTable.getInterpretation());
            log.info("SchemaInterpretation of master table is: " + masterTableType);
        }
        List<ColumnMetadata> masterCols = masterTable.getAttributes().stream().map(Attribute::getColumnMetadata)
                .collect(Collectors.toList());

        Pair<SchemaInterpretation, List<ColumnMetadata>> masterTablePair = new ImmutablePair<SchemaInterpretation, List<ColumnMetadata>>(
                masterTableType, masterCols);
        Pair<SchemaInterpretation, List<ColumnMetadata>> accountMasterPair = new ImmutablePair<SchemaInterpretation, List<ColumnMetadata>>(
                SchemaInterpretation.AccountMaster, cols);
        List<Pair<SchemaInterpretation, List<ColumnMetadata>>> interpretationToColMetadataList = new ArrayList<Pair<SchemaInterpretation, List<ColumnMetadata>>>();
        interpretationToColMetadataList.add(masterTablePair);
        interpretationToColMetadataList.add(accountMasterPair);

        return StatsCubeUtils.getStatisticsContainer(statsCube, interpretationToColMetadataList);
    }

}
