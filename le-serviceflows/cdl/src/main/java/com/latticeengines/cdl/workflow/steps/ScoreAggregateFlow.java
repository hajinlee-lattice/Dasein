package com.latticeengines.cdl.workflow.steps;

import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.ScoreAggregateParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ScoreAggregateFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("scoreAggregateFlow")
public class ScoreAggregateFlow extends RunDataFlow<ScoreAggregateFlowConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(ScoreAggregateFlow.class);

    @Override
    public void execute() {
        setupDataFlow();
        super.execute();
    }

    @Override
    public void onExecutionCompleted() {
        Table aggrTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(),
                configuration.getTargetTableName());
        List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration,
                aggrTable.getExtracts().get(0).getPath() + "/*.avro");
        log.info(
                "Table= " + configuration.getTargetTableName() + " Average Score="
                        + records.get(0).get("AverageScore"));
        putDoubleValueInContext(SCORING_AVG_SCORE, (Double) records.get(0).get("AverageScore"));

    }

    private void setupDataFlow() {
        ScoreAggregateParameters params = new ScoreAggregateParameters();
        params.setScoreResultsTableName(getScoreResultTableName());
        configuration.setDataFlowParams(params);
    }

    private String getScoreResultTableName() {
        String scoreResultTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        if (scoreResultTableName == null) {
            scoreResultTableName = getDataFlowParams().getScoreResultsTableName();
        }
        return scoreResultTableName;
    }

    private ScoreAggregateParameters getDataFlowParams() {
        return (ScoreAggregateParameters) configuration.getDataFlowParams();
    }

}
