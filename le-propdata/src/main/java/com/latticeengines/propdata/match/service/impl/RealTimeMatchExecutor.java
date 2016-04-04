package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyDimension;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.metric.MatchedAccount;
import com.latticeengines.propdata.match.metric.MatchedColumn;
import com.latticeengines.propdata.match.metric.RealTimeResponse;
import com.latticeengines.propdata.match.service.MatchExecutor;

@Component("realTimeMatchExecutor")
class RealTimeMatchExecutor extends MatchExecutorBase implements MatchExecutor {

    private static final Log log = LogFactory.getLog(RealTimeMatchExecutor.class);

    @Autowired
    private MetricService metricService;

    @Override
    @MatchStep
    public MatchContext execute(MatchContext matchContext) {
        matchContext = fetch(matchContext);
        matchContext = complete(matchContext);
        matchContext = appendMetadataToContext(matchContext);
        generateOutputMetrics(matchContext);
        return matchContext;
    }

    public MatchContext appendMetadataToContext(MatchContext matchContext) {
        ColumnSelection.Predefined selection = matchContext.getInput().getPredefinedSelection();
        matchContext.setOutput(appendMetadata(matchContext.getOutput(), selection));
        return matchContext;
    }

    @MatchStep
    private void generateOutputMetrics(MatchContext matchContext) {
        MatchInput input = matchContext.getInput();

        RealTimeResponse response = new RealTimeResponse(matchContext);
        metricService.write(MetricDB.LDC_Match, response);

        List<MatchedAccount> accountMeasurements = new ArrayList<>();
        List<MatchedColumn> columnMeasurements = new ArrayList<>();
        List<InternalOutputRecord> recordList = matchContext.getInternalResults();
        List<String> outputFields = matchContext.getOutput().getOutputFields();
        for (InternalOutputRecord record : recordList) {
            if (record.isFailed()) { continue; }
            MatchKeyDimension keyDimension =
                    new MatchKeyDimension(record.getParsedDomain(), record.getParsedNameLocation());
            MatchedAccount measurement = new MatchedAccount(input, keyDimension, matchContext.getMatchEngine(),
                    record.isMatched());
            accountMeasurements.add(measurement);

            for (int i = 0; i < outputFields.size(); i++) {
                String outputField = outputFields.get(i);
                Boolean matched = record.getColumnMatched().get(i);
                MatchedColumn matchedColumn = new MatchedColumn(matched, outputField, input, keyDimension,
                        matchContext.getMatchEngine());
                columnMeasurements.add(matchedColumn);
            }

        }

        metricService.write(MetricDB.LDC_Match, accountMeasurements);
        metricService.write(MetricDB.LDC_Match, columnMeasurements);
    }


}
