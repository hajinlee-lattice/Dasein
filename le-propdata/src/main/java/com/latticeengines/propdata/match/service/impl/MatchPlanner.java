package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatistics;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.propdata.core.service.ZkConfigurationService;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.metric.RealTimeRequest;
import com.latticeengines.propdata.match.service.ColumnSelectionService;

@Component
class MatchPlanner {

    @Autowired
    private ColumnSelectionService columnSelectionService;

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Autowired
    private MetricService metricService;

    @MatchStep
    MatchContext planForRealTime(MatchInput input) {
        MatchContext context = validateMatchInput(input);
        context.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        context = generateInputMetric(context);
        context = scanInputData(input, context);
        context = sketchExecutionPlan(context);
        return context;
    }

    @MatchStep
    private MatchContext validateMatchInput(MatchInput input) {
        MatchInputValidator.validateRealTimeInput(input, zkConfigurationService.maxRealTimeInput());
        MatchContext context = new MatchContext();
        context.setStatus(MatchStatus.NEW);
        context.setInput(input);
        MatchOutput output = initializeMatchOutput(input);
        context.setOutput(output);
        return context;
    }

    @MatchStep
    private static MatchContext scanInputData(MatchInput input, MatchContext context) {
        Map<MatchKey, Integer> keyPositionMap = getKeyPositionMap(input);

        List<InternalOutputRecord> records = new ArrayList<>();
        Set<String> domainSet = new HashSet<>();

        for (int i = 0; i < input.getData().size(); i++) {
            InternalOutputRecord record = scanInputRecordAndUpdateKeySets(input.getData().get(i), i,
                    input.getFields().size(), keyPositionMap, domainSet);
            record.setColumnMatched(new ArrayList<Boolean>());
            records.add(record);
        }

        context.setInternalResults(records);
        context.setDomains(domainSet);
        return context;
    }

    @MatchStep
    private MatchContext generateInputMetric(MatchContext context) {
        MatchInput input = context.getInput();
        Integer selectedCols = null;
        if (input.getPredefinedSelection() != null) {
            selectedCols = columnSelectionService.getTargetColumns(input.getPredefinedSelection()).size();
        }

        RealTimeRequest request = new RealTimeRequest(input, context.getMatchEngine(), selectedCols);
        metricService.write(MetricDB.LDC_Match, request);

        return context;
    }

    @MatchStep
    private MatchContext sketchExecutionPlan(MatchContext matchContext) {
        ColumnSelection.Predefined predefined = matchContext.getInput().getPredefinedSelection();
        if (predefined != null) {
            matchContext.setSourceColumnsMap(columnSelectionService.getSourceColumnMap(predefined));
            matchContext.setColumnPriorityMap(columnSelectionService.getColumnPriorityMap(predefined));
            matchContext.getOutput().setOutputFields(columnSelectionService.getTargetColumns(predefined));
        }
        return matchContext;
    }

    private static MatchOutput initializeMatchOutput(MatchInput input) {
        MatchOutput output = new MatchOutput();
        output.setReceivedAt(new Date());
        output.setInputFields(input.getFields());
        output.setKeyMap(input.getKeyMap());
        output.setSubmittedBy(input.getTenant());
        MatchStatistics statistics = initializeStatistics(input);
        output.setStatistics(statistics);
        return output;
    }

    private static MatchStatistics initializeStatistics(MatchInput input) {
        MatchStatistics statistics = new MatchStatistics();
        statistics.setRowsRequested(input.getData().size());
        return statistics;
    }

    private static InternalOutputRecord scanInputRecordAndUpdateKeySets(List<Object> inputRecord, int rowNum,
            int numInputFields, Map<MatchKey, Integer> keyPositionMap, Set<String> domainSet) {
        InternalOutputRecord record = new InternalOutputRecord();
        record.setRowNumber(rowNum);
        record.setMatched(false);
        record.setInput(inputRecord);

        int domainPos = keyPositionMap.containsKey(MatchKey.Domain) ? keyPositionMap.get(MatchKey.Domain) : -1;

        if (inputRecord.size() != numInputFields) {
            record.setErrorMessages(Collections.singletonList("The number of objects in this row [" + inputRecord.size()
                    + "] does not match the number of fields claimed [" + numInputFields + "]"));
        } else if (domainPos >= 0) {
            try {
                String originalDomain = (String) inputRecord.get(domainPos);
                String cleanDomain = DomainUtils.parseDomain(originalDomain);
                record.setParsedDomain(cleanDomain);

                // update domain set
                domainSet.add(cleanDomain);
            } catch (Exception e) {
                record.setErrorMessages(
                        Collections.singletonList("Error when cleanup domain field: " + e.getMessage()));
            }
        }

        return record;
    }

    private static Map<MatchKey, Integer> getKeyPositionMap(MatchInput input) {
        Map<MatchKey, Integer> posMap = new HashMap<>();
        for (int pos = 0; pos < input.getFields().size(); pos++) {
            String field = input.getFields().get(pos);
            for (MatchKey key : input.getKeyMap().keySet()) {
                if (field.equalsIgnoreCase(input.getKeyMap().get(key))) {
                    posMap.put(key, pos);
                }
            }
        }
        return posMap;
    }

}
