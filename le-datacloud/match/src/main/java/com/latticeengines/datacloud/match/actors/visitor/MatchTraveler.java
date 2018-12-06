package com.latticeengines.datacloud.match.actors.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

import akka.util.Timeout;

public class MatchTraveler extends Traveler implements Fact, Dimension {
    private final MatchKeyTuple matchKeyTuple;
    private String dataCloudVersion;
    private String decisionGraph;
    private String lastStop;

    private MatchInput matchInput;

    private List<DnBMatchContext> dnBMatchContexts = new ArrayList<>();

    private Boolean isMatched = false;
    // When the 1st time anchor receives the traveler, isProcessed will be set
    // as true
    private Boolean isProcessed = false;

    // only for metric purpose
    private Double totalTravelTime;
    private Boolean isBatchMode = false;

    private Timeout travelTimeout;

    private Map<String, String> dunsOriginMap;

    public MatchTraveler(String rootOperationUid, MatchKeyTuple matchKeyTuple) {
        super(rootOperationUid);
        this.matchKeyTuple = matchKeyTuple;
        this.start();
    }

    @Override
    protected Object getInputData() {
        return matchKeyTuple;
    }

    @Override
    @MetricField(name = "RootOperationUID")
    public String getRootOperationUid() {
        return super.getRootOperationUid();
    }

    public MatchInput getMatchInput() {
        return matchInput;
    }

    public void setMatchInput(MatchInput matchInput) {
        this.matchInput = matchInput;
        if (StringUtils.isNotEmpty(matchInput.getDecisionGraph())) {
            setDecisionGraph(matchInput.getDecisionGraph());
        }
        setLogLevel(matchInput.getLogLevelEnum());
        setDataCloudVersion(matchInput.getDataCloudVersion());
    }

    @MetricField(name = "Matched", fieldType = MetricField.FieldType.BOOLEAN)
    public Boolean isMatched() {
        return isMatched;
    }

    public void setMatched(Boolean isMatched) {
        this.isMatched = isMatched;
    }

    public Boolean isProcessed() {
        return isProcessed;
    }

    public void setProcessed(Boolean processed) {
        isProcessed = processed;
    }

    @MetricTag(tag = "DecisionGraph")
    public String getDecisionGraph() {
        return decisionGraph;
    }

    public void setDecisionGraph(String decisionGraph) {
        this.decisionGraph = decisionGraph;
    }

    @MetricFieldGroup
    public MatchKeyTuple getMatchKeyTuple() {
        return matchKeyTuple;
    }

    public List<DnBMatchContext> getDnBMatchContexts() {
        return dnBMatchContexts;
    }

    public void appendDnBMatchContext(DnBMatchContext dnBMatchContext) {
        this.dnBMatchContexts.add(dnBMatchContext);
    }

    @MetricField(name = "DataCloudVersion")
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    private void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    @MetricField(name = "LatticeAccountId")
    public String getLatticeAccountId() {
        return (String) getResult();
    }

    @MetricTag(tag = "LastStop")
    public String getLastStop() {
        return lastStop;
    }

    public void setLastStop(String lastStop) {
        this.lastStop = lastStop;
    }

    @MetricField(name = "TravelTime", fieldType = MetricField.FieldType.DOUBLE)
    public Double getTotalTravelTime() {
        return totalTravelTime;
    }

    public void recordTotalTime() {
        this.totalTravelTime = age().doubleValue();
    }

    @MetricTag(tag = "Mode")
    public String getMode() {
        return isBatchMode ? "Batch" : "RealTime";
    }

    public void setBatchMode(Boolean batchMode) {
        isBatchMode = batchMode;
    }

    public Map<String, String> getDunsOriginMap() {
        return dunsOriginMap;
    }

    public void setDunsOriginMap(Map<String, String> dunsOriginMap) {
        this.dunsOriginMap = dunsOriginMap;
    }

    public void setDunsOriginMapIfAbsent(Map<String, String> dunsOriginMap) {
        if (this.dunsOriginMap == null) {
            this.dunsOriginMap = dunsOriginMap;
        }
    }

    public Timeout getTravelTimeout() {
        return travelTimeout;
    }

    public void setTravelTimeout(Timeout travelTimeout) {
        this.travelTimeout = travelTimeout;
    }

    @Override
    public void start() {
        super.start();
        debug("Has " + getMatchKeyTuple() + " to begin with.");
    }

}
