package com.latticeengines.datacloud.match.actors.visitor;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;

public class MatchTraveler extends Traveler implements Fact, Dimension {
    private final MatchKeyTuple matchKeyTuple;
    private String dataCloudVersion;
    private String decisionGraph;
    private String lastStop;
    private Double totalTravelTime;

    private List<DnBMatchContext> dnBMatchContexts = new ArrayList<>();

    private Boolean isMatched = false;
    private Boolean isProcessed = false;

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

    public void setDataCloudVersion(String dataCloudVersion) {
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

    @Override
    public void start() {
        super.start();
        debug("Has " + getMatchKeyTuple() + " to begin with.");
    }
}
