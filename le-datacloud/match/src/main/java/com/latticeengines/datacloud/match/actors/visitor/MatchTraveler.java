package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchOutput;

public class MatchTraveler extends Traveler implements Fact, Dimension {
    private MatchKeyTuple matchKeyTuple;
    private String dataCloudVersion;
    private String decisionGraph;
    private String lastStop;

    private DnBMatchOutput dnBMatchOutput;

    private Boolean isMatched = false;
    private Boolean isProcessed = false;

    public MatchTraveler(String rootOperationUid) {
        super(rootOperationUid);
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

    public void setMatchKeyTuple(MatchKeyTuple matchKeyTuple) {
        this.matchKeyTuple = matchKeyTuple;
    }

    public DnBMatchOutput getDnBMatchOutput() {
        return dnBMatchOutput;
    }

    public void setDnBMatchOutput(DnBMatchOutput dnBMatchOutput) {
        this.dnBMatchOutput = dnBMatchOutput;
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

    @Override
    public String toString() {
        return String.format("MatchTraveler[%s:%s]%s", getTravelerId(), getRootOperationUid(), getMatchKeyTuple());
    }
}
