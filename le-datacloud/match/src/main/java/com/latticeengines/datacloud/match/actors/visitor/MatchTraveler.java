package com.latticeengines.datacloud.match.actors.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import akka.util.Timeout;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

public class MatchTraveler extends Traveler implements Fact, Dimension {
    private final MatchKeyTuple matchKeyTuple;
    private String dataCloudVersion;

    private MatchInput matchInput;

    private List<DnBMatchContext> dnBMatchContexts = new ArrayList<>();

    private Boolean isMatched = false;

    // only for metric purpose
    private Double totalTravelTime;
    private Boolean isBatchMode = false;

    private Timeout travelTimeout;

    private Map<String, String> dunsOriginMap;

    // Target entity represents the ultimate Business Entity that this match request is trying to find an ID for.
    // Since some entity matches will require matching other entities, eg. Contact might require Account, and
    // Transaction requires Account, Contact, and Product, it won't necessarily be clear from the Entity Key Map
    // what the ultimate match goal is.
    private String targetEntity;

    // Entity Match results.  Contains an ordered list of MatchKeyTuples used for lookup and the resulting lookup
    // results.  Lookup result is a list of strings because the MatchKeyTuple for SystemId may contain multiple System
    // IDs for lookup, giving multiple results.  If lookup failed, the result should be a list with one or more null
    // string elements corresponding to the MatchKeyTuples for which the lookup failed.
    private List<Pair<MatchKeyTuple, List<String>>> entityMatchLookupResults;

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

    public String getTargetEntity() { return targetEntity; }

    public void setTargetEntity(String targetEntity) { this.targetEntity = targetEntity; }

    public List<Pair<MatchKeyTuple, List<String>>> getEntityMatchLookupResults() {
        return entityMatchLookupResults;
    }

    public void setEntityMatchLookupResults(List<Pair<MatchKeyTuple, List<String>>> entityMatchLookupResults) {
        this.entityMatchLookupResults = entityMatchLookupResults;
    }

    @Override
    public void start() {
        super.start();
        debug("Has " + getMatchKeyTuple() + " to begin with.");
    }

}
