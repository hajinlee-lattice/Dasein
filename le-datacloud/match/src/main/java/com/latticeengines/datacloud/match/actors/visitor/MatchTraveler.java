package com.latticeengines.datacloud.match.actors.visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class MatchTraveler extends Traveler implements Fact, Dimension {
    /*************************
     * Bound to whole travel
     **************************/

    private String dataCloudVersion;

    // MatchInput only provides immutable field values. Don't make another copy in MatchTraveler.  Don't make
    // changes to MatchInput during travel in actor system
    private MatchInput matchInput;

    // DnB match result. Currently only make use of 1st element.
    private List<DnBMatchContext> dnBMatchContexts = new ArrayList<>();

    // Real time or bulk match.
    private Boolean isBatchMode = false;

    // Actor name (class name) -> duns
    // Duns from different source could be treated differently
    private Map<String, String> dunsOriginMap;

    // TODO(jwinter): Refactor code to avoid needing to include InternalOutputRecord in MatchTraveler.
    // For Entity Match, the InternalOutputRecord is included in the MatchTraveler so that Match Standardization can
    // occur in the Match Planner Actor.
    private InternalOutputRecord internalOutputRecord;

    // TODO(jwinter): Not used yet, for next implementation.
    // Entity -> (MatchKey -> list of match keys corresponding field indexes in MatchInput.data array)
    private Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMap;

    // TODO(jwinter): Better name?
    // The raw input data record we are matching.
    private List<Object> inputRecord;

    // Match result: entity -> entityId
    private final Map<String, String> entityIds = new HashMap<>();


    /***************************************************************************
     * Bound to current decision graph or request to external assistant actors
     ***************************************************************************/
    // Match key and value. Strong typed to set easily
    // Prepare before entering actor system or at match planner actor
    private MatchKeyTuple matchKeyTuple;

    // Whether match goal is achieved in current decision graph
    private Boolean isMatched = false;

    // Current decision graph decides current entity
    // Set before entering actor system or at junction actor before transferring
    // to next decision graph
    // Default value: entity for ldc match
    private String entity = BusinessEntity.LatticeAccount.name();

    // Entity Match results.  Contains an ordered list of MatchKeyTuples used for lookup and the resulting lookup
    // results.  Lookup result is a list of strings because the MatchKeyTuple for SystemId may contain multiple System
    // IDs for lookup, giving multiple results.  If lookup failed, the result should be a list with one or more null
    // string elements corresponding to the MatchKeyTuples for which the lookup failed.
    private List<Pair<MatchKeyTuple, List<String>>> entityMatchLookupResults;

    // Entity match errors
    private List<String> entityMatchErrors;

    /***********************
     * Overridden methods
     ***********************/

    @Override
    protected Object getInputData() {
        return matchKeyTuple;
    }

    @Override
    public void setResult(Object result) {
        super.setResult(result);
        entityIds.put(entity, (String) result);
    }

    @Override
    @MetricField(name = "RootOperationUID")
    public String getRootOperationUid() {
        return super.getRootOperationUid();
    }

    @Override
    public void start() {
        super.start();
        debug("Has " + getMatchKeyTuple() + " to begin with.");
    }

    /*******************************
     * Constructor & Getter/Setter
     *******************************/

    public MatchTraveler(String rootOperationUid, MatchKeyTuple matchKeyTuple) {
        super(rootOperationUid);
        this.matchKeyTuple = matchKeyTuple;
        this.start();
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

    public void setMatchKeyTuple(MatchKeyTuple matchKeyTuple) {
        this.matchKeyTuple = matchKeyTuple;
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
        return entityIds.get(BusinessEntity.LatticeAccount.name());
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

    public InternalOutputRecord getInternalOutputRecord() {
        return internalOutputRecord;
    }

    public void setInternalOutputRecord(InternalOutputRecord internalOutputRecord) {
        this.internalOutputRecord = internalOutputRecord;
    }

    public Map<String, Map<MatchKey, List<Integer>>> getEntityKeyPositionMap() {
        return entityKeyPositionMap;
    }

    public void setEntityKeyPositionMap(Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMap) {
        this.entityKeyPositionMap = entityKeyPositionMap;
    }

    public List<Object> getInputRecord() {
        return inputRecord;
    }

    public void setInputRecord(List<Object> inputRecord) {
        this.inputRecord = inputRecord;
    }

    public Map<String, String> getEntityIds() {
        return entityIds;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public List<Pair<MatchKeyTuple, List<String>>> getEntityMatchLookupResults() {
        return entityMatchLookupResults;
    }

    public void setEntityMatchLookupResults(List<Pair<MatchKeyTuple, List<String>>> entityMatchLookupResults) {
        this.entityMatchLookupResults = entityMatchLookupResults;
    }

    public List<String> getEntityMatchErrors() {
        return entityMatchErrors;
    }

    public void setEntityMatchErrors(List<String> entityMatchErrors) {
        this.entityMatchErrors = entityMatchErrors;
    }
}
