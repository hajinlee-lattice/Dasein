package com.latticeengines.datacloud.match.actors.visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
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
    // Predefined special key: DataCloudConstants.ACCOUNT_MASTER. Duns attached
    // with this key is the Duns for final matched account in LDC
    private Map<String, String> dunsOriginMap;

    // The raw input data record we are matching.
    private List<Object> inputDataRecord;

    // Entity -> (MatchKey -> list of field indexes in the input data record (above) which correspond to the match key)
    private Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps;

    private EntityMatchKeyRecord entityMatchKeyRecord;

    // Match result: entity -> entityId
    private final Map<String, String> entityIds = new HashMap<>();
    // Newly Allocated EntityIDs: entity -> entityId
    private final Map<String, String> newEntityIds = new HashMap<>();


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

    // actor name -> index of entity match lookup result got from the actor in
    // entityMatchLookupResults list
    private Map<String, Integer> actorLookupResIdxes = new HashMap<>();

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
    public void prepareForRetravel() {
        super.prepareForRetravel();
        entityMatchErrors = null;
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

    @MetricField(name = "DataCloudVersion")
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    private void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
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

    public List<DnBMatchContext> getDnBMatchContexts() {
        return dnBMatchContexts;
    }

    public void appendDnBMatchContext(DnBMatchContext dnBMatchContext) {
        this.dnBMatchContexts.add(dnBMatchContext);
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

    public List<Object> getInputDataRecord() {
        return inputDataRecord;
    }

    public void setInputDataRecord(List<Object> inputDataRecord) {
        this.inputDataRecord = inputDataRecord;
    }

    public Map<String, Map<MatchKey, List<Integer>>> getEntityKeyPositionMaps() {
        return entityKeyPositionMaps;
    }

    public void setEntityKeyPositionMaps(Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps) {
        this.entityKeyPositionMaps = entityKeyPositionMaps;
    }

    public EntityMatchKeyRecord getEntityMatchKeyRecord() {
        return entityMatchKeyRecord;
    }

    public void setEntityMatchKeyRecord(EntityMatchKeyRecord entityMatchKeyRecord) {
        this.entityMatchKeyRecord = entityMatchKeyRecord;
    }

    public Map<String, String> getEntityIds() {
        return entityIds;
    }

    public void addNewlyAllocatedEntityId(String entityId) {
        if (StringUtils.isBlank(entityId)) {
            return;
        }
        newEntityIds.put(entity, entityId);
    }

    public Map<String, String> getNewEntityIds() {
        return newEntityIds;
    }

    @MetricField(name = "LatticeAccountId")
    public String getLatticeAccountId() {
        return entityIds.get(BusinessEntity.LatticeAccount.name());
    }

    @MetricFieldGroup
    public MatchKeyTuple getMatchKeyTuple() {
        return matchKeyTuple;
    }

    public void setMatchKeyTuple(MatchKeyTuple matchKeyTuple) {
        this.matchKeyTuple = matchKeyTuple;
    }

    @MetricField(name = "Matched", fieldType = MetricField.FieldType.BOOLEAN)
    public Boolean isMatched() {
        return isMatched;
    }

    public void setMatched(Boolean isMatched) {
        this.isMatched = isMatched;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    // Use addLookupResult() to add entity match lookup result instead of
    // directly adding to the list
    public List<Pair<MatchKeyTuple, List<String>>> getEntityMatchLookupResults() {
        return entityMatchLookupResults;
    }

    public void setEntityMatchLookupResults(List<Pair<MatchKeyTuple, List<String>>> entityMatchLookupResults) {
        this.entityMatchLookupResults = entityMatchLookupResults;
    }

    // Use this method to add entity match lookup result instead of
    // directly adding to list entityMatchLookupResults
    public void addLookupResult(String actorName, Pair<MatchKeyTuple, List<String>> lookupResults) {
        if (actorLookupResIdxes.get(actorName) == null) {
            // 1st time lookup
            entityMatchLookupResults.add(lookupResults);
            actorLookupResIdxes.put(actorName, entityMatchLookupResults.size() - 1);
        } else {
            // overwrite existing lookup result in retried lookup
            int idx = actorLookupResIdxes.get(actorName);
            entityMatchLookupResults.set(idx, lookupResults);
        }
    }

    public boolean hasCompleteLookupResults(String actorName) {
        if (actorLookupResIdxes.get(actorName) == null) {
            return false;
        }
        int idx = actorLookupResIdxes.get(actorName);
        if (entityMatchLookupResults.size() <= idx || entityMatchLookupResults.get(idx) == null) {
            return false; // should not happen
        }
        if (CollectionUtils.isEmpty(entityMatchLookupResults.get(idx).getRight())) {
            return false; // should not happen
        }
        return !entityMatchLookupResults.get(idx).getRight().contains(null);
    }

    public List<String> getEntityMatchErrors() {
        return entityMatchErrors;
    }

    public void setEntityMatchErrors(List<String> entityMatchErrors) {
        this.entityMatchErrors = entityMatchErrors;
    }
}
