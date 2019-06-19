package com.latticeengines.datacloud.match.actors.visitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class MatchTraveler extends Traveler implements Fact, Dimension {
    private static final Logger log = LoggerFactory.getLogger(MatchTraveler.class);

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

    // Entity -> MatchKeyTuple
    // A map from each entity in the match process to the full MatchKeyTuple containing all the match keys provided.
    private final Map<String, MatchKeyTuple> entityMatchKeyTuples = new HashMap<>();

    // Entity -> (List of Pairs (MatchKeyTuple, List of LookupResults))
    // A map from each entity in the match process to an ordered list of MatchKeyTuples used for lookup and the list
    // of lookup results.  The lookup results is a list of strings, rather than one result, because SystemId may
    // contain multiple IDs for lookup, giving multiple results.
    private Map<String, List<Pair<MatchKeyTuple, List<String>>>> entityMatchLookupResults = new HashMap<>();

    // A list of pairs of EntityMatchType enums and corresponding MatchKeyTuples, used to track the progress through
    // the Lattice Data Cloud decision graph.
    private List<Pair<EntityMatchType, MatchKeyTuple>> entityLdcMatchTypeToTupleList = new ArrayList<>();

    private Set<String> fieldsToClear = new HashSet<>();

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

    // List of Pairs (MatchKeyTuple, List of LookupResults)
    // Entity Match results.  Contains an ordered list of MatchKeyTuples used for lookup and the resulting lookup
    // results.  Lookup result is a list of strings because the MatchKeyTuple for SystemId may contain multiple System
    // IDs for lookup, giving multiple results.  If lookup failed, the result should be a list with one or more null
    // string elements corresponding to the MatchKeyTuples for which the lookup failed.
    private List<Pair<MatchKeyTuple, List<String>>> matchLookupResults;

    // actor name -> index of entity match lookup result got from the actor in
    // matchLookupResults list
    private Map<String, Integer> actorLookupResIdxes;

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
        fieldsToClear.clear();
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

    // Pushed order needs to be in sync with recoverOtherTransitionHistory()
    @Override
    protected List<Object> getOtherTransitionHistoryToPush() {
        return Arrays.asList(matchLookupResults, actorLookupResIdxes);
    }

    @Override
    protected void clearOtherCurrentTransitionHistory() {
        matchLookupResults = null;
        actorLookupResIdxes = null;
    }

    // Fetched order needs to be in sync with getOtherTransitionHistoryToPush()
    @SuppressWarnings("unchecked")
    @Override
    protected void recoverOtherTransitionHistory(List<Object> others) {
        matchLookupResults = (List<Pair<MatchKeyTuple, List<String>>>) others.get(0);
        actorLookupResIdxes = (Map<String, Integer>) others.get(1);
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
    public List<Pair<MatchKeyTuple, List<String>>> getMatchLookupResults() {
        if (matchLookupResults == null) {
            matchLookupResults = new ArrayList<>();
        }
        return matchLookupResults;
    }

    public void setMatchLookupResults(List<Pair<MatchKeyTuple, List<String>>> matchLookupResults) {
        this.matchLookupResults = matchLookupResults;
    }

    // Use this method to add entity match lookup result instead of
    // directly adding to list matchLookupResults
    public void addLookupResult(String actorName, Pair<MatchKeyTuple, List<String>> lookupResults) {
        if (actorLookupResIdxes == null) {
            actorLookupResIdxes = new HashMap<>();
        }
        if (matchLookupResults == null) {
            matchLookupResults = new ArrayList<>();
        }
        if (actorLookupResIdxes.get(actorName) == null) {
            // 1st time lookup
            matchLookupResults.add(lookupResults);
            actorLookupResIdxes.put(actorName, matchLookupResults.size() - 1);
        } else {
            // overwrite existing lookup result in retried lookup
            int idx = actorLookupResIdxes.get(actorName);
            matchLookupResults.set(idx, lookupResults);
        }
    }

    public boolean hasCompleteLookupResults(String actorName) {
        if (actorLookupResIdxes == null || actorLookupResIdxes.get(actorName) == null) {
            return false;
        }
        int idx = actorLookupResIdxes.get(actorName);
        if (matchLookupResults.size() <= idx || matchLookupResults.get(idx) == null) {
            return false; // should not happen
        }
        if (CollectionUtils.isEmpty(matchLookupResults.get(idx).getRight())) {
            return false; // should not happen
        }
        return !matchLookupResults.get(idx).getRight().contains(null);
    }

    public Map<String, MatchKeyTuple> getEntityMatchKeyTuples() {
        return entityMatchKeyTuples;
    }

    public MatchKeyTuple getEntityMatchKeyTuple(String entity) {
        return entityMatchKeyTuples.get(entity);
    }

    public void addEntityMatchKeyTuple(String entity, MatchKeyTuple tuple) {
        entityMatchKeyTuples.put(entity, tuple);
    }

    public Map<String, List<Pair<MatchKeyTuple, List<String>>>> getEntityMatchLookupResults() {
        return entityMatchLookupResults;
    }

    public List<Pair<MatchKeyTuple, List<String>>> getEntityMatchLookupResult(String entity) {
        return entityMatchLookupResults.get(entity);
    }

    public void addEntityMatchLookupResults(String entity, List<Pair<MatchKeyTuple, List<String>>> lookupResultList) {
        if (entityMatchLookupResults.containsKey(entity)) {
            List<Pair<MatchKeyTuple, List<String>>> curLookupResultList = entityMatchLookupResults.get(entity);
            if (CollectionUtils.isNotEmpty(curLookupResultList)) {
                curLookupResultList.addAll(lookupResultList);
                return;
            }
            log.error("EntityMatchLookupResults key " + entity + " maps to null or empty list");
        }
        entityMatchLookupResults.put(entity, new ArrayList<>(lookupResultList));
    }

    public List<Pair<EntityMatchType, MatchKeyTuple>> getEntityLdcMatchTypeToTupleList() {
        return entityLdcMatchTypeToTupleList;
    }

    public void addEntityLdcMatchTypeToTupleList(Pair<EntityMatchType, MatchKeyTuple> pair) {
        entityLdcMatchTypeToTupleList.add(pair);
    }

    public Set<String> getFieldsToClear() {
        return fieldsToClear;
    }

    public void addFieldToClear(String field) {
        if (StringUtils.isNotBlank(field)) {
            fieldsToClear.add(field);
        }
    }

    public List<String> getEntityMatchErrors() {
        return getTravelErrors();
    }

    public void setEntityMatchErrors(List<String> entityMatchErrors) {
        logTravelErrors(entityMatchErrors);
    }

}
