package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.PivotRatings.BEAN_NAME;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.dataflow.runtime.cascading.MappingFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.PivotRatingsConfig;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.RatingEngine;

@Component(BEAN_NAME)
public class PivotRatings extends ConfigurableFlowBase<PivotRatingsConfig> {

    public static final String BEAN_NAME = "pivotRatings";
    public static final String TRANSFORMER_NAME = DataCloudConstants.TRANSFORMER_PIVOT_RATINGS;

    private Map<String, String> idAttrsMap;
    private Set<String> evEngineIds = Collections.emptySet();
    private Set<String> aiEngineIds = Collections.emptySet();
    private String keyCol = "EngineId";
    private String idCol = InterfaceName.AccountId.name();

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        PivotRatingsConfig config = getTransformerConfig(parameters);
        idAttrsMap = config.getIdAttrsMap();
        List<String> aiModelIds = config.getAiModelIds();
        List<String> evModelIds = config.getEvModelIds();

        if (CollectionUtils.isNotEmpty(aiModelIds)) {
            aiEngineIds = aiModelIds.stream().map(idAttrsMap::get).collect(Collectors.toSet());
        }
        if (CollectionUtils.isNotEmpty(evModelIds)) {
            evEngineIds = evModelIds.stream().map(idAttrsMap::get).collect(Collectors.toSet());
        }

        Integer ruleSrcIdx = config.getRuleSourceIdx();
        Integer aiSrcIdx = config.getAiSourceIdx();

        Node pivoted = null;
        if (ruleSrcIdx != null) {
            Set<String> ruleEngineIds;
            if (CollectionUtils.isNotEmpty(aiModelIds)) {
                ruleEngineIds = idAttrsMap.values().stream().filter(k -> !aiEngineIds.contains(k))
                        .collect(Collectors.toSet());
            } else {
                ruleEngineIds = new HashSet<>(idAttrsMap.values());
            }
            Node ruleRating = removeDummyRows(addSource(parameters.getBaseTables().get(ruleSrcIdx)));
            pivoted = pivotRuleBased(ruleEngineIds, ruleRating);
        }

        if (aiSrcIdx != null) {
            Node aiRating = removeDummyRows(addSource(parameters.getBaseTables().get(aiSrcIdx)));
            aiRating = aiRating.filter(idCol + " != null", new FieldList(idCol));
            Node aiPivoted = pivotAIBased(aiRating);
            if (pivoted == null) {
                pivoted = aiPivoted;
            } else {
                String aiIdCol = "AI_" + idCol;
                aiPivoted = aiPivoted.apply(idCol, new FieldList(idCol),
                        new FieldMetadata(aiIdCol, String.class));
                pivoted = pivoted.join(idCol, aiPivoted, aiIdCol, JoinType.OUTER);
                List<String> fields = new ArrayList<>(pivoted.getFieldNames());
                fields.addAll(aiPivoted.getFieldNames());
                pivoted = mergeIdCols(pivoted, aiIdCol);
                fields = pivoted.getFieldNames();
                Collections.sort(fields);
                pivoted = pivoted.retain(new FieldList(fields));
            }
        }

        if (pivoted == null) {
            throw new IllegalStateException("Must have either rule ratings or ai ratings input.");
        }

        Integer inactiveSrcIdx = config.getInactiveSourceIdx();
        if (inactiveSrcIdx != null) {
            Node inactive = removeDummyRows(addSource(parameters.getBaseTables().get(inactiveSrcIdx)));
            List<String> inactiveEngines = config.getInactiveEngines();
            pivoted = joinInactiveEngines(pivoted, inactive, inactiveEngines);
        }

        pivoted = removeExtraId(pivoted);
        pivoted = pivoted.filter(String.format("%s != null && %s != \"\"", idCol, idCol),
                new FieldList(idCol));
        pivoted = pivoted.addTimestamp(InterfaceName.CDLCreatedTime.name());
        pivoted = pivoted.addTimestamp(InterfaceName.CDLUpdatedTime.name());

        return pivoted;
    }

    private Node removeDummyRows(Node node) {
        return node.filter(String.format("%s != null", idCol), new FieldList(idCol));
    }

    private Node pivotRuleBased(Set<String> ruleEngineIds, Node ruleRating) {
        ruleRating = ruleRating.filter(idCol + " != null", new FieldList(idCol));
        ruleRating = ruleRating.discard(InterfaceName.__Composite_Key__.name());
        if (ruleRating.getFieldNames().contains(InterfaceName.CDLUpdatedTime.name())) {
            ruleRating = ruleRating.discard(InterfaceName.CDLUpdatedTime.name());
        }
        ruleRating = renameIds(ruleRating);
        return pivotField(ruleRating, ruleEngineIds, InterfaceName.Rating.name(), String.class,
                null).renamePipe("rule_rating");
    }

    private Node pivotAIBased(Node rawRatings) {
        rawRatings = renameIds(rawRatings);
        rawRatings = rawRatings.discard("Model_GUID", InterfaceName.__Composite_Key__.name());
        if (rawRatings.getFieldNames().contains(InterfaceName.CDLUpdatedTime.name())) {
            rawRatings = rawRatings.discard(InterfaceName.CDLUpdatedTime.name());
        }
        Node rating = pivotField(rawRatings, aiEngineIds, InterfaceName.Rating.name(), String.class,
                null).renamePipe("ai_rating");
        List<Node> rhs = new ArrayList<>();
        rhs.add(pivotScoreField(rawRatings, aiEngineIds, RatingEngine.ScoreType.Score));
        if (CollectionUtils.isNotEmpty(evEngineIds)) {
            rhs.add(pivotScoreField(rawRatings, evEngineIds,
                    RatingEngine.ScoreType.ExpectedRevenue));
            rhs.add(pivotScoreField(rawRatings, evEngineIds,
                    RatingEngine.ScoreType.PredictedRevenue));
        }
        FieldList joinKey = new FieldList(idCol);
        List<FieldList> joinKeys = rhs.stream().map(n -> joinKey).collect(Collectors.toList());
        Node joined = rating.coGroup(joinKey, rhs, joinKeys, JoinType.OUTER);
        return removeExtraId(joined);
    }

    private Node joinInactiveEngines(Node pivoted, Node inactive, List<String> inactiveEngines) {
        List<String> inactiveToRetain = new ArrayList<>();
        Set<String> existing = new HashSet<>(inactive.getFieldNames());
        for (String inactiveEngine : inactiveEngines) {
            for (RatingEngine.ScoreType scoreType : RatingEngine.ScoreType.values()) {
                String scoreAttr = RatingEngine.toRatingAttrName(inactiveEngine, scoreType);
                inactiveToRetain.add(scoreAttr);
            }
        }
        inactiveToRetain.retainAll(existing);

        FieldList filterFields = new FieldList(new ArrayList<>(inactiveToRetain));
        List<String> expressionTokens = new ArrayList<>();
        inactiveToRetain.forEach(attr -> expressionTokens.add(attr + " != null"));
        String expression = StringUtils.join(expressionTokens, " || ");
        // 255 is maximum number of method parameters allowed by java
        if (filterFields.getFields() != null && filterFields.getFields().length <= 255) {
            inactive = inactive.filter(expression, filterFields);
        }

        inactiveToRetain.add(InterfaceName.AccountId.name());
        inactive = inactive.retain(new FieldList(inactiveToRetain));
        String idCol2 = idCol + "_2";
        inactive = inactive.rename(new FieldList(idCol), new FieldList(idCol2));
        pivoted = pivoted.outerJoin(idCol, inactive, idCol2);
        pivoted = mergeIdCols(pivoted, idCol2);
        return pivoted;
    }

    private Node pivotScoreField(Node rawRatings, Set<String> engineIds,
            RatingEngine.ScoreType scoreType) {
        String suffix = RatingEngine.SCORE_ATTR_SUFFIX.get(scoreType);
        String interfaceName = scoreType.name();
        Class<? extends Serializable> scoreClz = RatingEngine.SCORE_ATTR_CLZ.get(scoreType);
        return pivotField(rawRatings, engineIds, interfaceName, scoreClz, suffix)
                .renamePipe("ai_" + suffix);
    }

    private <T extends Serializable> Node pivotField(Node rawRatings, Set<String> pivotedKeys,
            String valCol, Class<T> resultClz, String suffix) {
        PivotStrategyImpl pivotStrategy = PivotStrategyImpl.any(keyCol, valCol, pivotedKeys,
                resultClz, null);
        Node pivoted = rawRatings.pivot(new String[] { idCol }, pivotStrategy, false);

        List<String> retainFields = new ArrayList<>();
        retainFields.add(idCol);

        if (StringUtils.isNotBlank(suffix)) {
            List<String> oldFields = new ArrayList<>(pivotedKeys);
            List<String> newFields = oldFields.stream().map(f -> f + "_" + suffix)
                    .collect(Collectors.toList());
            pivoted = pivoted.rename(new FieldList(oldFields), new FieldList(newFields));
            retainFields.addAll(newFields);
        } else {
            retainFields.addAll(pivotedKeys);
        }

        return pivoted.retain(new FieldList(retainFields));
    }

    private Node renameIds(Node node) {
        MappingFunction mappingFunction = new MappingFunction(InterfaceName.ModelId.name(), keyCol,
                JsonUtils.convertMap(idAttrsMap, Serializable.class, Serializable.class));
        return node.apply(mappingFunction, new FieldList(InterfaceName.ModelId.name()),
                new FieldMetadata(keyCol, String.class));
    }

    private Node removeExtraId(Node joined) {
        List<String> toDiscard = joined.getFieldNames().stream() //
                .filter(f -> (f.contains(idCol) && !f.equals(idCol)) || //
                        (f.contains(RatingEngine.RATING_ENGINE_PREFIX) //
                                && !f.startsWith(RatingEngine.RATING_ENGINE_PREFIX))) //
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(toDiscard)) {
            joined = joined.discard(new FieldList(toDiscard));
        }
        return joined;
    }

    private Node mergeIdCols(Node joined, String idCol2) {
        String mergedCol = NamingUtils.uuid(idCol);
        joined = joined.apply(
                String.format("(%s == null || %s.length() == 0) ? %s : %s", //
                        idCol, idCol, idCol2, idCol),
                new FieldList(idCol, idCol2), new FieldMetadata(mergedCol, String.class));
        joined = joined.discard(idCol, idCol2);
        joined = joined.rename(new FieldList(mergedCol), new FieldList(idCol));
        return removeExtraId(joined);
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PivotRatingsConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

}
