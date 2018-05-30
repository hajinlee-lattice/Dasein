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
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.dataflow.runtime.cascading.MappingFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.PivotRatingsConfig;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
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
            Node ruleRating = addSource(parameters.getBaseTables().get(ruleSrcIdx));
            ruleRating = ruleRating.filter(idCol + " != null", new FieldList(idCol));
            ruleRating = ruleRating.discard(InterfaceName.__Composite_Key__.name(),
                    InterfaceName.CDLUpdatedTime.name());
            ruleRating = renameIds(ruleRating);
            pivoted = pivotField(ruleRating, ruleEngineIds, InterfaceName.Rating.name(), String.class, null)
                    .renamePipe("rule_rating");
        }

        if (aiSrcIdx != null) {
            Node aiRating = addSource(parameters.getBaseTables().get(aiSrcIdx));
            aiRating = aiRating.filter(idCol + " != null", new FieldList(idCol));
            Node aiPivoted = pivotAIBased(aiRating);
            if (pivoted == null) {
                pivoted = aiPivoted;
            } else {
                String aiIdCol = "AI_" + idCol;
                aiPivoted = aiPivoted.apply(idCol, new FieldList(idCol), new FieldMetadata(aiIdCol, String.class));
                pivoted = pivoted.join(idCol, aiPivoted, aiIdCol, JoinType.OUTER);
                List<String> fields = new ArrayList<>(pivoted.getFieldNames());
                fields.addAll(aiPivoted.getFieldNames());
                String mergedIdCol = "Merged_" + idCol;
                pivoted = pivoted.apply(String.format("%s == null ? %s : %s", idCol, aiIdCol, idCol),
                        new FieldList(idCol, aiIdCol), new FieldMetadata(mergedIdCol, String.class));
                pivoted = pivoted.discard(aiIdCol, idCol);
                pivoted = pivoted.rename(new FieldList(mergedIdCol), new FieldList(idCol));
                pivoted = pivoted.discard(findFieldsToDiscard(pivoted));
                fields = pivoted.getFieldNames();
                Collections.sort(fields);
                pivoted = pivoted.retain(new FieldList(fields));
            }
        }

        if (pivoted == null) {
            throw new IllegalStateException("Must have either rule ratings or ai ratings input.");
        }

        pivoted = pivoted.addTimestamp(InterfaceName.CDLCreatedTime.name());
        pivoted = pivoted.addTimestamp(InterfaceName.CDLUpdatedTime.name());
        return pivoted;
    }

    private Node pivotAIBased(Node rawRatings) {
        rawRatings = renameIds(rawRatings);
        rawRatings = rawRatings.discard(InterfaceName.CDLUpdatedTime.name(), "Model_GUID",
                InterfaceName.__Composite_Key__.name());
        Node rating = pivotField(rawRatings, aiEngineIds, InterfaceName.Rating.name(), String.class, null)
                .renamePipe("ai_rating");
        List<Node> rhs = new ArrayList<>();
        rhs.add(pivotScoreField(rawRatings, aiEngineIds, RatingEngine.ScoreType.Score));
        if (CollectionUtils.isNotEmpty(evEngineIds)) {
            rhs.add(pivotScoreField(rawRatings, evEngineIds, RatingEngine.ScoreType.ExpectedRevenue));
        }
        FieldList joinKey = new FieldList(idCol);
        List<FieldList> joinKeys = rhs.stream().map(n -> joinKey).collect(Collectors.toList());
        Node joined = rating.coGroup(joinKey, rhs, joinKeys, JoinType.OUTER);
        return joined.discard(findFieldsToDiscard(joined));
    }

    private Node pivotScoreField(Node rawRatings, Set<String> engineIds, RatingEngine.ScoreType scoreType) {
        String suffix = RatingEngine.SCORE_ATTR_SUFFIX.get(scoreType);
        String interfaceName = scoreType.name();
        Class<? extends Serializable> scoreClz = RatingEngine.SCORE_ATTR_CLZ.get(scoreType);
        return pivotField(rawRatings, engineIds, interfaceName, scoreClz, suffix).renamePipe("ai_" + suffix);
    }

    private <T extends Serializable> Node pivotField(Node rawRatings, Set<String> pivotedKeys, String valCol,
            Class<T> resultClz, String suffix) {
        PivotStrategyImpl pivotStrategy = PivotStrategyImpl.max(keyCol, valCol, pivotedKeys, resultClz, null);
        Node pivoted = rawRatings.pivot(new String[] { idCol }, pivotStrategy);

        List<String> retainFields = new ArrayList<>();
        retainFields.add(idCol);

        if (StringUtils.isNotBlank(suffix)) {
            List<String> oldFields = new ArrayList<>(pivotedKeys);
            List<String> newFields = oldFields.stream().map(f -> f + "_" + suffix).collect(Collectors.toList());
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

    private FieldList findFieldsToDiscard(Node joined) {
        List<String> toDiscard = joined.getFieldNames().stream() //
                .filter(f -> f.contains(idCol) && !f.equals(idCol)) //
                .collect(Collectors.toList());
        return new FieldList(toDiscard);
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
