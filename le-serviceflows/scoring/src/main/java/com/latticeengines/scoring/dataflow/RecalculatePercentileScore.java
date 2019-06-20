package com.latticeengines.scoring.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculatePercentile;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.RecalculatePercentileScoreParameters;
import com.latticeengines.scoring.dataflow.ev.NodeSplitter;

import cascading.operation.aggregator.Count;
import cascading.tuple.Fields;

@Component("recalculatePercentileScore")
public class RecalculatePercentileScore extends TypesafeDataFlowBuilder<RecalculatePercentileScoreParameters> {

    private static final Logger log = LoggerFactory.getLogger(RecalculatePercentileScore.class);

    public static final String SCORE_COUNT_FIELD_NAME = ScoreResultField.RawScore.displayName + "_count";

    @Inject
    private NodeSplitter nodeSplitter;

    @Override
    public Node construct(RecalculatePercentileScoreParameters parameters) {

        Node inputTable = addSource(parameters.getInputTableName());
        FieldList originalFields = new FieldList(inputTable.getFieldNames());

        String modelGuidFieldName = parameters.getModelGuidField();
        String rawScoreFieldName = parameters.getRawScoreFieldName();
        String scoreFieldName = parameters.getScoreFieldName();
        int minPct = parameters.getPercentileLowerBound();
        int maxPct = parameters.getPercentileUpperBound();
        Map<String, String> originalScoreFieldMap = parameters.getOriginalScoreFieldMap();
        Node mergedScoreCount = mergeCount(inputTable, modelGuidFieldName, scoreFieldName);

        Node[] nodes = nodeSplitter.splitRevenue(mergedScoreCount, originalScoreFieldMap, modelGuidFieldName);
        Node model = null, evModel = null;
        if (nodes[0] != null) {
            log.info("Use target score derivation=" + parameters.isTargetScoreDerivation());
            model = nodes[0].groupByAndBuffer(new FieldList(modelGuidFieldName), new FieldList(rawScoreFieldName),
                    new CalculatePercentile(new Fields(mergedScoreCount.getFieldNames().toArray(new String[0])), minPct,
                            maxPct, scoreFieldName, SCORE_COUNT_FIELD_NAME, rawScoreFieldName,
                            parameters.isTargetScoreDerivation(), parameters.getTargetScoreDerivationPaths(),
                            modelGuidFieldName),
                    true, new ArrayList<>(mergedScoreCount.getSchema()));
        }
        if (nodes[1] != null) {
            evModel = nodes[1].sort(Arrays.asList(modelGuidFieldName, rawScoreFieldName), true);
        }

        Node output = model;
        if (model != null && evModel != null) {
            output = model.merge(evModel);
        } else if (model == null) {
            output = evModel;
        }
        return output.retain(originalFields);
    }

    private Node mergeCount(Node node, String modelGuidFieldName, String scoreFieldName) {
        String scoreCountPipeName = "ModelScoreCount_" + UUID.randomUUID().toString().replace("-", "") + "_";
        Node score = node.retain(scoreFieldName, modelGuidFieldName).renamePipe(scoreCountPipeName);
        List<FieldMetadata> scoreCountFms = Arrays.asList( //
                new FieldMetadata(modelGuidFieldName, String.class), //
                new FieldMetadata(scoreFieldName, String.class), //
                new FieldMetadata(SCORE_COUNT_FIELD_NAME, Long.class) //
        );
        Node totalCount = score
                .groupByAndAggregate(new FieldList(modelGuidFieldName), //
                        new Count(new Fields(SCORE_COUNT_FIELD_NAME)), //
                        scoreCountFms, Fields.ALL) //
                .retain(modelGuidFieldName, SCORE_COUNT_FIELD_NAME);
        return node.innerJoin(modelGuidFieldName, totalCount, modelGuidFieldName);
    }
}
