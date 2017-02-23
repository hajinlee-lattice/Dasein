package com.latticeengines.leadprioritization.dataflow;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;

@Component("dedupEventTable")
public class DedupEventTable extends TypesafeDataFlowBuilder<DedupEventTableParameters> {

    private static final Log log = LogFactory.getLog(DedupEventTable.class);

    private static final String SORT_EVENT = "__Sort_Event__";
    private static final String SORT_HAS_LM = "__Sort_HasLastMod__";
    private static final String SORT_LM_DIST = "__Sort_LM_OptCreTime_Dist__";
    static final int OPTIMAL_CREATION_TIME_DAYS_FROM_TODAY = 45;

    @Override
    public Node construct(DedupEventTableParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);
        FieldList sortCols = sortCols(eventTable);
        log.info("Using " + sortCols + " as sorting attributes");
        eventTable = addSortColumns(eventTable);
        eventTable = eventTable.groupByAndLimit(new FieldList(INT_LDC_DEDUPE_ID), sortCols, 1, true, false);
        eventTable = removeInternalAttrs(eventTable);
        return eventTable;
    }

    private Node addSortColumns(Node eventTable) {
        String eventColumn = InterfaceName.Event.name();
        Table sourceSchema = eventTable.getSourceSchema();

        // add event sort column
        eventTable = eventTable.apply(String.format("Boolean.TRUE.equals(%s) ? 1 : 0", eventColumn), new FieldList(eventColumn),
                new FieldMetadata(SORT_EVENT, Integer.class));
        if (sourceSchema.getLastModifiedKey() != null) {
            // add has LM column
            String lastModifiedField = sourceSchema.getLastModifiedKey().getAttributes().get(0);
            eventTable = eventTable.apply(lastModifiedField + " == null ? 0 : 1", new FieldList(lastModifiedField),
                    new FieldMetadata(SORT_HAS_LM, Integer.class));

            // add LM to optCreTime dist
            long optimalCreationTime = DateTime.now().minusDays(OPTIMAL_CREATION_TIME_DAYS_FROM_TODAY).getMillis();
            eventTable = eventTable.apply(
                    String.format("%s == null ? Integer.MIN_VALUE : -Math.abs(%s - %dL)", lastModifiedField,
                            lastModifiedField, optimalCreationTime),
                    new FieldList(lastModifiedField), new FieldMetadata(SORT_LM_DIST, Long.class));
        }

        return eventTable;
    }

    private FieldList sortCols(Node eventTable) {
        Table sourceSchema = eventTable.getSourceSchema();
        if (sourceSchema.getLastModifiedKey() == null) {
            return new FieldList(SORT_EVENT);
        } else {
            return new FieldList(SORT_HAS_LM, SORT_EVENT, SORT_LM_DIST);
        }
    }

    private Node removeInternalAttrs(final Node node) {
        List<String> internalAttrs = new ArrayList<>();
        Arrays.asList(INT_LDC_DEDUPE_ID, SORT_EVENT, SORT_HAS_LM, SORT_LM_DIST).forEach(n -> {
            if (node.getFieldNames().contains(n)) {
                internalAttrs.add(n);
            }
        });
        Node result = node;
        if (!internalAttrs.isEmpty()) {
            result = node.discard(new FieldList(internalAttrs));
        }
        return result;
    }

}
