package com.latticeengines.leadprioritization.dataflow;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_REMOVED;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.DedupEventTableParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component("dedupEventTable")
public class DedupEventTable extends TypesafeDataFlowBuilder<DedupEventTableParameters> {
    private static final Logger log = LoggerFactory.getLogger(DedupEventTable.class);

    private static final String SORT_EVENT = "__Sort_Event__";

    @Override
    public Node construct(DedupEventTableParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);

        eventTable = eventTable.filter(INT_LDC_REMOVED + " == 0", new FieldList(INT_LDC_REMOVED));
        FieldList sortCols = new FieldList(SORT_EVENT);
        log.info("Using " + sortCols + " as sorting attributes");
        eventTable = addSortColumns(eventTable);

        Node hasDedupeId = eventTable.filter(INT_LDC_DEDUPE_ID + " != null", new FieldList(INT_LDC_DEDUPE_ID));
        Node noDedupeId = eventTable.filter(INT_LDC_DEDUPE_ID + " == null", new FieldList(INT_LDC_DEDUPE_ID));
        hasDedupeId = hasDedupeId.groupByAndLimit(new FieldList(INT_LDC_DEDUPE_ID), sortCols, 1, true, false);
        Node result = hasDedupeId.merge(noDedupeId);
        result = removeInternalAttrs(result);
        return result;
    }

    private Node addSortColumns(Node eventTable) {
        String eventColumn = InterfaceName.Event.name();
        eventTable = eventTable.apply(String.format("Boolean.TRUE.equals(%s) ? 1 : 0", eventColumn), new FieldList(
                eventColumn), new FieldMetadata(SORT_EVENT, Integer.class));
        return eventTable;
    }

    private Node removeInternalAttrs(final Node node) {
        List<String> internalAttrs = new ArrayList<>();
        Arrays.asList(INT_LDC_DEDUPE_ID, INT_LDC_REMOVED, SORT_EVENT).forEach(n -> {
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
