package com.latticeengines.leadprioritization.dataflow;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.dataflow.util.DataFlowUtils;

@Component("dedupEventTable")
public class DedupEventTable extends TypesafeDataFlowBuilder<DedupEventTableParameters> {
    private static final String DOMAIN = "__Domain";

    @Override
    public Node construct(DedupEventTableParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);
        Table schema = eventTable.getSourceSchema();
        Node last = eventTable;

        Attribute websiteColumn = schema.getAttribute(parameters.websiteColumn);
        // If website column is available, use that. Otherwise use email column
        if (websiteColumn != null) {
            last = DataFlowUtils.normalizeDomain(last, parameters.websiteColumn, DOMAIN);
        } else {
            last = DataFlowUtils.extractDomainFromEmail(last, parameters.emailColumn, DOMAIN);
            last = DataFlowUtils.normalizeDomain(last, DOMAIN);
        }

        last = last.groupByAndLimit(new FieldList(DOMAIN), //
                new FieldList(parameters.eventColumn, schema.getLastModifiedKey().getAttributes().get(0)), //
                1, //
                true, //
                false);
        return last.discard(new FieldList(DOMAIN));
    }

}
