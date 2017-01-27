package com.latticeengines.leadprioritization.dataflow;

import java.util.List;

import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.dataflow.util.DataFlowUtils;

@Component("dedupEventTable")
public class DedupEventTable extends TypesafeDataFlowBuilder<DedupEventTableParameters> {

    private static final String DOMAIN = "__Domain";
    private static final String SORT = "__Sort";
    public static final int OPTIMAL_CREATION_TIME_DAYS_FROM_TODAY = 45;

    @Override
    public Node construct(DedupEventTableParameters parameters) {
        Node eventTable = addSource(parameters.eventTable);

        List<String> outputColumns = eventTable.getFieldNames();

        Node last = DataFlowUtils.extractDomain(eventTable, DOMAIN);
        last = addSortColumn(last, eventTable, SORT);

        Node emptyDomains = last //
                .filter(String.format("%s == null || %s.equals(\"\")", DOMAIN, DOMAIN), new FieldList(DOMAIN)) //
                .renamePipe("nullDomains");
        last = last.filter(String.format("%s != null && !%s.equals(\"\")", DOMAIN, DOMAIN), new FieldList(DOMAIN));

        if (!DataFlowUtils.getPublicDomainResolutionFields(last).isEmpty()) {

            Node publicDomain = addSource(parameters.publicDomain);
            publicDomain = DataFlowUtils.normalizeDomain(publicDomain, InterfaceName.Domain.name(), DOMAIN);
            publicDomain.discard(new FieldList(InterfaceName.Domain.name()));

            Node publicDomains = last.innerJoin(new FieldList(DOMAIN), publicDomain, new FieldList(DOMAIN));
            Node nonPublicDomains = last.stopList(publicDomain, DOMAIN, DOMAIN);

            publicDomains = publicDomains.discard(new FieldList(DOMAIN));
            publicDomains = DataFlowUtils.addHash(publicDomains, DOMAIN,
                    DataFlowUtils.getPublicDomainResolutionFields(publicDomains));

            nonPublicDomains = nonPublicDomains.groupByAndLimit(new FieldList(DOMAIN), //
                    new FieldList(SORT), //
                    1, //
                    true, //
                    false);

            publicDomains = publicDomains.groupByAndLimit(new FieldList(DOMAIN), //
                    new FieldList(SORT), //
                    1, //
                    true, //
                    false);

            publicDomains = publicDomains.retain(new FieldList(outputColumns));
            nonPublicDomains = nonPublicDomains.retain(new FieldList(outputColumns));
            emptyDomains = emptyDomains.retain(new FieldList(outputColumns));

            last = publicDomains.merge(nonPublicDomains).merge(emptyDomains);
        } else {
            last = last //
                    .filter(String.format("%s != null && !%s.equals(\"\")", DOMAIN, DOMAIN), new FieldList(DOMAIN)) //
                    .groupByAndLimit(new FieldList(DOMAIN), //
                            new FieldList(SORT), //
                            1, //
                            true, //
                            false);

            last = last.retain(new FieldList(outputColumns));
            emptyDomains = emptyDomains.retain(new FieldList(outputColumns));

            last = last.merge(emptyDomains);
        }
        return last;
    }

    private Node addSortColumn(Node last, Node source, String field) {
        FieldMetadata targetField = new FieldMetadata(field, String.class);

        String eventColumn = InterfaceName.Event.name();
        Table sourceSchema = source.getSourceSchema();
        if (sourceSchema.getLastModifiedKey() == null) {
            return last.addFunction(String.format("%s", eventColumn), new FieldList(eventColumn), targetField);
        } else {
            long optimalCreationTime = DateTime.now().minusDays(OPTIMAL_CREATION_TIME_DAYS_FROM_TODAY).getMillis();
            String lastModifiedField = sourceSchema.getLastModifiedKey().getAttributes().get(0);
            String expression = String.format(
                    "%s == null ? 0.0 : ((%s != null && %s ? 1.0 : 0.0) + (1.0 / ((double)Math.abs(%s - %dL) + 1.1)))",
                    lastModifiedField, eventColumn, eventColumn, lastModifiedField, optimalCreationTime);
            return last.addFunction(expression, new FieldList(eventColumn, lastModifiedField), targetField);
        }
    }

}
