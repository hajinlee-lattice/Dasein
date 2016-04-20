package com.latticeengines.serviceflows.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FilterReportColumn;
import com.latticeengines.domain.exposed.dataflow.GroupByDomainReportColumn;
import com.latticeengines.domain.exposed.dataflow.ReportColumn;
import com.latticeengines.domain.exposed.dataflow.flows.CreateReportParameters;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.serviceflows.dataflow.util.DataFlowUtils;

@Component("createReport")
public class CreateReport extends TypesafeDataFlowBuilder<CreateReportParameters> {
    private static final String DOMAIN = "__Domain";
    public static final String TEMPORARY_FIELD_NAME = "__Temporary";

    @Override
    public Node construct(CreateReportParameters parameters) {
        Node source = addSource(parameters.sourceTableName);
        source = DataFlowUtils.addInternalId(source);
        Node last = null;
        for (ReportColumn aggregation : parameters.columns) {
            if (last == null) {
                last = addReport(aggregation, source);
            } else {
                last = last.combine(addReport(aggregation, source));
            }
        }

        return last;
    }

    private Node addReport(ReportColumn column, Node source) {
        Node ret = null;
        if (column instanceof GroupByDomainReportColumn) {
            GroupByDomainReportColumn casted = (GroupByDomainReportColumn) column;
            List<Aggregation> aggregationList = new ArrayList<>();
            source = DataFlowUtils.extractDomain(source, DOMAIN);
            aggregationList.add(new Aggregation(DOMAIN, TEMPORARY_FIELD_NAME, AggregationType
                    .valueOf(column.aggregationType)));
            ret = source.groupBy(new FieldList(DOMAIN), aggregationList);
            aggregationList = new ArrayList<>();
            aggregationList.add(new Aggregation(DOMAIN, column.columnName, AggregationType
                    .valueOf(column.aggregationType)));
            ret = ret.groupBy(new FieldList(), aggregationList);
            return ret;
        } else if (column instanceof FilterReportColumn) {
            FilterReportColumn casted = (FilterReportColumn) column;
            ret = source.filter(casted.expression, new FieldList(casted.expressionFields));
            List<Aggregation> aggregationList = new ArrayList<>();
            aggregationList.add(new Aggregation(InterfaceName.InternalId.toString(), column.columnName, AggregationType
                    .valueOf(column.aggregationType)));
            ret = ret.groupBy(new FieldList(), aggregationList);
        } else if (column instanceof ReportColumn) {
            List<Aggregation> aggregationList = new ArrayList<>();
            aggregationList.add(new Aggregation(InterfaceName.InternalId.toString(), column.columnName, AggregationType
                    .valueOf(column.aggregationType)));
            ret = source.groupBy(new FieldList(), aggregationList);
        } else {
            throw new RuntimeException("Unsupported report column");
        }
        return ret.renamePipe(column.columnName);
    }

    @Override
    public void validate(CreateReportParameters parameters) {
        if (parameters.columns.size() == 0) {
            throw new LedpException(LedpCode.LEDP_26016);
        }
    }
}
