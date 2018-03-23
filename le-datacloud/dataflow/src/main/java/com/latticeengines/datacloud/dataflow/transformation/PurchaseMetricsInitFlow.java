package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component(PurchaseMetricsInitFlow.BEAN_NAME)
public class PurchaseMetricsInitFlow extends ConfigurableFlowBase<TransformerConfig> {

    public static final String BEAN_NAME = "purchaseMetricsInitFlow";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node periodTable = addSource(parameters.getBaseTables().get(0));
        Node account = addSource(parameters.getBaseTables().get(1));
        Node product = addSource(parameters.getBaseTables().get(2));

        String periodName = getPeriodName(periodTable);

        if (account.getSchema(InterfaceName.SpendAnalyticsSegment.name()) == null) {
            account.addColumnWithFixedValue(InterfaceName.SpendAnalyticsSegment.name(), null, String.class);
        }
        
        /* TODO: After Ke checks in his change, this part needs to be uncommented
        product = product.filter(String.format("\"%s\".equalsIgnoreCase(%s)", ProductType.ANALYTIC.name(),
                InterfaceName.ProductType.name()), new FieldList(InterfaceName.ProductType.name()));
                */

        List<String> appendFields = new ArrayList<>();
        appendFields.add(InterfaceName.AccountId.name());
        appendFields.add(InterfaceName.ProductId.name());
        appendFields.add(InterfaceName.SpendAnalyticsSegment.name());
        account = account.addColumnWithFixedValue("_DUMMY_", null, String.class);
        product = product.addColumnWithFixedValue("_DUMMY_", null, String.class);
        Node base = account.join(new FieldList("_DUMMY_"), product, new FieldList("_DUMMY_"), JoinType.INNER)
                .retain(new FieldList(appendFields));

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(periodTable.getFieldNames());
        retainFields.add(InterfaceName.SpendAnalyticsSegment.name());

        periodTable = periodTable.rename(new FieldList(InterfaceName.ProductId.name(), InterfaceName.AccountId.name()),
                new FieldList("TXN_" + InterfaceName.ProductId.name(), "TXN_" + InterfaceName.AccountId.name()));
        periodTable = periodTable.retain(new FieldList(periodTable.getFieldNames()));
        
        periodTable = base
                .join(new FieldList(InterfaceName.AccountId.name(), InterfaceName.ProductId.name()), periodTable,
                        new FieldList("TXN_" + InterfaceName.AccountId.name(), "TXN_" + InterfaceName.ProductId.name()),
                        JoinType.LEFT)
                .retain(new FieldList(retainFields));
        
        periodTable = periodTable.apply(
                String.format("%s == null ? \"%s\" : %s", InterfaceName.PeriodName.name(), periodName,
                        InterfaceName.PeriodName.name()),
                new FieldList(InterfaceName.PeriodName.name()),
                new FieldMetadata(InterfaceName.PeriodName.name(), String.class));

        return periodTable;
    }

    private String getPeriodName(Node periodTable) {
        Extract extract = periodTable.getSourceSchema().getExtracts().get(0);
        String hdfsPath = extract.getPath();
        if (!hdfsPath.endsWith("*.avro")) {
            if (hdfsPath.endsWith("/")) {
                hdfsPath += "*.avro";
            } else {
                hdfsPath += "/*.avro";
            }
        }
        Iterator<GenericRecord> recordIterator = AvroUtils.iterator(periodTable.getHadoopConfig(), hdfsPath);
        if (recordIterator.hasNext()) {
            GenericRecord record = recordIterator.next();
            return String.valueOf(record.get(InterfaceName.PeriodName.name()));
        }
        throw new RuntimeException("Fail to find period name from period table " + periodTable.getPipeName());
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return PurchaseMetricsInitFlow.BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.PURCHASE_METRICS_INITIATOR;
    }
}
