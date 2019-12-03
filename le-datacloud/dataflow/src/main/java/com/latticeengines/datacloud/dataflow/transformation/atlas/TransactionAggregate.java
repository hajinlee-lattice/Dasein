package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_TRANSACTION_AGGREGATOR;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.FillDefaultFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.TransactionAggregator;
import com.latticeengines.dataflow.runtime.cascading.propdata.TypeConvertFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransactionAggregateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.transaction.NamedPeriod;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.metadata.transaction.TransactionMetrics;

import cascading.tuple.Fields;

@Component(TransactionAggregate.DATAFLOW_BEAN_NAME)
public class TransactionAggregate extends ConfigurableFlowBase<TransactionAggregateConfig> {

    public static final String DATAFLOW_BEAN_NAME = "TransactionAggregate";
    public static final String TRANSFORMER_NAME = TRANSFORMER_TRANSACTION_AGGREGATOR;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        TransactionAggregateConfig config = getTransformerConfig(parameters);
        Map<String, List<Product>> productMap = config.getProductMap();
        String idField = config.getIdField();
        String accountField = config.getAccountField();
        String productField = config.getProductField();
        String quantityField = config.getQuantityField();
        String amountField = config.getAmountField();
        String dateField = config.getDateField();
        String typeField = config.getTypeField();
        String transactionType = config.getTransactionType();
        List<String> inputPeriods = config.getPeriods();
        List<String> inputMetrics = config.getMetrics();
        Map<String, Object> defaultValues = new HashMap<>();

        Node transactions = addSource(parameters.getBaseTables().get(0));
        Node accounts = addSource(parameters.getBaseTables().get(1));


        transactions = transactions.filter(typeField + ".equals(\"" + transactionType + "\")", new FieldList(typeField));

        List<String> productIds = new ArrayList<>(productMap.keySet());

        List<FieldMetadata> fms = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        fms.add(new FieldMetadata(accountField, String.class));
        fields.add(accountField);

        List<String> periods = new ArrayList<>();
        List<String> metrics = new ArrayList<>();
        Long long0 = 0L;
        Double double0 = 0.0;
        for (int i = 0; i < inputPeriods.size(); i++) {
            NamedPeriod namedPeriod = NamedPeriod.fromName(inputPeriods.get(i));
            TransactionMetrics transactionMetrics = TransactionMetrics.fromName(inputMetrics.get(i));
            if ((namedPeriod == null) || (transactionMetrics == null)) {
                continue;
            }

            periods.add(namedPeriod.getName());
            metrics.add(transactionMetrics.getName());
            for (String productId : productIds) {
                String attrName = TransactionMetrics.getAttrName(productId, namedPeriod.getName(), transactionMetrics.getName());
                if (transactionMetrics == TransactionMetrics.PURCHASED) {
                    fms.add(new FieldMetadata(attrName, Boolean.class));
                    defaultValues.put(attrName, Boolean.FALSE);
                } else if (transactionMetrics == TransactionMetrics.QUANTITY) {
                    fms.add(new FieldMetadata(attrName, Long.class));
                    defaultValues.put(attrName, long0);
                } else if (transactionMetrics == TransactionMetrics.AMOUNT) {
                    fms.add(new FieldMetadata(attrName, Double.class));
                    defaultValues.put(attrName, double0);
                }
                fields.add(attrName);
            }
        }

        TransactionAggregator agg = new TransactionAggregator(new Fields(fields.toArray(new String[fields.size()])),
                                                       productIds, periods, metrics,
                                                       accountField, productField, dateField, quantityField, amountField);
        Node transactionHistory = transactions.groupByAndAggregate(new FieldList(accountField), agg, fms, Fields.RESULTS).renamePipe("TransactionAggregate");


        List<FieldMetadata> accountFms = accounts.getSchema();
        boolean isLongId = false;
        for (FieldMetadata accountFm : accountFms) {
            if (accountFm.getFieldName().equals(idField)) {
                if (accountFm.getJavaType() == Long.class) {
                    isLongId = true;
                }
                break;
            }
        }

        Node accountIds = accounts.retain(new FieldList(idField));
        if (!idField.equals(accountField)) {
            accountIds = accountIds.rename(new FieldList(idField), new FieldList(accountField));
        }
        if (isLongId) {
            TypeConvertFunction function = new TypeConvertFunction(accountField, TypeConvertStrategy.ANY_TO_STRING,
                    false);
            accountIds = accountIds.apply(function, new FieldList(accountField),
                           new FieldMetadata(accountField, String.class));
            transactionHistory = transactionHistory.leftJoin(accountField, accountIds, accountField);
        } else {
            transactionHistory = accountIds.leftJoin(accountField, transactionHistory, accountField);
        }
        transactionHistory = transactionHistory.retain(new FieldList(fields));

        List<String> fieldNames = transactionHistory.getFieldNames();
        List<Object> defaultValueList = new ArrayList<>();
        for (String fieldName : fieldNames) {
             defaultValueList.add(defaultValues.get(fieldName));
        }
        transactionHistory = transactionHistory.apply(
                        new FillDefaultFunction(fieldNames, defaultValueList),
                        new FieldList(fieldNames), transactionHistory.getSchema(),
                        new FieldList(fieldNames), Fields.REPLACE);

        return transactionHistory;

    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TransactionAggregateConfig.class;
    }
}
