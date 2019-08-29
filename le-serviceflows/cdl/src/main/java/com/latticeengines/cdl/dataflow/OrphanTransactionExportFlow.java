package com.latticeengines.cdl.dataflow;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanTransactionExportParameters;

@Component(OrphanTransactionExportFlow.DATAFLOW_BEAN_NAME)
public class OrphanTransactionExportFlow extends TypesafeDataFlowBuilder<OrphanTransactionExportParameters> {

    private static final Logger log = LoggerFactory.getLogger(OrphanTransactionExportFlow.class);

    public static final String DATAFLOW_BEAN_NAME = "orphanTransactionExportFlow";
    public static final String RENAME_PREFIX = "OrphanTransactionExport_";

    private static final Set<String> OPTIONAL_EXPORT_ATTRS = ImmutableSet.of( //
            InterfaceName.EntityId.name(), InterfaceName.AccountId.name(), InterfaceName.ContactId.name(), //
            InterfaceName.CustomerAccountId.name(), InterfaceName.CustomerContactId.name());

    @Override
    public Node construct(OrphanTransactionExportParameters parameters) {
        Node srcTxn = addSource(parameters.getTransactionTable());
        if (StringUtils.isEmpty(parameters.getAccountTable()) || StringUtils.isEmpty(parameters.getProductTable())) {
            return srcTxn;
        }
        Node srcAccount = addSource(parameters.getAccountTable());
        Node srcProduct = addSource(parameters.getProductTable());

        // dedup by ProductId
        srcProduct = srcProduct.groupByAndLimit(new FieldList(InterfaceName.ProductId.name()), 1);

        // only retain selected export attributes + standard transaction
        // attributes
        Set<String> validatedColumns = CollectionUtils.isEmpty(parameters.getValidatedColumns()) ? new HashSet<>()
                : new HashSet<>(parameters.getValidatedColumns());
        List<String> retainFields = srcTxn.getFieldNames().stream()
                .filter(name -> !OPTIONAL_EXPORT_ATTRS.contains(name) || validatedColumns.contains(name))
                .collect(Collectors.toList());

        String renamedAccount = RENAME_PREFIX + InterfaceName.AccountId.name();
        String renamedProduct = RENAME_PREFIX + InterfaceName.ProductId.name();
        srcAccount = renameFields(srcAccount, InterfaceName.AccountId.name(), renamedAccount);
        srcProduct = renameFields(srcProduct, InterfaceName.ProductId.name(), renamedProduct);
        Node result = srcTxn.join(new FieldList(InterfaceName.AccountId.name()), srcAccount, new FieldList(renamedAccount),
                JoinType.LEFT);
        result = result.join(new FieldList(InterfaceName.ProductId.name()), srcProduct, new FieldList(renamedProduct),
                JoinType.LEFT);
        List<String> filterFields = Arrays.asList(renamedAccount, renamedProduct);
        result = result.filter(String.format("%s == null || %s == null", renamedAccount, renamedProduct),
                new FieldList(filterFields));

        result = result.retain(new FieldList(retainFields));
        return result;
    }

    public Node renameFields(Node node, String previousName, String newName) {
        node = node.rename(new FieldList(previousName), new FieldList(newName));
        // add retain to force syncing metadata
        return node.retain(node.getFieldNamesArray());
    }
}
