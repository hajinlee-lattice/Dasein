package com.latticeengines.cdl.dataflow;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomTrxField;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ProductId;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.OrphanTransactionExportParameters;

@Component(OrphanTransactionExportFlow.DATAFLOW_BEAN_NAME)
public class OrphanTransactionExportFlow extends TypesafeDataFlowBuilder<OrphanTransactionExportParameters> {

    public static final String DATAFLOW_BEAN_NAME = "orphanTransactionExportFlow";
    public static final String RENAME_PREFIX = "OrphanTransactionExport_";

    private static final Set<String> OPTIONAL_EXPORT_ATTRS = ImmutableSet.of( //
            EntityId.name(), AccountId.name(), ContactId.name(), //
            CustomerAccountId.name(), CustomerContactId.name());

    private static final Set<String> DISABLE_EXPORT_ATTRS = ImmutableSet.of(CustomTrxField.name());

    @Override
    public Node construct(OrphanTransactionExportParameters parameters) {
        Node srcTxn = addSource(parameters.getTransactionTable());
        if (StringUtils.isEmpty(parameters.getAccountTable()) || StringUtils.isEmpty(parameters.getProductTable())) {
            return srcTxn;
        }
        Node srcAccount = addSource(parameters.getAccountTable());
        Node srcProduct = addSource(parameters.getProductTable());

        // dedup by ProductId
        srcProduct = srcProduct.groupByAndLimit(new FieldList(ProductId.name()), 1);

        // only retain selected export attributes + standard transaction
        // attributes
        Set<String> validatedColumns = CollectionUtils.isEmpty(parameters.getValidatedColumns()) ? new HashSet<>()
                : new HashSet<>(parameters.getValidatedColumns());
        List<String> retainFields = srcTxn.getFieldNames().stream()
                .filter(name -> (!OPTIONAL_EXPORT_ATTRS.contains(name) && !DISABLE_EXPORT_ATTRS.contains(name))
                        || validatedColumns.contains(name))
                .collect(Collectors.toList());

        String renamedAccount = RENAME_PREFIX + AccountId.name();
        String renamedProduct = RENAME_PREFIX + ProductId.name();
        srcAccount = renameFields(srcAccount, AccountId.name(), renamedAccount);
        srcProduct = renameFields(srcProduct, ProductId.name(), renamedProduct);
        Node result = srcTxn.join(new FieldList(AccountId.name()), srcAccount, new FieldList(renamedAccount),
                JoinType.LEFT);
        result = result.join(new FieldList(ProductId.name()), srcProduct, new FieldList(renamedProduct),
                JoinType.LEFT);
        List<String> filterFields = Arrays.asList(renamedAccount, renamedProduct);
        result = result.filter(String.format("%s == null || %s == null", renamedAccount, renamedProduct),
                new FieldList(filterFields));

        result = result.retain(new FieldList(retainFields));
        result = finalizeSchema(result);
        return result;
    }

    private Node renameFields(Node node, String previousName, String newName) {
        node = node.rename(new FieldList(previousName), new FieldList(newName));
        // add retain to force syncing metadata
        return node.retain(node.getFieldNamesArray());
    }

    private Node finalizeSchema(Node node) {
        if (node.getSchema(AccountId.name()) == null && node.getSchema(CustomerAccountId.name()) != null) {
            node = node.rename(new FieldList(CustomerAccountId.name()), new FieldList(AccountId.name()));
        }
        if (node.getSchema(ContactId.name()) == null && node.getSchema(CustomerContactId.name()) != null) {
            node = node.rename(new FieldList(CustomerContactId.name()), new FieldList(ContactId.name()));
        }
        return node;
    }
}
