package com.latticeengines.cdl.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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

    public static final String DATAFLOW_BEAN_NAME = "orphanTransactionExportFlow";
    private static final Logger log = LoggerFactory.getLogger(OrphanTransactionExportFlow.class);
    public static final String RENAME_PREFIX = "OrphanTransactionExport_";

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

        List<String> retainFields = srcTxn.getFieldNames();
        String renamedAccount = RENAME_PREFIX + InterfaceName.AccountId.name();
        String renamedProduct = RENAME_PREFIX + InterfaceName.ProductId.name();
        srcAccount = renameFields(srcAccount, InterfaceName.AccountId.name(), renamedAccount);
        srcProduct = renameFields(srcProduct, InterfaceName.ProductId.name(), renamedProduct);
        Node result = srcTxn.join(new FieldList(InterfaceName.AccountId.name()), srcAccount, new FieldList(renamedAccount),
                JoinType.LEFT);
        result = result.join(new FieldList(InterfaceName.ProductId.name()), srcProduct, new FieldList(renamedProduct),
                JoinType.LEFT);
        List<String> filterFields = Arrays.asList(renamedAccount, renamedProduct);
        log.info("rename: " + renamedAccount + ":" + renamedProduct);
        result = result.filter(String.format("%s == null || %s == null", renamedAccount, renamedProduct),
                new FieldList(filterFields));
        result = result.retain(new FieldList(retainFields));
        return result;
    }

    public Node renameFields(Node node, String previousName, String newName) {
        log.info("starting renaming: " + previousName + ":" + newName);
        List<String> previousNames = node.getFieldNames();
        List<String> newNames = new ArrayList<>(previousNames);
        log.info(":" + previousNames.indexOf(previousName));
        newNames.set(previousNames.indexOf(previousName), newName);
        return node.rename(new FieldList(previousNames), new FieldList(newNames));
    }
}
