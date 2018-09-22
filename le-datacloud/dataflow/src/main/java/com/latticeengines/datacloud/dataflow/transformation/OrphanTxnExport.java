package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component(OrphanTxnExport.DATAFLOW_BEAN_NAME)
public class OrphanTxnExport extends ConfigurableFlowBase<TransformerConfig>{
    public static final String DATAFLOW_BEAN_NAME = "OrphanTxnExport";
    public static final String TRANSFORMER_NAME = "OrphanTxnExportTransformer";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node srcAccount = addSource(parameters.getBaseTables().get(0));
        Node srcProduct = addSource(parameters.getBaseTables().get(1));
        Node srcTxn = addSource(parameters.getBaseTables().get(2));
        List<String> retainFields = srcTxn.getFieldNames();

        String renamedAccount = createNewName(srcAccount,InterfaceName.AccountId.name());
        String renamedProduct = createNewName(srcProduct,InterfaceName.ProductId.name());

        srcAccount = renameFields(srcAccount, InterfaceName.AccountId.name(),renamedAccount);
        srcProduct = renameFields(srcProduct, InterfaceName.ProductId.name(),renamedProduct);
        srcTxn = srcTxn.join(new FieldList(InterfaceName.AccountId.name()),srcAccount, new FieldList(renamedAccount), JoinType.LEFT);
        srcTxn = srcTxn.join(new FieldList(InterfaceName.ProductId.name()),srcProduct, new FieldList(renamedProduct), JoinType.LEFT);

        List<String> filterFields = Arrays.asList(renamedAccount,renamedProduct);
        srcTxn = srcTxn.filter(String.format("%s == null || %s == null",renamedAccount,renamedProduct), new FieldList(filterFields));
        srcTxn = srcTxn.retain(new FieldList(retainFields));
        return srcTxn;
    }
    public String createNewName(Node node, String previousName){
        return previousName + "_" + node.getPipeName();
    }

    public Node renameFields(Node node, String previousName, String newName){
        List<String> previousNames = node.getFieldNames();
        List<String> newNames = new ArrayList<>(previousNames);
        newNames.set(previousNames.indexOf(previousName),newName);
        return node.rename(new FieldList(previousNames),new FieldList(newNames));
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
        return TransformerConfig.class;
    }
}
