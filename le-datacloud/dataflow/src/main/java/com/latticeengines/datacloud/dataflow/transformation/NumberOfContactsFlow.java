package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import cascading.operation.aggregator.Count;
import cascading.tuple.Fields;

import org.apache.avro.Schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.NumberOfContactsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

// Description:  Transformation class for calculating the number of contacts associated with each account and
//     generating a table of account ID and count of associated contacts.
@Component(NumberOfContactsFlow.DATAFLOW_BEAN_NAME)
public class NumberOfContactsFlow extends ConfigurableFlowBase<NumberOfContactsConfig> {
    private static Logger log = LoggerFactory.getLogger(NumberOfContactsFlow.class);

    public static final String TRANSFORMER_NAME = DataCloudConstants.TRANSFORMER_NUMBER_OF_CONTACTS;
    public static final String DATAFLOW_BEAN_NAME = "NumberOfContactsFlow";
    private NumberOfContactsConfig config;

    public static final String ACCOUNT_ID = InterfaceName.AccountId.name();
    public static final String NUMBER_OF_CONTACTS = InterfaceName.NumberOfContacts.name();

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return NumberOfContactsConfig.class;
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
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);

        // Get the accounts table.
        Node accountsNode = addSource(parameters.getBaseTables().get(0));
        // Get the contacts table.
        Node contactsNode = addSource(parameters.getBaseTables().get(1));
        // Left join based on Account ID.
        Node joinedNode = accountsNode.leftJoin(config.getLhsJoinField(), contactsNode, config.getRhsJoinField());

        // Run a group-by-and-aggregate Node operation to group contacts by account ID and then count the number of
        // contacts for each account ID.
        List<String> groupby = Collections.singletonList(ACCOUNT_ID);
        List<FieldMetadata> outputFieldMetadataList = Arrays.asList(
                new FieldMetadata(ACCOUNT_ID, String.class),
                new FieldMetadata(NUMBER_OF_CONTACTS, Long.class));
        Node numberOfContactsNode = joinedNode.groupByAndAggregate(new FieldList(groupby),
                new Count(new Fields(NUMBER_OF_CONTACTS)), outputFieldMetadataList, Fields.ALL);

        Node integerFormattedNode = numberOfContactsNode.apply(String.format(
                "%s == null ? null : new Integer(%s.intValue())", NUMBER_OF_CONTACTS, NUMBER_OF_CONTACTS),
                new FieldList(NUMBER_OF_CONTACTS),
                new FieldMetadata(NUMBER_OF_CONTACTS, Integer.class));
        return integerFormattedNode;
    }

}
