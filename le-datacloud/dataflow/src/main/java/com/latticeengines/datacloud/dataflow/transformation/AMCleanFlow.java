package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.LATTICE_ACCOUNT_ID;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.LATTICE_ID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.AddNullColumns;
import com.latticeengines.dataflow.runtime.cascading.EmptyStrColsToNullFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.LatticeIdFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.TypeBatchConvertFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AMCleanerParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.AMCleanerParameters.CleanOpt;
import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(AMCleanFlow.DATAFLOW_BEAN_NAME)
public class AMCleanFlow extends TransformationFlowBase<BasicTransformationConfiguration, AMCleanerParameters> {

    public static final String DATAFLOW_BEAN_NAME = "AMCleanFlow";
    public static final String TRANSFORMER_NAME = "AMCleaner";

    private Map<CleanOpt, Pair<TypeConvertStrategy, Class<?>>> typeStrategies;
    private Map<TypeConvertStrategy, Class<?>> typeMetadata;

    @Override
    protected Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @PostConstruct
    private void init() {
        typeStrategies = ImmutableMap.of(CleanOpt.STRING, Pair.of(TypeConvertStrategy.ANY_TO_STRING, String.class), //
                CleanOpt.DOUBLE, Pair.of(TypeConvertStrategy.ANY_TO_DOUBLE, Double.class), //
                CleanOpt.INTEGER, Pair.of(TypeConvertStrategy.ANY_TO_INT, Integer.class), //
                CleanOpt.LONG, Pair.of(TypeConvertStrategy.ANY_TO_LONG, Long.class), //
                CleanOpt.BOOLEAN, Pair.of(TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.class));
        typeMetadata = ImmutableMap.of(TypeConvertStrategy.ANY_TO_STRING, String.class, //
                TypeConvertStrategy.ANY_TO_DOUBLE, Double.class, //
                TypeConvertStrategy.ANY_TO_INT, Integer.class, //
                TypeConvertStrategy.ANY_TO_LONG, Long.class, //
                TypeConvertStrategy.ANY_TO_BOOLEAN, Boolean.class);
    }

    @Override
    public Node construct(AMCleanerParameters parameters) {
        Node accountMaster = addSource(parameters.getBaseTables().get(0));
        // Map for storing attribute and argument (data type expected)
        Map<String, TypeConvertStrategy> attrsAndExpectedType = new HashMap<>();
        List<String> dropAttributes = new ArrayList<>();
        Map<String, CleanOpt> attrArgs = parameters.getAttrOpts();
        List<String> expectedAMFields = accountMaster.getFieldNames();
        List<String> addAttributes = new ArrayList<>();
        List<FieldMetadata> fieldMetaData = new ArrayList<>();
        Set<String> existedAMFields = new HashSet<String>(accountMaster.getFieldNames());
        for (Map.Entry<String, CleanOpt> entry : attrArgs.entrySet()) {
            String attribute = entry.getKey();
            CleanOpt argument = entry.getValue();
            // checking if attribute not exists in am schema
            if (!attribute.equals(LATTICE_ACCOUNT_ID) && !existedAMFields.contains(attribute)) {
                addAttributes.add(attribute);
                expectedAMFields.add(attribute);
                if (typeStrategies.containsKey(argument)) {
                    fieldMetaData.add(new FieldMetadata(attribute, typeStrategies.get(argument).getValue()));
                }
                continue;
            }
            switch (argument) {
                case RETAIN:
                    break;
                case DROP:
                    dropAttributes.add(attribute);
                    break;
                case LATTICEID:
                    LatticeIdFunction idFunc = new LatticeIdFunction(new Fields(attribute), LATTICE_ID);
                    accountMaster = accountMaster //
                            .apply(idFunc, new FieldList(LATTICE_ID), new FieldMetadata(attribute, String.class));
                    expectedAMFields.add(attribute);
                    break;
                case STRING:
                case DOUBLE:
                case INTEGER:
                case LONG:
                case BOOLEAN:
                    if (!attribute.getClass().getName().equalsIgnoreCase(String.valueOf(argument))) {
                        attrsAndExpectedType.put(attribute, typeStrategies.get(argument).getKey());
                    }
                    break;
                default:
                    throw new RuntimeException(
                            String.format("AMCleaner argument %s is not supported in AM cleanup.", argument));
            }
        }

        // type cast to required type
        List<FieldMetadata> fms = new ArrayList<>();
        for (String field : accountMaster.getFieldNames()) {
            if(attrsAndExpectedType.containsKey(field)) {
                fms.add(new FieldMetadata(field, typeMetadata.get(attrsAndExpectedType.get(field))));
            } else {
                if (field.equals(LATTICE_ID))
                    fms.add(new FieldMetadata(field, Long.class));
                else
                    fms.add(new FieldMetadata(field, field.getClass()));
            }
        }

        Fields fieldDeclaration = new Fields(
                accountMaster.getFieldNames().toArray(new String[accountMaster.getFieldNames().size()]));
        TypeBatchConvertFunction convertFunc = new TypeBatchConvertFunction(fieldDeclaration,
                attrsAndExpectedType, expectedAMFields, true);
        accountMaster = accountMaster.apply(convertFunc, new FieldList(accountMaster.getFieldNames()), fms,
                new FieldList(expectedAMFields), Fields.REPLACE);

        // adding columns from sourceAttribute that don't exist in AM
        AddNullColumns addNullCol = new AddNullColumns(
                new Fields(addAttributes.toArray(new String[addAttributes.size()])));
        accountMaster = accountMaster //
                .apply(addNullCol, new FieldList(accountMaster.getFieldNames()), fieldMetaData,
                        new FieldList(expectedAMFields));

        // checking if amColumn not exists in attrArgs
        for (String amCol : expectedAMFields) {
            if (!attrArgs.containsKey(amCol)) {
                dropAttributes.add(amCol);
                fieldMetaData.remove(amCol);
            }
        }

        // Removing columns that are to be dropped
        accountMaster = accountMaster.discard(new FieldList(dropAttributes));

        // Replace all empty string with null
        Fields fieldDecForEmptyStr = new Fields(
                accountMaster.getFieldNames().toArray(new String[accountMaster.getFieldNames().size()]));
        EmptyStrColsToNullFunction emptyWithNull = new EmptyStrColsToNullFunction(fieldDecForEmptyStr);

        accountMaster = accountMaster //
                .apply(emptyWithNull, new FieldList(accountMaster.getFieldNames()), fieldMetaData,
                        new FieldList(accountMaster.getFieldNames()), Fields.REPLACE);
        return accountMaster;
    }

}
