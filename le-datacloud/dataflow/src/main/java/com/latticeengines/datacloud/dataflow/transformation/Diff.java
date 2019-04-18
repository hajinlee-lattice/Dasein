package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.AddMD5Hash;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.DiffferParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(Diff.DATAFLOW_BEAN_NAME)
public class Diff extends TransformationFlowBase<BasicTransformationConfiguration, DiffferParameters> {

    public static final String DATAFLOW_BEAN_NAME = "DiffFlow";
    public static final String TRANSFORMER_NAME = DataCloudConstants.TRANSFORMER_DIFFER;

    private static final String CHECK_SUM = "_CHECKSUM_";
    private static final String COMP = "_COMP_";

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Node construct(DiffferParameters parameters) {
        Node src = parameters.getBaseTables().size() == 2 ? addSource(parameters.getBaseTables().get(0))
                : addSource(getTableName(parameters.getBaseTables().get(0), parameters.getDiffVersion()));
        Node srcCompared = parameters.getBaseTables().size() == 2 ? addSource(parameters.getBaseTables().get(1))
                : addSource(getTableName(parameters.getBaseTables().get(0), parameters.getDiffVersionCompared()));

        List<String> finalAttrs = new ArrayList<>();
        finalAttrs.addAll(src.getFieldNames());
        Set<String> excludeFields = parameters.getExcludeFields() == null ? null
                : new HashSet(Arrays.asList(parameters.getExcludeFields()));
        List<String> compareFields = new ArrayList<>(Arrays.asList(parameters.getKeys()));
        compareFields.add(CHECK_SUM);

        src = src
                .apply(new AddMD5Hash(new Fields(CHECK_SUM), excludeFields),
                        new FieldList(src.getFieldNames()), new FieldMetadata(CHECK_SUM, String.class));
        srcCompared = srcCompared.apply(new AddMD5Hash(new Fields(CHECK_SUM), excludeFields),
                new FieldList(srcCompared.getFieldNames()), new FieldMetadata(CHECK_SUM, String.class))
                .retain(new FieldList(compareFields));
        srcCompared = renameCompared(srcCompared);

        Node joined = src.coGroup(new FieldList(parameters.getKeys()), Arrays.asList(srcCompared),
                Arrays.asList(new FieldList(renameCompKeys(parameters.getKeys()))),
                JoinType.LEFT);
        joined = joined.filter(String.format("!%s.equals(%s)", CHECK_SUM, renameCompAttr(CHECK_SUM)),
                new FieldList(CHECK_SUM, renameCompAttr(CHECK_SUM))).retain(new FieldList(finalAttrs));

        return joined;
    }

    public static String getTableName(String source, String version) {
        if (StringUtils.isEmpty(version)) {
            return source;
        }
        return source + "_" + version;
    }

    private String renameCompAttr(String attr) {
        return COMP + attr;
    }

    private String[] renameCompKeys(String[] keys) {
        String[] newKeys = new String[keys.length];
        for (int i = 0; i < keys.length; i++) {
            newKeys[i] = renameCompAttr(keys[i]);
        }
        return newKeys;
    }

    private Node renameCompared(Node node) {
        List<String> newAttrs = new ArrayList<>();
        node.getFieldNames().forEach(attr -> newAttrs.add(renameCompAttr(attr)));
        return node.rename(new FieldList(node.getFieldNames()), new FieldList(newAttrs));
    }

    @Override
    protected Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }


}
