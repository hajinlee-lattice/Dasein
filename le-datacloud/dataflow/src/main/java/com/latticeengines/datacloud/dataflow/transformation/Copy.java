package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.Copy.BEAN_NAME;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.CopierParameters;

/**
 * Directly copy/merge base sources into output avros
 */
@Component(BEAN_NAME)
public class Copy extends TypesafeDataFlowBuilder<CopierParameters> {

    public static final String BEAN_NAME = "copy";

    @Override
    public Node construct(CopierParameters parameters) {
        Node first = addSource(parameters.getBaseTables().get(0));
        if (parameters.getBaseTables().size() > 1) {
            List<Node> remaining = new ArrayList<>();
            for (int i = 1; i < parameters.getBaseTables().size(); i++) {
                Node node = addSource(parameters.getBaseTables().get(i));
                node = node.retain(first.getFieldNamesArray());
                remaining.add(node);
            }
            first = first.merge(remaining);
        }

        List<String> retainAttrs = filterAttrs(first, parameters.retainAttrs);
        if (retainAttrs != null && !retainAttrs.isEmpty()) {
            first = first.retain(new FieldList(retainAttrs));
        }

        List<String> discardAttrs = filterAttrs(first, parameters.discardAttrs);
        if (discardAttrs != null && !discardAttrs.isEmpty()) {
            first = first.discard(new FieldList(discardAttrs));
        }

        if (parameters.sortKeys != null && !parameters.sortKeys.isEmpty()) {
            first = first.sort(parameters.sortKeys, parameters.sortDecending);
        }

        return first;
    }

    private List<String> filterAttrs(Node node, List<String> attrs) {
        if (CollectionUtils.isEmpty(attrs)) {
            return attrs;
        }
        Set<String> attrSet = new HashSet<>();
        List<String> fieldNames = node.getFieldNames();
        return attrs.stream().filter(attr -> fieldNames.contains(attr) && attrSet.add(attr))
                .collect(Collectors.toList());
    }

}
