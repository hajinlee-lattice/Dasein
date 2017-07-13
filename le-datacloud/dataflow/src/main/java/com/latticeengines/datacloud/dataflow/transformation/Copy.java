package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.Copy.BEAN_NAME;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(Copy.class);

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

        if (parameters.retainAttrs != null && !parameters.retainAttrs.isEmpty()) {
            first = first.retain(new FieldList(parameters.retainAttrs));
        }

        if (parameters.discardAttrs != null && !parameters.discardAttrs.isEmpty()) {
            first = first.retain(new FieldList(parameters.discardAttrs));
        }

        if (parameters.sortKeys != null && !parameters.sortKeys.isEmpty()) {
            first = first.sort(parameters.sortKeys, parameters.sortDecending);
        }

        return first;
    }

}
