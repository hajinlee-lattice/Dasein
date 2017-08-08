package com.latticeengines.datacloud.dataflow.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class LatticeAccountIdUtils {
    private static final Logger log = LoggerFactory.getLogger(LatticeAccountIdUtils.class);

    public static Node convetLatticeAccountIdDataType(Node node) {
        String latticeAccountId = InterfaceName.LatticeAccountId.name();
        if (node.getFieldNames().contains(latticeAccountId)) {
            FieldMetadata fm = node.getSchema(latticeAccountId);
            if (!Long.class.equals(fm.getJavaType())) {
                log.info("Converting " + latticeAccountId + " from " + fm.getJavaType() + " to Long.");
                String expression = String.format("%s == null ? null : Long.valueOf(%s)", latticeAccountId,
                        latticeAccountId);
                node = node.apply(expression, new FieldList(latticeAccountId),
                        new FieldMetadata(latticeAccountId, Long.class));
            } else {
                log.info(latticeAccountId + " is already in the type of Long.");
            }
        } else {
            log.info("There is no " + latticeAccountId + " column to convert.");
        }
        return node;
    }
}
