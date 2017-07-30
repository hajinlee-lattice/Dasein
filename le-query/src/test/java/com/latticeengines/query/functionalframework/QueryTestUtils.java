package com.latticeengines.query.functionalframework;

import java.io.InputStream;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;

public class QueryTestUtils {

    private static AttributeRepository amAttrRepo;
    private static AttributeRepository customAttrRepo;

    public static AttributeRepository getCustomerAttributeRepo() {
        if (customAttrRepo == null) {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("attrrepo.json");
            customAttrRepo = JsonUtils.deserialize(is, AttributeRepository.class);
        }
        return customAttrRepo;
    }

    public static AttributeRepository getAMAttributeRepo() {
        if (amAttrRepo == null) {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("am_attrrepo.json");
            amAttrRepo = JsonUtils.deserialize(is, AttributeRepository.class);
        }
        return amAttrRepo;
    }
}
