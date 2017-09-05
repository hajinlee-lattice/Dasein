package com.latticeengines.query.functionalframework;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;

public class QueryTestUtils {

    private static AttributeRepository attrRepo;

    public static AttributeRepository getCustomerAttributeRepo() {
        if (attrRepo == null) {
            synchronized (QueryTestUtils.class) {
                try {
                    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("attrrepo.json.gz");
                    GZIPInputStream gis = new GZIPInputStream(is);
                    attrRepo = JsonUtils.deserialize(gis, AttributeRepository.class);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read attrrepo.json.gz");
                }
            }
        }
        return attrRepo;
    }
}
