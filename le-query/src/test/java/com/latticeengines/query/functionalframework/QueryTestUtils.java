package com.latticeengines.query.functionalframework;

import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;

public class QueryTestUtils {

    private static AttributeRepository attrRepo;

    public static final String ATTR_REPO_S3_DIR = "le-query/attrrepo";
    public static final String ATTR_REPO_S3_FILENAME = "attrrepo.json.gz";

    public static AttributeRepository getCustomerAttributeRepo(InputStream is) {
        if (attrRepo == null) {
            try {
                GZIPInputStream gis = new GZIPInputStream(is);
                attrRepo = JsonUtils.deserialize(gis, AttributeRepository.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to read attrrepo.json.gz", e);
            }
        }
        return attrRepo;
    }
}
