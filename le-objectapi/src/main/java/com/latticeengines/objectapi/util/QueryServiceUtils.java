package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;

public class QueryServiceUtils {
    public static AttributeRepository checkAndGetAttrRepo(CustomerSpace customerSpace,
                                                          QueryEvaluatorService queryEvaluatorService) {
        String tenant = customerSpace.toString();
        AttributeRepository attrRepo = queryEvaluatorService.getAttributeRepository(tenant);
        if (attrRepo == null) {
            throw new LedpException(LedpCode.LEDP_37015, new Object[]{tenant});
        }
        return attrRepo;
    }
}
