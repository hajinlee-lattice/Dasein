package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;

public class QueryServiceUtils {

    public static AttributeRepository checkAndGetAttrRepo(CustomerSpace customerSpace,
                                                          DataCollection.Version version,
                                                          QueryEvaluatorService queryEvaluatorService) {
        String tenant = customerSpace.toString();
        AttributeRepository attrRepo = queryEvaluatorService.getAttributeRepository(tenant, version);
        if (attrRepo == null) {
            throw new LedpException(LedpCode.LEDP_37015, new Object[]{ tenant, version });
        }
        return attrRepo;
    }

    public static TimeFilterTranslator getTimeFilterTranslator(TransactionService transactionService,
                                                               FrontEndQuery frontEndQuery) {
        if (frontEndQuery != null && transactionService.needTimeFilterTranslator(frontEndQuery)) {
            return transactionService.getTimeFilterTranslator(frontEndQuery.getEvaluationDateStr());
        } else {
            return null;
        }
    }
}
