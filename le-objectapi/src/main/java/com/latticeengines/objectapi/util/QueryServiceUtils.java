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

    private static boolean localAttrRepoMode = false;
    private static AttributeRepository attrRepo;

    public static AttributeRepository checkAndGetAttrRepo(CustomerSpace customerSpace,
                                                          DataCollection.Version version,
                                                          QueryEvaluatorService queryEvaluatorService) {
        if (localAttrRepoMode) {
            if (attrRepo == null) {
                throw new IllegalStateException("Local attr repo has not been set.");
            }
            return attrRepo;
        } else {
            String tenant = customerSpace.toString();
            AttributeRepository attrRepo = queryEvaluatorService.getAttributeRepository(tenant, version);
            if (attrRepo == null) {
                throw new LedpException(LedpCode.LEDP_37015, new Object[]{tenant, version});
            }
            return attrRepo;
        }
    }

    public static TimeFilterTranslator getTimeFilterTranslator(TransactionService transactionService,
                                                               FrontEndQuery frontEndQuery) {
        if (frontEndQuery != null && transactionService.needTimeFilterTranslator(frontEndQuery)) {
            return transactionService.getTimeFilterTranslator(frontEndQuery.getEvaluationDateStr());
        } else {
            return null;
        }
    }

    public static void toLocalAttrRepoMode() {
        localAttrRepoMode = true;
    }

    public static void setAttrRepo(AttributeRepository attrRepo) {
        QueryServiceUtils.attrRepo = attrRepo;
    }

    public static AttributeRepository getAttrRepo() {
        return attrRepo;
    }

}
