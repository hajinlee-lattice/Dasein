package com.latticeengines.objectapi.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.TimeFilterTranslator;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;

public class QueryServiceUtils {

    private static final Logger log = LoggerFactory.getLogger(QueryServiceUtils.class);

    private static boolean localAttrRepoMode = false;
    private static AttributeRepository attrRepo;
    private static boolean localQueryFlag = false;
    private static boolean queryLogging = false;

    // Sync with le-admin resources cdl_default.json and cdl_metadata.json
    private static final String CDL = "CDL";
    private static final String DIAGNOSTICS = "/Diagnostics";
    private static final String ENABLE_QUERY_LOGGING = "/QueryLogging";

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

    public static boolean getQueryLoggingConfig() {
        if (!localQueryFlag) {
            Path path = null;
            CustomerSpace customerSpace = null;
            String podId = null;
            try {
                Camille camille = CamilleEnvironment.getCamille();
                podId = CamilleEnvironment.getPodId();
                CustomerSpace customer = MultiTenantContext.getCustomerSpace();
                path = PathBuilder.buildCustomerSpaceServicePath(podId, customer, CDL)
                        .append(DIAGNOSTICS)
                        .append(ENABLE_QUERY_LOGGING);
                String querySetting = camille.get(path).getData();
                queryLogging = querySetting.equalsIgnoreCase("true");
                localQueryFlag = true;
            } catch (Exception e) {
                log.info("Failed to find QueryLogging config " + path
                        + " for customer {} in podId " + podId
                        + ". Defaulting to " + queryLogging + ".", customerSpace);
            }
        }
        return queryLogging;
    }

}
