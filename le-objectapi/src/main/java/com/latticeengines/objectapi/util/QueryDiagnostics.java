package com.latticeengines.objectapi.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;

@Component("queryDiagnostics")
public class QueryDiagnostics {

    private static final Logger log = LoggerFactory.getLogger(QueryDiagnostics.class);

    // Sync these paths with le-admin/src/main/resources/cdl_default.json and cdl_metadata.json
    private static final String CDL = "CDL";
    private static final String DIAGNOSTICS = "/Diagnostics";
    private static final String ENABLE_QUERY_LOGGING = "/QueryLogging";
    private boolean queryLoggingPathLogged = false;

    public QueryDiagnostics() {
    }

    public boolean getQueryLoggingConfig() {
        Path path = null;
        CustomerSpace customerSpace = null;
        String podId = null;
        try {
            Camille camille = CamilleEnvironment.getCamille();
            podId = CamilleEnvironment.getPodId();
            customerSpace = MultiTenantContext.getCustomerSpace();
            path = PathBuilder.buildCustomerSpaceServicePath(podId, customerSpace, CDL)
                    .append(DIAGNOSTICS).append(ENABLE_QUERY_LOGGING);
            return camille.get(path).getData().equalsIgnoreCase("true");
        } catch (Exception e) {
            if (!queryLoggingPathLogged) {
                queryLoggingPathLogged = true;
                log.info("Failed to find config {} for customer {} in podId {}. Defaulting to false. Exception: {}",
                        path, podId, customerSpace, e);
            }
        }
        return false;
    }

}
