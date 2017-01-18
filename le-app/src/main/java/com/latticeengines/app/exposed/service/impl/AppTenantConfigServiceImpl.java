package com.latticeengines.app.exposed.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.AppTenantConfigService;
import com.latticeengines.app.exposed.util.ValidateEnrichAttributesUtils;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;

@Component("appTenantConfigService")
public class AppTenantConfigServiceImpl implements AppTenantConfigService {
    public static final String ENRICHMENT_ATTRIBUTES_MAX_NUMBER_ZNODE = "/EnrichAttributesMaxNumber";
    private static final Log log = LogFactory.getLog(AppTenantConfigServiceImpl.class);
    public static final String PLS = "PLS";

    @Override
    public int getMaxPremiumLeadEnrichmentAttributes(String tenantId) {
        String maxPremiumLeadEnrichmentAttributes;
        Camille camille = CamilleEnvironment.getCamille();
        Path contractPath = null;
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

            contractPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace, PLS);
            Path path = contractPath.append(ENRICHMENT_ATTRIBUTES_MAX_NUMBER_ZNODE);
            maxPremiumLeadEnrichmentAttributes = camille.get(path).getData();
        } catch (KeeperException.NoNodeException ex) {
            log.error("Will replace maxPremiumLeadEnrichmentAttributes with the default value since there is none for the tenant: "
                    + tenantId);
            Path defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), PLS)
                    .append(new Path(ENRICHMENT_ATTRIBUTES_MAX_NUMBER_ZNODE));
            String defaultPremiumLeadEnrichmentAttributes;
            try {
                defaultPremiumLeadEnrichmentAttributes = camille.get(defaultConfigPath).getData();
            } catch (Exception e) {
                throw new RuntimeException("Cannot get default value for maximum premium lead enrichment attributes ");
            }
            try {
                camille.upsert(contractPath, DocumentUtils.toRawDocument(defaultPremiumLeadEnrichmentAttributes),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } catch (Exception e) {
                throw new RuntimeException("Cannot update value for maximum premium lead enrichment attributes ");
            }
            return ValidateEnrichAttributesUtils.validateEnrichAttributes(defaultPremiumLeadEnrichmentAttributes);
        } catch (Exception e) {
            throw new RuntimeException("Cannot get maximum premium lead enrichment attributes ", e);
        }

        return ValidateEnrichAttributesUtils.validateEnrichAttributes(maxPremiumLeadEnrichmentAttributes);
    }
}
