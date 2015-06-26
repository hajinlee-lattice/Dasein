package com.latticeengines.upgrade.dl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component("dataLoaderManager")
public class DataLoaderManager {

    private static final String DL_URL1 = "https://dataloader-prod.lattice-engines.com/Dataloader_PLS";
    private static final String DL_URL2 = "https://data-pls.lattice-engines.com/Dataloader_PLS";

    @Autowired
    private DataLoaderService dataLoaderService;

    public SpaceConfiguration constructSpaceConfiguration(String tenantName) {
        String version = dataLoaderService.getTemplateVersion(tenantName, DL_URL1);
        if (!StringUtils.isEmpty(version)) {
            return constructSpaceConfiguration(DL_URL1, parseTopoloy(version));
        }

        version = dataLoaderService.getTemplateVersion(tenantName, DL_URL2);
        if (!StringUtils.isEmpty(version)) {
            return constructSpaceConfiguration(DL_URL2, parseTopoloy(version));
        }

        Exception e = new RuntimeException("Neither of the two prod dl urls can find the tenant requested, " +
                "or they both timeout.");
        throw new LedpException(LedpCode.LEDP_24000, e);

    }

    private static SpaceConfiguration constructSpaceConfiguration(String dlUrl, CRMTopology topology) {
        SpaceConfiguration spaceConfiguration = new SpaceConfiguration();
        spaceConfiguration.setProduct(LatticeProduct.LPA);
        spaceConfiguration.setDlAddress(dlUrl);
        spaceConfiguration.setTopology(topology);
        return spaceConfiguration;
    }

    private static CRMTopology parseTopoloy(String dlSpecVersion) {
        if (dlSpecVersion.contains("MKTO")) {
            return CRMTopology.MARKETO;
        }
        if (dlSpecVersion.contains("ELQ")) {
            return CRMTopology.ELOQUA;
        }
        Exception e = new RuntimeException("Unkown topology version " + dlSpecVersion);
        throw new LedpException(LedpCode.LEDP_24000, e);
    }

}
