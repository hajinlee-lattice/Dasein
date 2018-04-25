package com.latticeengines.apps.core.service.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.AttrValidator;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrSubType;
import com.latticeengines.domain.exposed.serviceapps.core.AttrType;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationErrors;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationMsg;


@Component("limitationValidator")
public class LimitationValidator extends AttrValidator {
    public static final String VALIDATOR_NAME = "LIMITATION_VALIDATOR";
    private static final String DATA_CLOUD_LICENSE = "/DataCloudLicense";
    private static final String MAX_ENRICH_ATTRIBUTES = "/MaxEnrichAttributes";
    private static final String PLS = "PLS";
    private static final String EXPORT = "Export";
    private static final int LIMIT = 500;
    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    protected LimitationValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> attrConfigs) {
        String tenantId = MultiTenantContext.getTenantId();
        List<AttrConfig> existingConfigs = attrConfigEntityMgr.findAllByTenantId(tenantId);
        existingConfigs.addAll(attrConfigs); // append the user selected configs to existing in DB
        checkDataLicense(existingConfigs, attrConfigs);
        checkSystemLimit(existingConfigs, attrConfigs);
    }

    private void checkDataLicense(List<AttrConfig> configs, List<AttrConfig> userSelectedConfigs) {
        String tenantId = MultiTenantContext.getTenantId();
        int totalSelected = 0;
        for (DataLicense license : DataLicense.values()) {
            int limit = getMaxPremiumLeadEnrichmentAttributesByLicense(tenantId, license);
            List<AttrConfig> premiumConfigs = configs.stream()
                    .filter(entity -> license.getDataLicense().equals(entity.getDataLicense()))
                    .collect(Collectors.toList());
            int userSelectedNumber = premiumConfigs.size();
            totalSelected += userSelectedNumber;
            if (limit < userSelectedNumber) {
                userSelectedConfigs.forEach(e -> {
                    if (license.getDataLicense().equals(e.getDataLicense())) {
                        addErrorMsg(ValidationErrors.Type.EXCEED_DATA_LICENSE,
                                String.format(ValidationMsg.Errors.EXCEED_LICENSE_LIMIT, userSelectedNumber,
                                        license.getDescription(), limit),
                                e);
                    }
                });
            }
        }
        int totalLimit = getMaxPremiumLeadEnrichmentAttributesByLicense(tenantId, null);
        int selected = totalSelected;
        if (totalSelected > totalLimit) {
            userSelectedConfigs.forEach(e -> {
                if (e.getDataLicense() != null) {
                    addErrorMsg(ValidationErrors.Type.EXCEED_DATA_LICENSE,
                            String.format(ValidationMsg.Errors.EXCEED_LICENSE_LIMIT, selected, EXPORT, totalLimit),
                            e);
                }
            });
        }
    }

    private void checkSystemLimit(List<AttrConfig> configs, List<AttrConfig> userSelectedConfigs) {
        checkDetailLimit(configs, userSelectedConfigs, AttrType.Custom, AttrSubType.Extension, BusinessEntity.Account,
                LIMIT);
        checkDetailLimit(configs, userSelectedConfigs, AttrType.Custom, AttrSubType.Extension, BusinessEntity.Contact,
                LIMIT);
    }

    private void checkDetailLimit(List<AttrConfig> configs, List<AttrConfig> userSelectedConfigs, AttrType type,
            AttrSubType subType, BusinessEntity entity, int limit) {
        List<AttrConfig> list = configs.stream().filter(
                e -> type.equals(e.getAttrType()) && subType.equals(e.getAttrSubType()) && entity.equals(e.getEntity()))
                .collect(Collectors.toList());
        int number = list.size();
        if (number > limit) {
            userSelectedConfigs.forEach(e -> {
                if (type.equals(e.getAttrType()) && subType.equals(e.getAttrSubType())
                        && entity.equals(e.getEntity())) {
                    addErrorMsg(ValidationErrors.Type.EXCEED_SYSTEM_LIMIT,
                            String.format(ValidationMsg.Errors.EXCEED_SYSTEM_LIMIT, number, entity.name(), limit), e);
                }
            });
        }
    }

    @VisibleForTesting
    int getMaxPremiumLeadEnrichmentAttributesByLicense(String tenantId, DataLicense dataLicense) {
        String maxPremiumLeadEnrichmentAttributes;
        Camille camille = CamilleEnvironment.getCamille();
        Path contractPath = null;
        Path path = null;
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            contractPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace, PLS);
            if (dataLicense == null) {
                path = contractPath.append(DATA_CLOUD_LICENSE).append(MAX_ENRICH_ATTRIBUTES);
            } else {
                path = contractPath.append(DATA_CLOUD_LICENSE).append("/" + dataLicense.getDataLicense());
            }
            maxPremiumLeadEnrichmentAttributes = camille.get(path).getData();
        } catch (KeeperException.NoNodeException ex) {
            Path defaultConfigPath = null;
            if (dataLicense == null) {
                defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), PLS)
                        .append(new Path(DATA_CLOUD_LICENSE).append(new Path(MAX_ENRICH_ATTRIBUTES)));
            } else {
                defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), PLS)
                        .append(new Path(DATA_CLOUD_LICENSE).append(new Path("/" + dataLicense.getDataLicense())));
            }

            try {
                maxPremiumLeadEnrichmentAttributes = camille.get(defaultConfigPath).getData();
            } catch (Exception e) {
                throw new RuntimeException("Cannot get default value for maximum premium lead enrichment attributes ");
            }
            try {
                camille.upsert(path, DocumentUtils.toRawDocument(maxPremiumLeadEnrichmentAttributes),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } catch (Exception e) {
                throw new RuntimeException("Cannot update value for maximum premium lead enrichment attributes ");
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot get maximum premium lead enrichment attributes ", e);
        }
        return Integer.parseInt(maxPremiumLeadEnrichmentAttributes.replaceAll("\"", ""));
    }
}
