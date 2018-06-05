package com.latticeengines.apps.core.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
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
    private static final int DEFAULT_LIMIT = 500;
    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    protected LimitationValidator() {
        super(VALIDATOR_NAME);
    }

    @Override
    public void validate(List<AttrConfig> attrConfigs, boolean isAdmin) {
        // make sure user selected attr don't have two same attribute
        checkAmbiguityInFieldNames(attrConfigs);
        String tenantId = MultiTenantContext.getTenantId();
        // split user selected configs into active and inactive, props always
        // are not empty after render method
        List<AttrConfig> userSelectedActiveConfigs = returnStateConfigs(attrConfigs, AttrState.Active);
        List<AttrConfig> userSelectedInactiveConfigs = returnStateConfigs(attrConfigs, AttrState.Inactive);

        List<AttrConfig> existingConfigs = attrConfigEntityMgr.findAllByTenantId(tenantId);
        List<AttrConfig> existingActiveConfigs = getStateConfigsInDB(existingConfigs);

        // dedup, since the configs has merged with the configs in DB, no need
        // to count same configs in DB
        List<AttrConfig> activeConfigs = generateActiveConfig(existingActiveConfigs, userSelectedActiveConfigs);
        checkDataLicense(activeConfigs, userSelectedInactiveConfigs, userSelectedActiveConfigs);
        checkSystemLimit(activeConfigs, userSelectedInactiveConfigs, userSelectedActiveConfigs);
    }

    private List<AttrConfig> generateActiveConfig(List<AttrConfig> existingActiveConfigs,
            List<AttrConfig> userSelectedActiveConfigs) {
        List<AttrConfig> result = new ArrayList<AttrConfig>();
        result.addAll(userSelectedActiveConfigs);
        Set<String> attrNames = userSelectedActiveConfigs.stream().map(config -> config.getAttrName())
                .collect(Collectors.toSet());
        existingActiveConfigs.forEach(config -> {
            if (!attrNames.contains(config.getAttrName())) {
                result.add(config);
            }
        });
        return result;
    }

    private List<AttrConfig> returnStateConfigs(List<AttrConfig> attrConfigs, AttrState state) {
        List<AttrConfig> stateConfigs = new ArrayList<>();
        for (AttrConfig config : attrConfigs) {
            try {
                if (Boolean.TRUE.equals(config.getAttrProps().get(ColumnMetadataKey.State).isAllowCustomization())
                        && state.equals(config.getAttrProps().get(ColumnMetadataKey.State).getCustomValue())) {
                    stateConfigs.add(config);
                }
            } catch (NullPointerException e) {
                throw new LedpException(LedpCode.LEDP_40026, new String[] { config.getAttrName() });
            }
        }
        return stateConfigs;
    }

    private List<AttrConfig> getStateConfigsInDB(List<AttrConfig> attrConfigs) {
        List<AttrConfig> stateConfigs = new ArrayList<>();
        for (AttrConfig config : attrConfigs) {
            try {
                if (config.getAttrProps().get(ColumnMetadataKey.State) != null
                        && AttrState.Active.equals(config.getAttrProps().get(ColumnMetadataKey.State).getCustomValue())) {
                    stateConfigs.add(config);
                }
            } catch (NullPointerException e) {
                throw new LedpException(LedpCode.LEDP_40027, new String[] { config.getAttrName() });
            }
        }
        return stateConfigs;
    }

    private void checkAmbiguityInFieldNames(List<AttrConfig> attrConfigs) {
        Set<String> attrSet = new HashSet<>();
        if (!CollectionUtils.isEmpty(attrConfigs)) {
            for (AttrConfig config : attrConfigs) {
                String attrName = config.getAttrName();
                if (attrSet.contains(attrName)) {
                    throw new LedpException(LedpCode.LEDP_18113, new String[] { attrName });
                }
                attrSet.add(attrName);
            }
        }

    }

    private void checkDataLicense(List<AttrConfig> configs, List<AttrConfig> userSelectedInactiveConfigs,
            List<AttrConfig> userSelectedActiveConfigs) {
        String tenantId = MultiTenantContext.getTenantId();
        int totalSelectedPremiumNumber = 0;
        for (DataLicense license : DataLicense.values()) {
            int limit = getMaxPremiumLeadEnrichmentAttributesByLicense(tenantId, license);
            List<AttrConfig> premiumActiveConfigs = configs.stream()
                    .filter(entity -> (license.getDataLicense().equals(entity.getDataLicense())))
                    .collect(Collectors.toList());
            List<AttrConfig> premiumInactiveConfigs = userSelectedInactiveConfigs.stream()
                    .filter(entity -> (license.getDataLicense().equals(entity.getDataLicense())))
                    .collect(Collectors.toList());

            int userSelectedNumber = premiumActiveConfigs.size() - premiumInactiveConfigs.size();
            if (limit < userSelectedNumber) {
                userSelectedActiveConfigs.forEach(e -> {
                    if (license.getDataLicense().equals(e.getDataLicense())) {
                        addErrorMsg(ValidationErrors.Type.EXCEED_DATA_LICENSE,
                                String.format(ValidationMsg.Errors.EXCEED_LIMIT, userSelectedNumber,
                                        license.getDescription(), limit),
                                e);
                    }
                });
            }
            totalSelectedPremiumNumber += userSelectedNumber;
        }
        int totalLimit = getMaxPremiumLeadEnrichmentAttributesByLicense(tenantId, null);
        int selected = totalSelectedPremiumNumber;
        if (selected > totalLimit) {
            userSelectedActiveConfigs.forEach(e -> {
                if (e.getDataLicense() != null) {
                    addErrorMsg(ValidationErrors.Type.EXCEED_SYSTEM_LIMIT,
                            String.format(ValidationMsg.Errors.EXCEED_LIMIT, selected, EXPORT, totalLimit), e);
                }
            });
        }
    }

    private void checkSystemLimit(List<AttrConfig> configs, List<AttrConfig> userSelectedInactiveConfigs,
            List<AttrConfig> userSelectedActiveConfigs) {
        checkDetailLimit(configs, userSelectedInactiveConfigs, userSelectedActiveConfigs, AttrType.Custom,
                AttrSubType.Extension, BusinessEntity.Account,
                DEFAULT_LIMIT);
        checkDetailLimit(configs, userSelectedInactiveConfigs, userSelectedActiveConfigs, AttrType.Custom,
                AttrSubType.Extension, BusinessEntity.Contact,
                DEFAULT_LIMIT);
    }

    private void checkDetailLimit(List<AttrConfig> configs, List<AttrConfig> userSelectedInactiveConfigs,
            List<AttrConfig> userSelectedActiveConfigs, AttrType type,
            AttrSubType subType, BusinessEntity entity, int limit) {
        List<AttrConfig> list = configs.stream().filter(
                e -> type.equals(e.getAttrType()) && subType.equals(e.getAttrSubType()) && entity.equals(e.getEntity()))
                .collect(Collectors.toList());
        List<AttrConfig> inactiveList = userSelectedInactiveConfigs.stream().filter(
                e -> type.equals(e.getAttrType()) && subType.equals(e.getAttrSubType()) && entity.equals(e.getEntity()))
                .collect(Collectors.toList());
        int number = list.size() - inactiveList.size();
        if (number > limit) {
            userSelectedActiveConfigs.forEach(config -> {
                if (type.equals(config.getAttrType()) && subType.equals(config.getAttrSubType())
                        && entity.equals(config.getEntity())) {
                    addErrorMsg(ValidationErrors.Type.EXCEED_SYSTEM_LIMIT,
                            String.format(ValidationMsg.Errors.EXCEED_LIMIT, number, entity.name(), limit), config);
                }
            });
        }
    }

    @VisibleForTesting
    public int getMaxPremiumLeadEnrichmentAttributesByLicense(String tenantId, DataLicense dataLicense) {
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
