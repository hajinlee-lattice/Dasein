package com.latticeengines.apps.core.service.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
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
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
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

    private List<AttrConfig> dbConfigs;


    protected LimitationValidator() {
        super(VALIDATOR_NAME);
    }

    public void setDBConfigs(List<AttrConfig> existingConfigs) {
        this.dbConfigs = existingConfigs;
    }

    @Override
    public void validate(List<AttrConfig> attrConfigs, boolean isAdmin) {
        // make sure user selected attr don't have two same attribute
        checkAmbiguityInFieldNames(attrConfigs);
        // split user selected configs into active and inactive, props always
        // are not empty after render method
        List<AttrConfig> userSelectedActiveConfigs = returnPropertyConfigs(attrConfigs, ColumnMetadataKey.State,
                AttrState.Active);
        List<AttrConfig> userSelectedInactiveConfigs = returnPropertyConfigs(attrConfigs, ColumnMetadataKey.State,
                AttrState.Inactive);


        List<AttrConfig> existingActiveConfigs = getPropertyConfigsInDB(dbConfigs, ColumnMetadataKey.State,
                AttrState.Active);

        // dedup, since the user selected configs has merged with the configs in
        // DB, no need to count same configs in DB
        List<AttrConfig> activeConfigs = generateDedupConfig(existingActiveConfigs, userSelectedActiveConfigs);
        checkDataLicense(activeConfigs, userSelectedInactiveConfigs, userSelectedActiveConfigs);
        checkSystemLimit(activeConfigs, userSelectedInactiveConfigs, userSelectedActiveConfigs);
    }

    private List<AttrConfig> generateDedupConfig(List<AttrConfig> existingActiveConfigs,
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



    private <T extends Serializable> List<AttrConfig> returnPropertyConfigs(List<AttrConfig> attrConfigs,
            String propName, T value) {
        List<AttrConfig> stateConfigs = new ArrayList<>();
        for (AttrConfig config : attrConfigs) {
            try {
                if (Boolean.TRUE.equals(config.getAttrProps().get(propName).isAllowCustomization())
                        && value.equals(config.getAttrProps().get(propName).getCustomValue())) {
                    stateConfigs.add(config);
                }
            } catch (NullPointerException e) {
                throw new LedpException(LedpCode.LEDP_40026, new String[] { config.getAttrName() });
            }
        }
        return stateConfigs;
    }

    private <T extends Serializable> List<AttrConfig> getPropertyConfigsInDB(List<AttrConfig> attrConfigs,
            String propName, T value) {
        List<AttrConfig> stateConfigs = new ArrayList<>();
        for (AttrConfig config : attrConfigs) {
            try {
                if (config.getAttrProps().get(propName) != null
                        && value.equals(config.getAttrProps().get(propName).getCustomValue())) {
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
            int limit = getMaxPremiumLeadEnrichmentAttributesByLicense(tenantId, license.getDataLicense());
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


    // check category limit
    private void checkSystemLimit(List<AttrConfig> configs, List<AttrConfig> userSelectedInactiveConfigs,
            List<AttrConfig> userSelectedActiveConfigs) {
        checkDetailSystemLimit(configs, userSelectedInactiveConfigs, userSelectedActiveConfigs,
                Category.ACCOUNT_ATTRIBUTES,
                DEFAULT_LIMIT);
        checkDetailSystemLimit(configs, userSelectedInactiveConfigs, userSelectedActiveConfigs,
                Category.CONTACT_ATTRIBUTES,
                DEFAULT_LIMIT);
    }

    private void checkDetailSystemLimit(List<AttrConfig> configs, List<AttrConfig> userSelectedInactiveConfigs,
            List<AttrConfig> userSelectedActiveConfigs, Category category, int limit) {
        List<AttrConfig> list = configs.stream().filter(
                e -> category.equals(e.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class)))
                .collect(Collectors.toList());
        List<AttrConfig> inactiveList = userSelectedInactiveConfigs.stream().filter(
                e -> category.equals(e.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class)))
                .collect(Collectors.toList());
        int number = list.size() - inactiveList.size();
        if (number > limit) {
            userSelectedActiveConfigs.forEach(config -> {
                if (category.equals(config.getPropertyFinalValue(ColumnMetadataKey.Category, Category.class))) {
                    addErrorMsg(ValidationErrors.Type.EXCEED_SYSTEM_LIMIT,
                            String.format(ValidationMsg.Errors.EXCEED_LIMIT, number, category.name(), limit), config);
                }
            });
        }
    }

    @VisibleForTesting
    public int getMaxPremiumLeadEnrichmentAttributesByLicense(String tenantId, String dataLicense) {
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
                path = contractPath.append(DATA_CLOUD_LICENSE).append("/" + dataLicense);
            }
            maxPremiumLeadEnrichmentAttributes = camille.get(path).getData();
        } catch (KeeperException.NoNodeException ex) {
            Path defaultConfigPath = null;
            if (dataLicense == null) {
                defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), PLS)
                        .append(new Path(DATA_CLOUD_LICENSE).append(new Path(MAX_ENRICH_ATTRIBUTES)));
            } else {
                defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(CamilleEnvironment.getPodId(), PLS)
                        .append(new Path(DATA_CLOUD_LICENSE).append(new Path("/" + dataLicense)));
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
