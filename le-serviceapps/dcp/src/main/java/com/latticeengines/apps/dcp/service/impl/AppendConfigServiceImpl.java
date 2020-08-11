package com.latticeengines.apps.dcp.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.Id.baseinfo;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.Id.companyinfo;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.Id.entityresolution;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel.L1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.apps.dcp.service.AppendConfigService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusAppendConfig;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.remote.exposed.service.IDaaSService;
import com.latticeengines.security.exposed.service.TenantService;

@Service
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class AppendConfigServiceImpl implements AppendConfigService {

    private static final Logger log = LoggerFactory.getLogger(AppendConfigServiceImpl.class);

    private static final Set<String> UNSUPPORTED_BLOCKS = ImmutableSet.<String>builder() //
            .add("baseinfo", "companyentityresolution", "companynews", "industryprofile").build();

    @Inject
    private TenantService tenantService;

    @Inject
    private AppendConfigServiceImpl _self;

    @Inject
    private IDaaSService iDaaSService;

    @Override
    public DataBlockEntitlementContainer getEntitlement(String customerSpace) {
        String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace);
        return  _self.getTenantEntitlementFromCache(tenantId);
    }

    @Override
    public DplusAppendConfig getAppendConfig(String customerSpace, String sourceId) {
        // FIXME: to be changed to based on customer's selection
        List<String> elementIds = Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "countryisoalpha2code", //
                "tradestylenames_name", //
                "primaryaddr_street_line1", //
                "primaryaddr_street_line2", //
                "primaryaddr_addrlocality_name", //
                "primaryaddr_addrregion_name", //
                "primaryaddr_postalcode", //
                "primaryaddr_country_name", //
                "telephone_telephonenumber", //
                "primaryindcode_ussicv4" //
        );
        DplusAppendConfig appendConfig = new DplusAppendConfig();
        appendConfig.setElementIds(elementIds);
        return appendConfig;
    }

    @Cacheable(cacheNames = CacheName.Constants.IDaaSEntitlementCacheName, //
            key = "T(java.lang.String).format(\"%s|entitlement\", #tenantId)", unless = "#result == null")
    public DataBlockEntitlementContainer getTenantEntitlementFromCache(String tenantId) {
        DataBlockEntitlementContainer container = null;
        Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(tenantId).toString());
        Preconditions.checkNotNull(tenant, "No tenant with short id " + tenantId);
        String subsriberNumber = tenant.getSubscriberNumber();
        if (StringUtils.isNotBlank(subsriberNumber)) {
            container = getSubscriberEntitlement(subsriberNumber);
        } else {
            log.warn("Tenant {} does not have a subscriber number", subsriberNumber);
        }
        return (container == null) ? getDefaultEntitlement() : container;
    }

    DataBlockEntitlementContainer getSubscriberEntitlement(@NotNull String subsriberNumber) {
        try {
            String response = iDaaSService.getEntitlement(subsriberNumber);
            return parseIDaaSEntitlement(response);
        } catch (RemoteLedpException e) {
            if (e.getRemoteStackTrace().contains("\"code\":\"IEC-AM-0001\"")) {
                // {"code":"IEC-AM-0001","message":"Subscriber/Contract doesnt exists in the system"}
                return null;
            }
            throw e;
        }
    }

    static DataBlockEntitlementContainer getDefaultEntitlement() {
        List<DataBlockEntitlementContainer.Block> blocks = new ArrayList<>();
        blocks.add(new DataBlockEntitlementContainer.Block(baseinfo, L1));
        blocks.add(new DataBlockEntitlementContainer.Block(entityresolution,L1));
        blocks.add(new DataBlockEntitlementContainer.Block(companyinfo, L1));
        Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> types = new HashMap<>();
        types.put(DataRecordType.Domain, blocks);
        DataBlockEntitlementContainer.Domain domain = //
                new DataBlockEntitlementContainer.Domain(DataDomain.SalesMarketing, types);
        return new DataBlockEntitlementContainer(Collections.singletonList(domain));
    }

    static DataBlockEntitlementContainer parseIDaaSEntitlement(String idaasResponse) {
        JsonNode jsonNode = JsonUtils.deserialize(idaasResponse, JsonNode.class);
        List<DataBlockEntitlementContainer.Domain> domains = new ArrayList<>();
        if (hasNonEmptyArray(jsonNode, "products")) {
            for (JsonNode productNode : jsonNode.get("products")) {
                DataBlockEntitlementContainer.Domain domain = parseProduct(productNode);
                if (domain != null) {
                    domains.add(domain);
                }
            }
        }
        if (domains.isEmpty()) {
            log.warn("IDaaS response is parsed to null entitlement: {}", idaasResponse);
            return null;
        } else {
            return new DataBlockEntitlementContainer(domains);
        }
    }

    private static DataBlockEntitlementContainer.Domain parseProduct(JsonNode productNode) {
        String productName = productNode.get("name").asText();
        DataDomain domain = parseDataDomain(productName);
        Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> types = new HashMap<>();
        if (domain != null && hasNonEmptyArray(productNode, "packages")) {
            for (JsonNode packageNode: productNode.get("packages")) {
                String packageName = packageNode.get("name").asText();
                DataRecordType recordType = parseDataRecordType(packageName);
                if (recordType != null) {
                    types.put(recordType, parseEntitlements(packageNode));
                }
            }
        }
        if (types.isEmpty()) {
            return null;
        } else {
            return new DataBlockEntitlementContainer.Domain(domain, types);
        }
    }

    private static List<DataBlockEntitlementContainer.Block> parseEntitlements(JsonNode packageNode) {
        List<DataBlockEntitlementContainer.Block> blocks = new ArrayList<>();
        blocks.add(new DataBlockEntitlementContainer.Block(baseinfo, L1));
        blocks.add(new DataBlockEntitlementContainer.Block(entityresolution, L1));
        if (hasNonEmptyArray(packageNode, "entitlements")) {
            for (JsonNode entitlementNode : packageNode.get("entitlements")) {
                String status = entitlementNode.get("status").asText();
                if ("Active".equalsIgnoreCase(status) && hasNonEmptyArray(entitlementNode, "allowed_levels")) {
                    String canonicalName = entitlementNode.get("canonical_name").asText();
                    String blockId = parseBlockId(canonicalName);
                    if (StringUtils.isNotBlank(blockId)) {
                        List<DataBlockLevel> levels = new ArrayList<>();
                        for (JsonNode levelNode : entitlementNode.get("allowed_levels")) {
                            String levelVal = levelNode.asText();
                            levels.add(DataBlockLevel.parseDataBlockLevel(levelVal));
                        }
                        blocks.add(new DataBlockEntitlementContainer.Block(blockId, levels));
                    }
                }
            }
        }
        return blocks;
    }

    private static boolean hasNonEmptyArray(JsonNode node, String field) {
        return node.has(field) && node.get(field).size() > 0;
    }

    private static String parseBlockId(String canonicalName) {
        if (UNSUPPORTED_BLOCKS.contains(canonicalName.toLowerCase())) {
            return null;
        } else if (DataBlock.blockNameMap.containsKey(canonicalName.toLowerCase())) {
            return canonicalName.toLowerCase();
        } else {
            log.warn("Unknown block canonical name [{}]", canonicalName);
            return null;
        }
    }

    private static DataDomain parseDataDomain(String productName) {
        String productNameLower = productName.toLowerCase();
        if (productNameLower.contains("sales") && productNameLower.contains("marketing")) {
            return DataDomain.SalesMarketing;
        } else if (productNameLower.contains("finance")) {
            return DataDomain.Finance;
        } else if (productNameLower.contains("supply")) {
            return DataDomain.Supply;
        } else if (productNameLower.contains("compliance")) {
            return DataDomain.Compliance;
        } else if (productNameLower.contains("master")) {
            return DataDomain.EnterpriseMasterData;
        } else {
            log.warn("Unknown product name [{}]", productName);
            return null;
        }
    }

    private static DataRecordType parseDataRecordType(String packageName) {
        if (packageName.toLowerCase().contains("master")) {
            return DataRecordType.MasterData;
        } else if (packageName.toLowerCase().contains("analytical")) {
            return DataRecordType.Analytical;
        } else if (packageName.toLowerCase().contains("domain")) {
            return DataRecordType.Domain;
        } else {
            log.warn("Unknown package name [{}]", packageName);
            return null;
        }
    }

}
