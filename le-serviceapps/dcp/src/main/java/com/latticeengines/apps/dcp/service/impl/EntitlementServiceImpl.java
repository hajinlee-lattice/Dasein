package com.latticeengines.apps.dcp.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.Id.baseinfo;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.Id.companyinfo;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.Id.entityresolution;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel.L1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.apps.dcp.service.EntitlementService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.exception.RemoteLedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.remote.exposed.service.IDaaSService;
import com.latticeengines.security.exposed.service.TenantService;

@Service
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class EntitlementServiceImpl implements EntitlementService {

    private static final Logger log = LoggerFactory.getLogger(EntitlementServiceImpl.class);

    private static final Set<String> UNSUPPORTED_BLOCKS = ImmutableSet.<String> builder() //
            .add("baseinfo", "companyentityresolution", "companynews", "industryprofile").build();

    @Inject
    private TenantService tenantService;

    @Inject
    private EntitlementServiceImpl _self;

    @Inject
    private IDaaSService iDaaSService;

    @Value("${dcp.entitlement.allow.param.null}")
    private boolean allowNull;

    @Override
    public DataBlockEntitlementContainer getEntitlement(String customerSpace, String domainName, String recordType) {
        String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace);
        log.info("Getting entitlements for Tenant " + tenantId);
        DataBlockEntitlementContainer result = _self.getTenantEntitlementFromCache(tenantId, domainName, recordType);
        log.info("Returning entitlements: " + JsonUtils.serialize(result));
        return result;
    }

    static DataBlockEntitlementContainer filterDataBlockContainer(DataBlockEntitlementContainer container,
            String domainName, String recordType) {
        if (container == null || ("ALL".equals(domainName) && "ALL".equals(recordType))) {
            return container;
        } else {
            List<DataBlockEntitlementContainer.Domain> filteredDomains = new ArrayList<>();

            for (DataBlockEntitlementContainer.Domain domain : container.getDomains()) {
                if (!"ALL".equals(domainName) && !domain.getDomain().equals(DataDomain.parse(domainName))) {
                    continue;
                }

                if ("ALL".equals(recordType)) {
                    filteredDomains.add(domain);
                } else {
                    Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> filteredRecords = domain
                            .getRecordTypes().entrySet().stream()
                            .filter(entry -> entry.getKey().equals(DataRecordType.parse(recordType)))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    filteredDomains.add(new DataBlockEntitlementContainer.Domain(domain.getDomain(), filteredRecords));
                }
            }

            return new DataBlockEntitlementContainer(filteredDomains);
        }
    }

    @Override
    public boolean checkEntitledWith(String customerSpace, DataDomain dataDomain, DataRecordType dataRecordType,
            String blockName) {
        Preconditions.checkNotNull(dataDomain);
        Preconditions.checkNotNull(dataRecordType);
        Preconditions.checkArgument(StringUtils.isNotEmpty(blockName));
        String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace);
        Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(tenantId).toString());
        Preconditions.checkNotNull(tenant, "No tenant with short id " + tenantId);
        String subscriberNumber = tenant.getSubscriberNumber();
        if (StringUtils.isNotBlank(subscriberNumber)) {
            try {
                DataBlockEntitlementContainer entitlement = //
                        _self.getTenantEntitlementFromCache(tenantId, dataDomain.name(), dataRecordType.name());
                if (entitlement != null) {
                    for (DataBlockEntitlementContainer.Domain domain : entitlement.getDomains()) {
                        if (!dataDomain.equals(domain.getDomain())) {
                            continue;
                        }
                        if (!domain.getRecordTypes().containsKey(dataRecordType) || //
                                CollectionUtils.isEmpty(domain.getRecordTypes().get(dataRecordType))) {
                            continue;
                        }
                        for (DataBlockEntitlementContainer.Block block : domain.getRecordTypes().get(dataRecordType)) {
                            if (blockName.equals(block.getBlockId())) {
                                return true;
                            }
                        }
                    }
                }
                log.info("Subscriber {} is not entitled to {} and {}: {}", //
                        subscriberNumber, dataDomain, dataRecordType, JsonUtils.serialize(entitlement));
                return false;
            } catch (Exception e) {
                log.warn("Cannot get entitlement for subscriberNumber: " + subscriberNumber);
                return false;
            }
        } else {
            log.warn("Tenant {} does not have a subscriber number", tenantId);
            return allowNull;
        }
    }

    @Cacheable(cacheNames = CacheName.Constants.IDaaSEntitlementCacheName, //
            key = "T(java.lang.String).format(\"%s|%s|%s|entitlement\", #tenantId, #domainName, #recordType)", //
            unless = "#result == null")
    public DataBlockEntitlementContainer getTenantEntitlementFromCache(String tenantId, String domainName,
            String recordType) {
        DataBlockEntitlementContainer container = null;
        Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(tenantId).toString());
        Preconditions.checkNotNull(tenant, "No tenant with short id " + tenantId);
        String subsriberNumber = tenant.getSubscriberNumber();
        if (StringUtils.isNotBlank(subsriberNumber)) {
            container = getSubscriberEntitlement(subsriberNumber);
        } else {
            log.warn("Tenant {} does not have a subscriber number", tenantId);
        }
        return filterDataBlockContainer(((container == null) ? getDefaultEntitlement() : container), domainName,
                recordType);
    }

    DataBlockEntitlementContainer getSubscriberEntitlement(@NotNull String subsriberNumber) {
        try {
            String response = iDaaSService.getEntitlement(subsriberNumber);
            log.info("IDaaS entitlements: " + response);
            DataBlockEntitlementContainer container = parseIDaaSEntitlement(response);
            return filterFinancialDataBlockLevels(container);
        } catch (RemoteLedpException e) {
            if (e.getRemoteStackTrace().contains("\"code\":\"IEC-AM-0001\"")) {
                // {"code":"IEC-AM-0001","message":"Subscriber/Contract doesnt exists in the
                // system"}
                return null;
            } else if (e.getRemoteStackTrace().contains("\"code\":\"IEC_SM_0015\"")) {
                // {"code":"IEC_SM_0015","message":"Subscriber doesnt exists in the system"}
                return null;
            }
            throw e;
        }
    }

    static DataBlockEntitlementContainer filterFinancialDataBlockLevels(DataBlockEntitlementContainer container) {
        List<DataBlockEntitlementContainer.Domain> domainList = new ArrayList<>();

        for (DataBlockEntitlementContainer.Domain domain : container.getDomains()) {
            EnumMap<DataRecordType, List<DataBlockEntitlementContainer.Block>> filteredMap = new EnumMap<>(
                    DataRecordType.class);
            for (EnumMap.Entry<DataRecordType, List<DataBlockEntitlementContainer.Block>> entry : domain
                    .getRecordTypes().entrySet()) {

                List<DataBlockEntitlementContainer.Block> filteredBlocks = new ArrayList<>();

                for (DataBlockEntitlementContainer.Block block : entry.getValue()) {
                    if (DataBlock.Id.companyfinancials.equals(block.getBlockId())) {
                        List<DataBlockLevel> filteredLevels = new ArrayList<>();
                        for (DataBlockLevel level : block.getDataBlockLevels()) {
                            if (level.equals(L1)) {
                                filteredLevels.add(level);
                            }
                        }

                        filteredBlocks.add(new DataBlockEntitlementContainer.Block(block.getBlockId(), filteredLevels));
                    } else {
                        filteredBlocks.add(block);
                    }
                }

                filteredMap.put(entry.getKey(), filteredBlocks);
            }

            domainList.add(new DataBlockEntitlementContainer.Domain(domain.getDomain(), filteredMap));
        }
        return new DataBlockEntitlementContainer(domainList);
    }

    static DataBlockEntitlementContainer getDefaultEntitlement() {
        List<DataBlockEntitlementContainer.Block> blocks = new ArrayList<>();
        blocks.add(new DataBlockEntitlementContainer.Block(baseinfo, L1));
        blocks.add(new DataBlockEntitlementContainer.Block(entityresolution, L1));
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
            domains.removeIf(domain -> domain.getRecordTypes().isEmpty());
            return new DataBlockEntitlementContainer(domains);
        }
    }

    private static DataBlockEntitlementContainer.Domain parseProduct(JsonNode productNode) {
        String productName = productNode.get("name").asText();
        DataDomain domain = parseDataDomain(productName);
        Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> types = new EnumMap<>(DataRecordType.class);
        if (domain != null && hasNonEmptyArray(productNode, "packages")) {
            for (JsonNode packageNode : productNode.get("packages")) {
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
            types.remove(DataRecordType.Analytical);
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
