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
                "activities_desc", //
                "activities_language_desc", //
                "activities_language_code", //
                "dunscontrolstatus_isdelisted", //
                "dunscontrolstatus_ismailundeliverable", //
                "dunscontrolstatus_ismarketable", //
                "dunscontrolstatus_istelephonedisconnected", //
                "dunscontrolstatus_operatingstatus_desc", //
                "dunscontrolstatus_operatingstatus_code", //
                "dunscontrolstatus_subjecthandling_desc", //
                "dunscontrolstatus_subjecthandling_code", //
                "email_addr", //
                "isnonclassifiedestablishment", //
                "primaryaddr_country_isoalpha2code", //
                "primaryaddr_country_name", //
                "primaryaddr_county_name", //
                "primaryaddr_addrlocality_name", //
                "primaryaddr_addrregion_abbreviatedname", //
                "primaryaddr_addrregion_name", //
                "primaryaddr_continentalregion_name", //
                "primaryaddr_isregisteredaddr", //
                "primaryaddr_language_desc", //
                "primaryaddr_language_code", //
                "primaryaddr_minortownname", //
                "primaryaddr_postalcode", //
                "primaryaddr_postalcodeposition_desc", //
                "primaryaddr_postalcodeposition_code", //
                "primaryaddr_postofficebox_postofficeboxnumber", //
                "primaryaddr_postofficebox_typedesc", //
                "primaryaddr_postofficebox_typecode", //
                "primaryaddr_street_line1", //
                "primaryaddr_street_line2", //
                "primaryaddr_streetname", //
                "primaryaddr_streetnumber", //
                "primaryindcode_ussicv4", //
                "primaryindcode_ussicv4desc", //
                "registrationnumbers_ispreferredregistrationnumber", //
                "registrationnumbers_registrationnumber", //
                "registrationnumbers_typedesc", //
                "registrationnumbers_typecode", //
                "stockexchanges_exchangename_desc", //
                "stockexchanges_isprimary", //
                "stockexchanges_tickername", //
                "telephone_isdcode", //
                "telephone_telephonenumber", //
                "tradestylenames_name", //
                "tradestylenames_priority", //
                "unspsccodes_code", //
                "unspsccodes_desc", //
                "unspsccodes_priority", //
                "website_domainname", //
                "website_url", //
                "banks_addr_addrlocality_name", //
                "banks_addr_addrregion_name", //
                "banks_addr_postalcode", //
                "banks_addr_street_line1", //
                "banks_addr_street_line2", //
                "banks_name", //
                "banks_registrationnumbers_registrationnumber", //
                "banks_telephone_telephonenumber", //
                "businessentitytype_desc", //
                "businessentitytype_code", //
                "chartertype_desc", //
                "chartertype_code", //
                "controlownershipdate", //
                "controlownershiptype_desc", //
                "controlownershiptype_code", //
                "dunscontrolstatus_operatingstatus_startdate", //
                "dunscontrolstatus_recordclass_desc", //
                "dunscontrolstatus_recordclass_code", //
                "fin_fintatementduration", //
                "fin_fintatementtodate", //
                "fin_infoscopedesc", //
                "fin_infoscopecode", //
                "fin_reliabilitydesc", //
                "fin_reliabilitycode", //
                "fin_unitcode", //
                "fin_yrrevenue_currency", //
                "fin_yrrevenue_val", //
                "franchiseoperationtype_desc", //
                "franchiseoperationtype_code", //
                "incorporateddate", //
                "indcodes_code", //
                "indcodes_desc", //
                "indcodes_priority", //
                "indcodes_typedesc", //
                "indcodes_typecode", //
                "isagent", //
                "isexporter", //
                "isforbeslargestprivatecompanieslisted", //
                "isfortune1000listed", //
                "isimporter", //
                "isstandalone", //
                "legalform_desc", //
                "legalform_code", //
                "legalform_registrationlocation_addrregion", //
                "legalform_startdate", //
                "mailingaddr_country_isoalpha2code", //
                "mailingaddr_country_name", //
                "mailingaddr_county_name", //
                "mailingaddr_addrlocality_name", //
                "mailingaddr_addrregion_abbreviatedname", //
                "mailingaddr_addrregion_name", //
                "mailingaddr_continentalregion_name", //
                "mailingaddr_language_desc", //
                "mailingaddr_language_code", //
                "mailingaddr_minortownname", //
                "mailingaddr_postalcode", //
                "mailingaddr_postalcodeposition_desc", //
                "mailingaddr_postalcodeposition_code", //
                "mailingaddr_postalroute", //
                "mailingaddr_postofficebox_postofficeboxnumber", //
                "mailingaddr_postofficebox_typedesc", //
                "mailingaddr_postofficebox_typecode", //
                "mailingaddr_street_line1", //
                "mailingaddr_street_line2", //
                "mailingaddr_streetname", //
                "mailingaddr_streetnumber", //
                "numberofemployees_employeecategories_employmentbasisdesc", //
                "numberofemployees_employeecategories_employmentbasiscode", //
                "numberofemployees_employeefiguresdate", //
                "numberofemployees_infoscopedesc", //
                "numberofemployees_infoscopecode", //
                "numberofemployees_reliabilitydesc", //
                "numberofemployees_reliabilitycode", //
                "numberofemployees_trend_gr", //
                "numberofemployees_trend_reliabilitydesc", //
                "numberofemployees_trend_reliabilitycode", //
                "numberofemployees_trend_timeperiod_desc", //
                "numberofemployees_trend_timeperiod_code", //
                "numberofemployees_trend_val", //
                "numberofemployees_val", //
                "primaryaddr_geographicalprecision_desc", //
                "primaryaddr_geographicalprecision_code", //
                "primaryaddr_ismanufacturinglocation", //
                "primaryaddr_latitude", //
                "primaryaddr_locationownership_desc", //
                "primaryaddr_locationownership_code", //
                "primaryaddr_longitude", //
                "primaryaddr_premisesarea_measurement", //
                "primaryaddr_premisesarea_reliabilitydesc", //
                "primaryaddr_premisesarea_reliabilitycode", //
                "primaryaddr_premisesarea_unitdesc", //
                "primaryaddr_premisesarea_unitcode", //
                "primaryaddr_statisticalarea_economicareaofinfluencecode", //
                "registeredaddr_country_isoalpha2code", //
                "registeredaddr_country_name", //
                "registeredaddr_county_name", //
                "registeredaddr_addrlocality_name", //
                "registeredaddr_addrregion_abbreviatedname", //
                "registeredaddr_addrregion_name", //
                "registeredaddr_language_desc", //
                "registeredaddr_language_code", //
                "registeredaddr_minortownname", //
                "registeredaddr_postalcode", //
                "registeredaddr_postalcodeposition_desc", //
                "registeredaddr_postalcodeposition_code", //
                "registeredaddr_postofficebox_postofficeboxnumber", //
                "registeredaddr_postofficebox_typedesc", //
                "registeredaddr_postofficebox_typecode", //
                "registeredaddr_street_line1", //
                "registeredaddr_street_line2", //
                "registeredaddr_street_line3", //
                "registeredaddr_street_line4", //
                "registeredaddr_streetname", //
                "registeredaddr_streetnumber", //
                "registered_legalform_desc", //
                "registered_legalform_code", //
                "registered_legalform_registrationstatus_desc", //
                "registered_legalform_registrationstatus_code", //
                "registeredname", //
                "startdate" //
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
                if (recordType != null && recordType != DataRecordType.Analytical) {
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
