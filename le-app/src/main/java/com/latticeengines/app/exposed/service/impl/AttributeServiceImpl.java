package com.latticeengines.app.exposed.service.impl;

import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections4.Closure;
import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.download.DlFileHttpDownloader;
import com.latticeengines.app.exposed.entitymanager.SelectedAttrEntityMgr;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.app.exposed.service.FileDownloadService;
import com.latticeengines.app.exposed.util.FileDownloaderRegistry;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.DataLicense;
import com.latticeengines.domain.exposed.pls.LatticeInsightsDownloadConfig;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.SelectedAttribute;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component("selectedAttrService")
public class AttributeServiceImpl implements AttributeService {

    private static final Logger log = LoggerFactory.getLogger(AttributeServiceImpl.class);

    private static final String UNIQUE_CONSTRAINT_SELECTED_ATTRIBUTES = "UQ__SELECTED__";

    private static final int MAX_SAVE_RETRY = 5;

    private static final String MaxEnrichAttributes = "MaxEnrichAttributes";

    private static List<String> CSV_HEADERS = Arrays.asList("Attribute", "Category", "SubCategory", "Description",
            "Data Type", "Status", "Premium");

    private static final String KEY_SUFFIX = "Data_Pivoted_Source";

    @Inject
    private SelectedAttrEntityMgr selectedAttrEntityMgr;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private CommonTenantConfigServiceImpl appTenantConfigService;

    @Inject
    private AttributeCustomizationService attributeCustomizationService;

    @Inject
    private BatonService batonService;

    @Inject
    private FileDownloadService fileDownloadService;

    @PostConstruct
    private void postConstruct() {
        columnMetadataProxy.scheduleLoadColumnMetadataCache();
        FileDownloaderRegistry.register(this);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void save(LeadEnrichmentAttributesOperationMap attributes, Tenant tenant, Map<String, Integer> limitationMap,
            Boolean considerInternalAttributes) {
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        Integer premiumAttributeLimitation = getTotalMaxCount(limitationMap);
        checkAmbiguityInFieldNames(attributes);

        int existingSelectedAttributePremiumCount = getSelectedAttributePremiumCount(tenant,
                considerInternalAttributes);
        int additionalPremiumAttrCount = 0;
        List<LeadEnrichmentAttribute> allAttributes = getAttributes(tenant, null, null, null, false, null, null,
                considerInternalAttributes);

        final List<SelectedAttribute> addAttrList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(attributes.getSelectedAttributes())) {
            for (String selectedAttrStr : attributes.getSelectedAttributes()) {
                LeadEnrichmentAttribute selectedAttr = findEnrichmentAttributeByName(allAttributes, selectedAttrStr);
                SelectedAttribute attr = populateAttrObj(selectedAttr, tenant);
                addAttrList.add(attr);
                if (attr.getIsPremium()) {
                    additionalPremiumAttrCount++;
                }
            }
        }

        final List<SelectedAttribute> deleteAttrList = new ArrayList<>();

        if (!CollectionUtils.isEmpty(attributes.getDeselectedAttributes())) {
            for (String deselectedAttrStr : attributes.getDeselectedAttributes()) {
                LeadEnrichmentAttribute deselectedAttr = findEnrichmentAttributeByName(allAttributes,
                        deselectedAttrStr);
                SelectedAttribute attr = populateAttrObj(deselectedAttr, tenant);
                deleteAttrList.add(attr);
                if (attr.getIsPremium()) {
                    additionalPremiumAttrCount--;
                }
            }
        }

        // TODO - add check for per datasource limitation as well
        if (premiumAttributeLimitation < existingSelectedAttributePremiumCount + additionalPremiumAttrCount) {
            // throw exception if effective premium count crosses limitation
            throw new LedpException(LedpCode.LEDP_18112, new String[] { premiumAttributeLimitation.toString(), "HG" });
        }

        log.info("Saving enrichment attribute config changes. Additions:{}, Deletions:{}", addAttrList, deleteAttrList);
        DatabaseUtils.retry("saveEnrichmentAttributeSelection", MAX_SAVE_RETRY, ConstraintViolationException.class,
                "Ignoring ConstraintViolationException exception and retrying save operation",
                UNIQUE_CONSTRAINT_SELECTED_ATTRIBUTES, new Closure() {
                    @Override
                    public void execute(Object input) {
                        selectedAttrEntityMgr.upsert(addAttrList, deleteAttrList);
                    }
                });
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void saveSelectedAttribute(LeadEnrichmentAttributesOperationMap attributes, Tenant tenant,
            Map<String, Integer> limitationMap, Boolean considerInternalAttributes) {
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        checkAmbiguityInFieldNames(attributes);

        Map<String, Integer> existingSelectedAttributePremiumMap = getSelectedAttributePremiumMap(tenant,
                considerInternalAttributes);
        Map<String, Integer> additionalPremiumAttrMap = new HashMap<>();
        initAdditionalAttrMap(limitationMap, additionalPremiumAttrMap);
        List<LeadEnrichmentAttribute> allAttributes = getAttributes(tenant, null, null, null, false, null, null,
                considerInternalAttributes);

        final List<SelectedAttribute> addAttrList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(attributes.getSelectedAttributes())) {
            for (String selectedAttrStr : attributes.getSelectedAttributes()) {
                LeadEnrichmentAttribute selectedAttr = findEnrichmentAttributeByName(allAttributes, selectedAttrStr);
                SelectedAttribute attr = populateAttrObj(selectedAttr, tenant);
                addAttrList.add(attr);
                if (attr.getIsPremium()) {
                    String key = selectedAttr.getDataLicense() + KEY_SUFFIX;
                    Integer val = additionalPremiumAttrMap.get(key);
                    additionalPremiumAttrMap.put(key, val + 1);
                }
            }
        }

        final List<SelectedAttribute> deleteAttrList = new ArrayList<>();

        if (!CollectionUtils.isEmpty(attributes.getDeselectedAttributes())) {
            for (String deselectedAttrStr : attributes.getDeselectedAttributes()) {
                LeadEnrichmentAttribute deselectedAttr = findEnrichmentAttributeByName(allAttributes,
                        deselectedAttrStr);
                SelectedAttribute attr = populateAttrObj(deselectedAttr, tenant);
                deleteAttrList.add(attr);
                if (attr.getIsPremium()) {
                    String key = deselectedAttr.getDataLicense() + KEY_SUFFIX;
                    Integer val = additionalPremiumAttrMap.get(key);
                    additionalPremiumAttrMap.put(key, val - 1);
                }
            }
        }

        // TODO - add check for per datasource limitation as well
        Integer premiumAttributeLimitation;
        Integer existingSelectedAttributePremiumCount;
        Integer additionalPremiumAttrCount;
        Integer maxExistingSelectedAttributes = 0;
        for (String key : limitationMap.keySet()) {
            premiumAttributeLimitation = limitationMap.get(key);
            existingSelectedAttributePremiumCount = existingSelectedAttributePremiumMap.get(key) == null ? 0
                    : existingSelectedAttributePremiumMap.get(key);
            additionalPremiumAttrCount = additionalPremiumAttrMap.get(key);
            maxExistingSelectedAttributes += existingSelectedAttributePremiumCount + additionalPremiumAttrCount;
            if (premiumAttributeLimitation < existingSelectedAttributePremiumCount + additionalPremiumAttrCount) {
                // throw exception if effective premium count crosses limitation
                throw new LedpException(LedpCode.LEDP_18112, new String[] { premiumAttributeLimitation.toString(),
                        key.indexOf(KEY_SUFFIX) > 0 ? key.substring(0, key.indexOf(KEY_SUFFIX)) : key });
            }
        }
        Integer maxLimitation = limitationMap.get(MaxEnrichAttributes);
        if (maxExistingSelectedAttributes > maxLimitation) {
            throw new LedpException(LedpCode.LEDP_18112,
                    new String[] { maxLimitation.toString(), MaxEnrichAttributes });
        }

        DatabaseUtils.retry("saveEnrichmentAttributeSelection", MAX_SAVE_RETRY, ConstraintViolationException.class,
                "Ignoring ConstraintViolationException exception and retrying save operation",
                UNIQUE_CONSTRAINT_SELECTED_ATTRIBUTES, new Closure() {
                    @Override
                    public void execute(Object input) {
                        selectedAttrEntityMgr.upsert(addAttrList, deleteAttrList);
                    }
                });
    }

    @Override
    public List<LeadEnrichmentAttribute> getAttributes(Tenant tenant, String attributeDisplayNameFilter,
            Category category, String subcategory, Boolean onlySelectedAttributes, Integer offset, Integer max,
            Boolean considerInternalAttributes) {
        AttributePageProcessor processor = new AttributePageProcessor.Builder().columnMetadataProxy(columnMetadataProxy) //
                .batonService(batonService) //
                .selectedAttrEntityMgr(selectedAttrEntityMgr) //
                .attributeCustomizationService(attributeCustomizationService) //
                .attributeDisplayNameFilter(attributeDisplayNameFilter) //
                .category(category) //
                .subcategory(subcategory) //
                .onlySelectedAttributes(onlySelectedAttributes) //
                .offset(offset) //
                .max(max) //
                .considerInternalAttributes(considerInternalAttributes) //
                .build();

        return processor.getPage();
    }

    @Override
    public List<LeadEnrichmentAttribute> getAllAttributes() {
        AttributePageProcessor processor = new AttributePageProcessor.Builder().columnMetadataProxy(columnMetadataProxy) //
                .batonService(batonService) //
                .selectedAttrEntityMgr(selectedAttrEntityMgr) //
                .considerInternalAttributes(true) //
                .skipTenantLevelCustomization(true) //
                .build();
        return processor.getPage();
    }

    @Override
    public int getAttributesCount(Tenant tenant, String attributeDisplayNameFilter, Category category,
            String subcategory, Boolean onlySelectedAttributes, Boolean considerInternalAttributes) {
        List<LeadEnrichmentAttribute> matchingAttr = getAttributes(tenant, attributeDisplayNameFilter, category,
                subcategory, onlySelectedAttributes, null, null, considerInternalAttributes);
        return (matchingAttr == null) ? 0 : matchingAttr.size();
    }

    @Override
    public Integer getSelectedAttributeCount(Tenant tenant, Boolean considerInternalAttributes) {
        return getSelectedAttrCount(tenant, Boolean.FALSE, considerInternalAttributes);
    }

    @Override
    public Integer getSelectedAttributePremiumCount(Tenant tenant, Boolean considerInternalAttributes) {
        return getSelectedAttrCount(tenant, Boolean.TRUE, considerInternalAttributes);
    }

    @Override
    public Class<LatticeInsightsDownloadConfig> configClz() {
        return LatticeInsightsDownloadConfig.class;
    }

    @Override
    public String getDownloadAttrsToken(boolean isSelected) {
        LatticeInsightsDownloadConfig config = new LatticeInsightsDownloadConfig();
        config.setOnlySelectedAttrs(isSelected);
        return fileDownloadService.generateDownloadToken(config);
    }

    @Override
    public void downloadByConfig(LatticeInsightsDownloadConfig downloadConfig, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        Tenant tenant = MultiTenantContext.getTenant();
        Boolean considerInternalAttributes = shouldConsiderInternalAttributes(tenant);
        DateFormat dateFormat = DateTimeUtils.getSimpleDateFormatObj("MM-dd-yyyy");
        String dateString = dateFormat.format(new Date());
        String fileName = downloadConfig.isOnlySelectedAttrs()
                ? String.format("selectedEnrichmentAttributes_%s.csv", dateString)
                : String.format("enrichmentAttributes_%s.csv", dateString);
        downloadAttributes(request, response, "application/csv", fileName, tenant, downloadConfig.isOnlySelectedAttrs(),
                considerInternalAttributes);
    }

    private boolean shouldConsiderInternalAttributes(Tenant tenant) {
        CustomerSpace space = CustomerSpace.parse(tenant.getId());
        return batonService.isEnabled(space, LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES);
    }

    protected Integer getSelectedAttrCount(Tenant tenant, Boolean countOnlySelectedPremiumAttr,
            Boolean considerInternalAttributes) {
        DataCloudVersion dataCloudVersion = columnMetadataProxy.latestVersion(null);
        Set<String> premiumAttrs = columnMetadataProxy.premiumAttributes(dataCloudVersion.getVersion());

        List<LeadEnrichmentAttribute> attributeList = getAttributes(tenant, null, null, null, null, null, null,
                considerInternalAttributes);
        int count = 0;
        for (LeadEnrichmentAttribute attr : attributeList) {
            if (attr.getIsSelected()) {
                if (countOnlySelectedPremiumAttr == Boolean.TRUE) {
                    if (premiumAttrs.contains(attr.getColumnId())) {
                        count++;
                    }
                } else {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public Map<String, Integer> getSelectedAttributePremiumMap(Tenant tenant, Boolean considerInternalAttributes) {
        return getSelectedAttrMap(tenant, Boolean.TRUE, considerInternalAttributes);
    }

    protected Map<String, Integer> getSelectedAttrMap(Tenant tenant, Boolean countOnlySelectedPremiumAttr,
            Boolean considerInternalAttributes) {
        DataCloudVersion dataCloudVersion = columnMetadataProxy.latestVersion(null);
        Set<String> premiumAttrs = columnMetadataProxy.premiumAttributes(dataCloudVersion.getVersion());

        List<LeadEnrichmentAttribute> attributeList = getAttributes(tenant, null, null, null, null, null, null,
                considerInternalAttributes);
        Map<String, Integer> count = new HashMap<String, Integer>();
        for (LeadEnrichmentAttribute attr : attributeList) {
            if (attr.getIsSelected()) {
                if (countOnlySelectedPremiumAttr == Boolean.TRUE) {
                    if (premiumAttrs.contains(attr.getColumnId())) {
                        String key = attr.getDataLicense() + KEY_SUFFIX;
                        Integer val = count.get(key);
                        val = val == null ? 0 : val;
                        count.put(key, val + 1);
                    }
                } else {
                    String key = attr.getDataLicense() + KEY_SUFFIX;
                    Integer val = count.get(key);
                    val = val == null ? 0 : val;
                    count.put(key, val + 1);
                }
            }
        }
        return count;
    }

    @Override
    public Map<String, Integer> getPremiumAttributesLimitation(Tenant tenant) {
        Map<String, Integer> limitationMap = new HashMap<>();
        int premiumAttributesLimitation = appTenantConfigService.getMaxPremiumLeadEnrichmentAttributes(tenant.getId());
        // For future use case that we might have multiple sources of premium
        // attrs. Fow now only attributes from HG are marked as premium.
        limitationMap.put("HGData_Pivoted_Source", premiumAttributesLimitation);
        return limitationMap;
    }

    @Override
    public Map<String, Integer> getPremiumAttributesLimitationMap(Tenant tenant) {
        Map<String, Integer> limitationMap = new HashMap<>();
        for (DataLicense license : DataLicense.values()) {
            int attributeLimitation = appTenantConfigService
                    .getMaxPremiumLeadEnrichmentAttributesByLicense(tenant.getId(), license.getDataLicense());
            limitationMap.put(license.getDataLicense() + KEY_SUFFIX, attributeLimitation);
        }
        int maxLimitation = appTenantConfigService.getMaxPremiumLeadEnrichmentAttributesByLicense(tenant.getId(), null);
        limitationMap.put(MaxEnrichAttributes, maxLimitation);
        // For future use case that we might have multiple sources of premium
        // attrs. Fow now only attributes from HG and Bombora are marked as
        // premium.
        return limitationMap;
    }

    @Override
    public void downloadAttributes(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String fileName, Tenant tenant, Boolean isSelected, Boolean considerInternalAttributes) {
        AttributePageProcessor processor = new AttributePageProcessor.Builder().columnMetadataProxy(columnMetadataProxy) //
                .batonService(batonService) //
                .selectedAttrEntityMgr(selectedAttrEntityMgr) //
                .onlySelectedAttributes(isSelected) //
                .considerInternalAttributes(considerInternalAttributes) //
                .attributeCustomizationService(attributeCustomizationService) //
                .build();

        List<LeadEnrichmentAttribute> attributes = processor.getPage();
        DlFileHttpDownloader.DlFileDownloaderBuilder builder = new DlFileHttpDownloader.DlFileDownloaderBuilder();
        builder.setMimeType(mimeType).setFileName(fileName).setFileContent(getCSVFromAttributes(attributes))
                .setBatonService(batonService);
        DlFileHttpDownloader downloader = new DlFileHttpDownloader(builder);
        downloader.downloadFile(request, response);
    }

    @Override
    public LeadEnrichmentAttribute getAttribute(String fieldName) {
        List<LeadEnrichmentAttribute> matches = getAllAttributes().stream()
                .filter(a -> a.getFieldName().equals(fieldName)).collect(Collectors.toList());
        if (matches.size() == 0) {
            return null;
        }
        return matches.get(0);
    }

    @Override
    public List<LeadEnrichmentAttribute> getAttributesBaseOnCategory(Category category) {
        List<LeadEnrichmentAttribute> matches = getAllAttributes().stream()
                .filter(a -> a.getCategory().equals(category.getName())).collect(Collectors.toList());
        if (matches.size() == 0) {
            return null;
        }
        return matches;
    }

    @Override
    public List<LeadEnrichmentAttribute> getAttributesBaseOnSubCategory(Category category, String subCategory) {
        List<LeadEnrichmentAttribute> matches = getAllAttributes().stream()
                .filter(a -> a.getCategory().equals(category.getName()) && a.getSubcategory().equals(subCategory))
                .collect(Collectors.toList());
        if (matches.size() == 0) {
            return null;
        }
        return matches;
    }

    private String getCSVFromAttributes(List<LeadEnrichmentAttribute> attributes) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < CSV_HEADERS.size(); i++) {
            if (i + 1 < CSV_HEADERS.size()) {
                stringBuffer.append(modifyStringForCSV(CSV_HEADERS.get(i)) + ",");
            } else {
                stringBuffer.append(modifyStringForCSV(CSV_HEADERS.get(i)));
            }
        }
        stringBuffer.append("\r\n");

        for (LeadEnrichmentAttribute attribute : attributes) {
            stringBuffer.append(modifyStringForCSV(attribute.getDisplayName()) + ",");
            stringBuffer.append(modifyStringForCSV(attribute.getCategory()) + ",");
            stringBuffer.append(modifyStringForCSV(attribute.getSubcategory()) + ",");
            stringBuffer.append(modifyStringForCSV(attribute.getDescription()) + ",");
            stringBuffer.append(modifyStringForCSV(getDataTypeDisplay(attribute.getFieldJavaType())) + ",");
            stringBuffer.append(modifyStringForCSV(attribute.getIsSelected() ? "On" : "Off") + ",");
            stringBuffer.append(modifyStringForCSV(attribute.getIsPremium() ? "Yes" : "No"));
            stringBuffer.append("\r\n");
        }

        return stringBuffer.toString();
    }

    private String modifyStringForCSV(String value) {
        if (value == null) {
            return "";
        }
        return "\"" + value.replaceAll("\"", "\"\"") + "\"";
    }

    private String getDataTypeDisplay(String dataType) {
        String displayName;

        switch (dataType.toUpperCase()) {
        case "BOOLEAN":
            displayName = "Boolean";
            break;
        case "DOUBLE":
        case "INTEGER":
        case "LONG":
            displayName = "Number";
            break;
        case "STRING":
        default:
            displayName = "Text";
            break;
        }

        return displayName;
    }

    private LeadEnrichmentAttribute findEnrichmentAttributeByName(List<LeadEnrichmentAttribute> allAttributes,
            String selectedAttrStr) {
        for (LeadEnrichmentAttribute attr : allAttributes) {
            if (attr.getFieldName().equals(selectedAttrStr)) {
                return attr;
            }
        }

        throw new LedpException(LedpCode.LEDP_18114, new String[] { selectedAttrStr });
    }

    private SelectedAttribute populateAttrObj(LeadEnrichmentAttribute leadEnrichmentAttr, Tenant tenant) {
        SelectedAttribute attr = new SelectedAttribute(leadEnrichmentAttr.getFieldName(), tenant);
        attr.setIsPremium(leadEnrichmentAttr.getIsPremium());
        attr.setDataLicense(leadEnrichmentAttr.getDataLicense());
        return attr;
    }

    private Integer getTotalMaxCount(Map<String, Integer> limitationMap) {
        Integer totalCount = 0;
        for (String key : limitationMap.keySet()) {
            totalCount += limitationMap.get(key);
        }
        return totalCount;
    }

    private void initAdditionalAttrMap(Map<String, Integer> limitationMap,
            Map<String, Integer> additionalPremiumAttrMap) {

        for (String key : limitationMap.keySet()) {
            additionalPremiumAttrMap.put(key, 0);
        }
    }

    private void checkAmbiguityInFieldNames(LeadEnrichmentAttributesOperationMap attributes) {
        Set<String> attrSet = new HashSet<>();
        checkDuplicate(attrSet, attributes.getSelectedAttributes());
        checkDuplicate(attrSet, attributes.getDeselectedAttributes());
    }

    private void checkDuplicate(Set<String> attrSet, List<String> attributes) {
        if (!CollectionUtils.isEmpty(attributes)) {
            for (String attrStr : attributes) {
                if (attrSet.contains(attrStr)) {
                    throw new LedpException(LedpCode.LEDP_18113, new String[] { attrStr });
                }
                attrSet.add(attrStr);
            }
        }
    }

}
