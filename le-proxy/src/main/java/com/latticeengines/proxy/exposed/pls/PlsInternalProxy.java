package com.latticeengines.proxy.exposed.pls;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.security.Tenant;

public interface PlsInternalProxy {

    ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid);

    boolean createTenant(Tenant tenant);

    boolean sendS3TemplateUpdateEmail(String tenantId, S3ImportEmailInfo emailInfo);

    boolean sendS3TemplateCreateEmail(String tenantId, S3ImportEmailInfo emailInfo);

    boolean sendS3ImportEmail(String result, String tenantId, S3ImportEmailInfo emailInfo);

    void sendMetadataSegmentExportEmail(String result, String tenantId, MetadataSegmentExport export);

    void sendAtlasExportEmail(String result, String tenantId, AtlasExport export);

    void sendOrphanRecordsExportEmail(String result, String tenantId, OrphanRecordsExportRequest export);

    void sendPlsEnrichInternalAttributeEmail(String result, String tenantId,
                                             AdditionalEmailInfo info);

    boolean sendCDLProcessAnalyzeEmail(String result, String tenantId, AdditionalEmailInfo info);

    void sendPlsScoreEmail(String result, String tenantId, AdditionalEmailInfo info);

    void sendPlsCreateModelEmail(String result, String tenantId, AdditionalEmailInfo info);

    void saveLeadEnrichmentAttributes(CustomerSpace customerSpace,
                                      LeadEnrichmentAttributesOperationMap attributes);

    List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace,
                                                              String attributeDisplayNameFilter, Category category,
                                                              Boolean onlySelectedAttributes);

    List<String> getLeadEnrichmentCategories(CustomerSpace customerSpace);

    List<String> getLeadEnrichmentSubcategories(CustomerSpace customerSpace, String category);

    Integer getSelectedAttributeCount(CustomerSpace customerSpace, Boolean considerInternalAttributes);

    Map<String, Integer> getPremiumAttributesLimitation(CustomerSpace customerSpace);

    Integer getSelectedAttributePremiumCount(CustomerSpace customerSpace, Boolean considerInternalAttributes);

    int getLeadEnrichmentAttributesCount(CustomerSpace customerSpace, String attributeDisplayNameFilter,
                                         Category category, String subcategory, Boolean onlySelectedAttributes,
                                         Boolean considerInternalAttributes);

    List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace,
                                                              String attributeDisplayNameFilter, Category category, String subcategory,
                                                              Boolean onlySelectedAttributes, Integer offset,
                                                              Integer max, Boolean considerInternalAttributes);

    List<LeadEnrichmentAttribute> getAllLeadEnrichmentAttributes();

    List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, String attributeDisplayNameFilter, Category category,
                                                              Boolean onlySelectedAttributes, Boolean considerInternalAttributes);

    List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, String attributeDisplayNameFilter, Category category,
                                                              String subcategory, Boolean onlySelectedAttributes, Boolean considerInternalAttributes);

    void sendUploadEmail(UploadEmailInfo uploadEmailInfo);
}
