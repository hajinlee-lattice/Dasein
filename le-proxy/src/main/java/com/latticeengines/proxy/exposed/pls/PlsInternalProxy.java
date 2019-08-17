package com.latticeengines.proxy.exposed.pls;

import java.util.List;
import java.util.Map;

import org.springframework.lang.NonNull;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportEmailInfo;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AdditionalEmailInfo;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;

public interface PlsInternalProxy {

    ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid);

    void sendS3TemplateUpdateEmail(String tenantId, S3ImportEmailInfo emailInfo);

    void sendS3TemplateCreateEmail(String tenantId, S3ImportEmailInfo emailInfo);

    void sendS3ImportEmail(String result, String tenantId, S3ImportEmailInfo emailInfo);

    List<Job> findJobsBasedOnActionIdsAndType(@NonNull String customerSpace, List<Long> actionPids,
                                              ActionType actionType);

    void sendMetadataSegmentExportEmail(String result, String tenantId, MetadataSegmentExport export);

    MetadataSegmentExport getMetadataSegmentExport(CustomerSpace customerSpace, String exportId);

    void sendAtlasExportEmail(String result, String tenantId, AtlasExport export);

    MetadataSegmentExport updateMetadataSegmentExport(CustomerSpace customerSpace, //
                                                      String exportId, MetadataSegmentExport.Status state);

    void copyNotes(String fromModelSummaryId, String toModelSummaryId);

    void createNote(String modelId, NoteParams noteParams);

    void sendOrphanRecordsExportEmail(String result, String tenantId, OrphanRecordsExportRequest export);

    void sendPlsEnrichInternalAttributeEmail(String result, String tenantId,
                                             AdditionalEmailInfo info);

    void sendCDLProcessAnalyzeEmail(String result, String tenantId, AdditionalEmailInfo info);

    void sendPlsScoreEmail(String result, String tenantId, AdditionalEmailInfo info);

    void sendPlsCreateModelEmail(String result, String tenantId, AdditionalEmailInfo info);

    void registerReport(Report report, String tenantId);

    void updateSourceFile(SourceFile sourceFile, String tenantId);

    Report findReportByName(String name, String tenantId);

    SourceFile findSourceFileByName(String name, String tenantId);

    void createSourceFile(SourceFile sourceFile, String tenantId);

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

}
