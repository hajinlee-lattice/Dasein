package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.DeleteRequest;
import com.latticeengines.domain.exposed.cdl.ImportFileInfo;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.TemplateFieldPreview;
import com.latticeengines.domain.exposed.query.EntityType;

public interface CDLService {

    ApplicationId processAnalyze(String string, ProcessAnalyzeRequest request);

    ApplicationId submitCSVImport(String customerSpace, String templateFileName,
                                  String dataFileName, String source, String entity, String feedType);

    String createS3Template(String customerSpace, String templateFileName, String source,
                            String entity, String feedType, String subType, String displayName);

    ApplicationId submitS3ImportWithTemplateData(String customerSpace, String taskId, String templateFileName);

    ApplicationId submitS3ImportOnlyData(String customerSpace, String taskId, String dataFileName);

    void importFileToS3(String customerSpace, String templateFileName, String s3Path);

    UIAction delete(DeleteRequest deleteRequest);

    UIAction cleanup(String customerSpace, String sourceFileName, SchemaInterpretation schemaInterpretation,
                     CleanupOperationType cleanupOperationType);

    ApplicationId cleanupByTimeRange(String customerSpace, String startTime, String endTime,
                                     SchemaInterpretation schemaInterpretation);

    ApplicationId cleanupAllData(String customerSpace, SchemaInterpretation schemaInterpretation);

    void replaceData(String customerSpace, SchemaInterpretation schemaInterpretation);

    List<S3ImportTemplateDisplay> getS3ImportTemplate(String string, String sortBy, Set<EntityType> excludeTypes);

    /**
     *
     * @param customerSpace: TenantId
     * @param s3Path: Full path: eg: {bucket}/dropfolder/{dropbox}/Templates/DefaultSystem_AccountData/
     * @param filter: file extension
     * @return List of {@link FileProperty}
     */
    List<FileProperty> getFileListForS3Path(String customerSpace, String s3Path, String filter);

    String createS3ImportSystem(String customerSpace, String systemDisplayName, S3ImportSystem.SystemType systemType,
                              Boolean primary);

    S3ImportSystem getS3ImportSystem(String customerSpace, String systemName);

    S3ImportSystem getDefaultImportSystem(String customerSpace);

    List<S3ImportSystem> getAllS3ImportSystem(String customerSpace);

    Set<String> getAllS3ImportSystemIdSet(String customerSpace);

    List<S3ImportSystem> getS3ImportSystemWithFilter(String customerSpace, boolean filterAccount,
                                                     boolean filterContact, S3ImportTemplateDisplay templateDisplay);

    List<TemplateFieldPreview> getTemplatePreview(String customerSpace, Table templateTable, Table standardTable,
                                                  Set<String> matchingFieldNames);

    boolean autoImport(String templateFileName);

    String getTemplateMappingContent(List<TemplateFieldPreview> previews);

    String getSystemNameFromFeedType(String feedType);

    void updateS3ImportSystem(String customerSpace, S3ImportSystem importSystem);

    void updateS3ImportSystemPriorityBasedOnSequence(String customerSpace, List<S3ImportSystem> systemList);

    boolean createWebVisitProfile(String customerSpace, EntityType entityType, InputStream inputStream);

    boolean checkBundleUpload(String customerSpace);

    boolean createDefaultOpportunityTemplate(String customerSpace, String systemName);

    boolean createDefaultMarketingTemplate(String customerSpace, String systemName, String systemType);

    boolean createDefaultDnbIntentDataTemplate(String customerSpace);

    /**
     *
     * @param customerSpace Identify current tenant
     * @param entityType EntityType for current template.
     * @return Attribute name as map key and Attribute display name as map value.
     */
    Map<String, String> getDecoratedDisplayNameMapping(String customerSpace, EntityType entityType);

    /**
     *
     * @param customerSpace Identify current tenant
     * @param systemName {@link S3ImportSystem#getName()} of target S3ImportSystem
     * @param entityType EntityType for current template.
     * @return map of dimensionName -> metadataValue, will not be {@code null}
     */
    Map<String, List<Map<String, Object>>> getDimensionMetadataInStream(String customerSpace, String systemName,
                                                                        EntityType entityType);

    /**
     *
     * @param request
     * @param response
     * @param mimeType such as "application/csv"
     * @param fileName downloadCSV fileName
     * @param customerSpace Identify current tenant
     * @param systemName {@link S3ImportSystem#getName()} of target system
     * @param entityType EntityType for current template.
     */
    void downloadDimensionMetadataInStream(HttpServletRequest request, HttpServletResponse response,
                                           String mimeType, String fileName, String customerSpace,
                                           String systemName, EntityType entityType);

    List<ImportFileInfo> getAllImportFiles(String customerSpace);

    /**
     *
     * @param customerSpace Identify current tenant.
     * @param feedType DataFeedTask.FeedType
     */
    void resetTemplate(String customerSpace, String feedType, Boolean forceReset);

    /**
     *
     * @param customerSpace Identify current tenant.
     * @param systemList The full system list, and system priority will be calculated based on the sequence.
     * @return true if validate pass and priority updated.
     */
    boolean validateAndUpdateS3ImportSystemPriority(String customerSpace, List<S3ImportSystem> systemList);

    ApplicationId generateIntentAlert(String customerSpace);
}
