package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.StreamDimension;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.frontend.TemplateFieldPreview;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
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

    UIAction cleanup(String customerSpace, String sourceFileName, SchemaInterpretation schemaInterpretation,
                     CleanupOperationType cleanupOperationType);

    ApplicationId cleanupByTimeRange(String customerSpace, String startTime, String endTime,
                                     SchemaInterpretation schemaInterpretation);

    ApplicationId cleanupAllData(String customerSpace, SchemaInterpretation schemaInterpretation);

    void replaceData(String customerSpace, SchemaInterpretation schemaInterpretation);

    List<S3ImportTemplateDisplay> getS3ImportTemplate(String string, String sortBy, Set<EntityType> excludeTypes);

    List<FileProperty> getFileListForS3Path(String customerSpace, String s3Path, String filter);

    void createS3ImportSystem(String customerSpace, String systemDisplayName, S3ImportSystem.SystemType systemType,
                              Boolean primary);

    S3ImportSystem getS3ImportSystem(String customerSpace, String systemName);

    S3ImportSystem getDefaultImportSystem(String customerSpace);

    List<S3ImportSystem> getAllS3ImportSystem(String customerSpace);

    List<S3ImportSystem> getS3ImportSystemWithFilter(String customerSpace, boolean filterAccount,
                                                     boolean filterContact, S3ImportTemplateDisplay templateDisplay);

    List<TemplateFieldPreview> getTemplatePreview(String customerSpace, Table templateTable, Table standardTable);

    boolean autoImport(String templateFileName);

    String getTemplateMappingContent(Table templateTable, Table standardTable);

    String getSystemNameFromFeedType(String feedType);

    void updateS3ImportSystem(String customerSpace, S3ImportSystem importSystem);

    void updateS3ImportSystemPriorityBasedOnSequence(String customerSpace, List<S3ImportSystem> systemList);

    boolean createWebVisitProfile(String customerSpace, EntityType entityType, InputStream inputStream);

    boolean checkBundleUpload(String customerSpace);

    boolean createDefaultOpportunityTemplate(String customerSpace, String systemName);

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
     * @param streamName {@link AtlasStream#getName()} of target stream
     * @param signature signature of metadata, if not provided, will use the signature
     *      associated to current active version
     * @return map of dimensionName -> metadataValue, will not be {@code null}
     */
    Map<String, List<Map<String, Object>>> getDimensionMetadataInStream(String customerSpace, String streamName,
                                                                String signature);

    /**
     *
     * @param request
     * @param response
     * @param mimeType such as "application/csv"
     * @param fileName downloadCSV fileName
     * @param customerSpace Identify current tenant
     * @param streamName {@link AtlasStream#getName()} of target stream
     * @param signature signature of metadata, if not provided, will use the signature
     *      associated to current active version
     * @param dimensionName {@link StreamDimension#getName()} of target dimension
     */
    void downloadDimensionMetadataInStream(HttpServletRequest request, HttpServletResponse response,
                                           String mimeType, String fileName, String customerSpace,
                                           String streamName, String signature, String dimensionName);
}
