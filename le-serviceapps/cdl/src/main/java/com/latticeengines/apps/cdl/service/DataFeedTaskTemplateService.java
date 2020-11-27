package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.validator.SimpleValueFilter;
import com.latticeengines.domain.exposed.query.EntityType;

public interface DataFeedTaskTemplateService {

    /**
     * @param customerSpace target tenant
     * @param simpleTemplateMetadata Template description.
     * @param systemDisplayName s3ImportSystem display name
     * @return true if success.
     */
    boolean setupWebVisitProfile(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata,
                                 String systemDisplayName);

    /**
     * Uses Import Workflow 2.0 to set up the 3 WebVisit import templates from Import Workflow Specs.
     *
     * @param customerSpace target tenant
     * @param simpleTemplateMetadata Template description.
     * @param systemDisplayName s3ImportSystem display name
     * @return true if success.
     */
    boolean setupWebVisitProfile2(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata,
                                  String systemDisplayName);

    /**
     *
     * @param customerSpace target tenant.
     * @param uniqueTaskId DataFeedTask unique Id to be backup.
     * @return filename that has the backup data.
     */
    String backupTemplate(String customerSpace, String uniqueTaskId);

    /**
     *
     * @param customerSpace target tenant.
     * @param uniqueTaskId DataFeedTask unique Id to be restore.
     * @param backupName Name from backupTemplate
     * @param onlyGetTable if true, only return the template table, won't restore datafeedtask.
     * @return A table object from the backup file. / Null if file not exists.
     */
    Table restoreTemplate(String customerSpace, String uniqueTaskId, String backupName, boolean onlyGetTable);

    /**
     *
     * @param customerSpace target tenant.
     * @param systemName The user defined name for the system for which a template is being created
     * @param entityType using to judge if this is Opportunity entityType
     * @return true if validate pass.
     */
    boolean validationOpportunity(String customerSpace, String systemName, EntityType entityType);

    /**
     *
     * @param customerSpace target tenant
     * @param systemName The user defined name for the system for which a template is being created
     * @return true if success
     */
    boolean createDefaultOpportunityTemplate(String customerSpace, String systemName);

    /**
     *
     * @param customerSpace target tenant
     * @param systemName The user defined name for the system for which a template is being created
     * @param entityType using to create Template
     * @param simpleTemplateMetadata EntityType.Opportunity Template description.
     * @return true if success
     */
    boolean createOpportunityTemplate(String customerSpace, String systemName, EntityType entityType,
                                           SimpleTemplateMetadata simpleTemplateMetadata);

    /**
     *
     * @param customerSpace target tenant
     * @param systemName The user defined name for the system for which a template is being created
     * @param systemType choose use Marketo spec or eloqua spec
     * @param entityType using to create Template
     * @return true if validate pass.
     */
    boolean validationMarketing(String customerSpace, String systemName, String systemType, EntityType entityType);

    /**
     *
     * @param customerSpace target tenant
     * @param systemName The user defined name for the system for which a template is being created
     * @param systemType choose use Marketo spec or eloqua spec
     * @return true if success
     */
    boolean createDefaultMarketingTemplate(String customerSpace, String systemName, String systemType);

    /**
     *
     * @param customerSpace target tenant
     * @param systemName The user defined name for the system for which a template is being created
     * @param systemType choose use Marketo spec or eloqua spec
     * @param entityType using to create Template
     * @param simpleTemplateMetadata EntityType.Opportunity Template description.
     * @return true if success
     */
    boolean createMarketingTemplate(String customerSpace, String systemName, String systemType, EntityType entityType,
                                    SimpleTemplateMetadata simpleTemplateMetadata);

    /**
     *
     * @param customerSpace target tenant
     * @param systemDisplayName s3ImportSystem display name
     * @return true if success
     */
    boolean createDefaultDnbIntentDataTemplate(String customerSpace, String systemDisplayName);

    /**
     *
     * @param customerSpace target tenant
     * @param entityType using to create Template
     * @param simpleTemplateMetadata EntityType.DnbIntentData Template description.
     * @param systemDisplayName s3ImportSystem display name
     * @return true if success
     */
    boolean createDnbIntentDataTemplate(String customerSpace, EntityType entityType,
                                        SimpleTemplateMetadata simpleTemplateMetadata, String systemDisplayName);

    /**
     *
     * @param enableGA if true, GA tenant can use setup API to create activity store template
     * @return true if success
     */
    boolean validateGAEnabled(String customerSpace, boolean enableGA);

    /**
     *
     * @param customerSpace target tenant
     * @param source Indicate the task source {VisiDB / File}
     * @param feedType DataFeedTask feedType to be reset.
     * @return true if success.
     */
    boolean resetTemplate(String customerSpace, String source, String feedType, Boolean forceDelete);

    boolean hasPAConsumedImportAction(String customerSpace, String taskUniqueId);

    boolean hasPAConsumedImportAction(String customerSpace, String source, String feedType);

    /**
     *
     * @return All template UUIDs that being used by PA
     */
    List<String> getPAConsumedTemplates(String customerSpace);

    void addAttributeLengthValidator(String customerSpace, String uniqueTaskId, String attrName, int length,
                                     boolean nullable);

    void updateAttributeLengthValidator(String customerSpace, String uniqueTaskId, String attrName, Integer length,
                                     boolean nullable);

    void addSimpleValueFilter(String customerSpace, String uniqueTaskId, SimpleValueFilter simpleValueFilter);
}
