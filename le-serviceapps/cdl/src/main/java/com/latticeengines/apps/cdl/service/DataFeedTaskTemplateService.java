package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.EntityType;

public interface DataFeedTaskTemplateService {

    /**
     * @param customerSpace target tenant
     * @param simpleTemplateMetadata Template description.
     * @return true if success.
     */
    boolean setupWebVisitProfile(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata);

    /**
     * Uses Import Workflow 2.0 to set up the 3 WebVisit import templates from Import Workflow Specs.
     *
     * @param customerSpace target tenant
     * @param simpleTemplateMetadata Template description.
     * @return true if success.
     */
    boolean setupWebVisitProfile2(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata);

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
}
