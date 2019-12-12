package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.metadata.Table;

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
}
