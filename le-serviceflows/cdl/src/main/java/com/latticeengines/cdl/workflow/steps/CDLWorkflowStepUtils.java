package com.latticeengines.cdl.workflow.steps;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

public class CDLWorkflowStepUtils {

    static Table getMasterTable(DataCollection dataCollection) {
        return dataCollection.getTable(SchemaInterpretation.Account);
    }

    static Table getProfileTable(DataCollection dataCollection) {
        return dataCollection.getTable(SchemaInterpretation.Profile);
    }

}
