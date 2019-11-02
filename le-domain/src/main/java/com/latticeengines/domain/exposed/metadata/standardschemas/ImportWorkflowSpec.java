package com.latticeengines.domain.exposed.metadata.standardschemas;

import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;

public class ImportWorkflowSpec extends FieldDefinitionsRecord {

    @Override
    public boolean equals(Object object) {
        if (object instanceof ImportWorkflowSpec) {
            ImportWorkflowSpec spec = (ImportWorkflowSpec) object;
            return super.equals(spec);
        }
        return false;
    }
}


