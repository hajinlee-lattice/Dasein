package com.latticeengines.release.exposed.activities;

import com.latticeengines.release.exposed.domain.ProcessContext;

public interface Activity {

    public ProcessContext execute(ProcessContext context);

}
