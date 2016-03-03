package com.latticeengines.pls.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class BaseModelWorkflowSubmitter extends WorkflowSubmitter {
    @Autowired
    protected MetadataProxy metadataProxy;

    @Value("${pls.modelingservice.basedir}")
    protected String modelingServiceHdfsBaseDir;
}
