package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class BaseCDLWorkflowConfiguration extends WorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        ImmutableSet.Builder<String> builder = ImmutableSet.<String>builder() //
                .add(SoftwareLibrary.CDL.getName());
        if (CollectionUtils.isNotEmpty(super.getSwpkgNames())) {
            builder.addAll(super.getSwpkgNames());
        }
        return builder.build();
    }

}
