package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

public class BaseCDLWorkflowConfiguration extends WorkflowConfiguration {

    // @Override
    // public Collection<String> getSwpkgNames() {
    // return Collections.singleton(SoftwareLibrary.CDL.getName());
    // }
    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Modeling.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

}
