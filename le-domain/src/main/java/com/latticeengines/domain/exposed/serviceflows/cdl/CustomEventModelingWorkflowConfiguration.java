package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.Collection;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class CustomEventModelingWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    @Override
    public Collection<String> getSwpkgNames() {
        return ImmutableSet.<String> builder() //
                .add(SoftwareLibrary.Modeling.getName())//
                .addAll(super.getSwpkgNames()) //
                .build();
    }

    public static class Builder {
        private CustomEventModelingWorkflowConfiguration configuration = new CustomEventModelingWorkflowConfiguration();

    }
}
