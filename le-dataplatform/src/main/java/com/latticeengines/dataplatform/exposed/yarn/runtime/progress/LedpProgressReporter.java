package com.latticeengines.dataplatform.exposed.yarn.runtime.progress;

import org.springframework.stereotype.Component;
import org.springframework.yarn.am.allocate.ContainerAllocator;

@Component("ledpProgressReporter")
public class LedpProgressReporter {

    private ContainerAllocator containerAllocator;

    public void setContainerAllocator(ContainerAllocator containerAllocator) {
        this.containerAllocator = containerAllocator;
    }

    public void setProgress(float progress) {
        containerAllocator.setProgress(progress);
    }
}
