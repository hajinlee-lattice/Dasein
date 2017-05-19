package com.latticeengines.datacloud.yarn.runtime;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

public interface DedupeHelper {

    void appendDedupeValues(ProcessorContext processorContext, List<Object> allValues, OutputRecord outputRecord);

}
