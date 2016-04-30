package com.latticeengines.monitor.exposed.metric.stats;

import java.util.List;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;

public interface Inspection {

    List<Measurement<? extends Fact, ? extends Dimension>> report();
    Long interval();

}
