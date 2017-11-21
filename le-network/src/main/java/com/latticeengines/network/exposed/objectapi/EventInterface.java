package com.latticeengines.network.exposed.objectapi;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public interface EventInterface {

    long getScoringCount(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery);

    long getTrainingCount(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery);

    long getEventCount(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery);

    DataPage getTrainingTuples(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery);

    DataPage getScoringTuples(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery);

    DataPage getEventTuples(@PathVariable String customerSpace, @RequestBody FrontEndQuery frontEndQuery);
}
