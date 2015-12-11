package com.latticeengines.pls.service;

import javax.servlet.http.HttpServletRequest;

import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SegmentService {

    void createSegment(Segment segment, HttpServletRequest request);

    void update(String segmentName, Segment segment);

    void synchronizeModelingAndScoring(Tenant tenant);
}
