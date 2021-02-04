package com.latticeengines.security.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.domain.exposed.dcp.vbo.VboCallback;
import com.latticeengines.domain.exposed.dcp.vbo.VboUserSeatUsageEvent;

public interface VboService {
    void sendProvisioningCallback(VboCallback callback);

    void sendUserUsageEvent(VboUserSeatUsageEvent usageEvent);

    JsonNode getSubscriberMeter(String subscriberNumber);
}
