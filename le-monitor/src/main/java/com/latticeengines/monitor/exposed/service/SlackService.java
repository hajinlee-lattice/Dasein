package com.latticeengines.monitor.exposed.service;

import com.latticeengines.domain.exposed.monitor.SlackSettings;

public interface SlackService {
    void sendSlack(SlackSettings settings);
}
