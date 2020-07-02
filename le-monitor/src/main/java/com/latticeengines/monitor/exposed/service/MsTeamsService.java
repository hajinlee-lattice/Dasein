package com.latticeengines.monitor.exposed.service;

import com.latticeengines.domain.exposed.monitor.MsTeamsSettings;

public interface MsTeamsService {
    void sendMsTeams(MsTeamsSettings settings);
}
