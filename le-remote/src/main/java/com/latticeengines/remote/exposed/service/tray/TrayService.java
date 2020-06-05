package com.latticeengines.remote.exposed.service.tray;

import com.latticeengines.domain.exposed.remote.tray.TraySettings;

public interface TrayService {

    Object removeSolutionInstance(TraySettings settings);

    Object removeAuthentication(TraySettings settings);
}
