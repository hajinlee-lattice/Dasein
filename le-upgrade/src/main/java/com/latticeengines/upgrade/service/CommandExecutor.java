package com.latticeengines.upgrade.service;

import java.util.Map;

public interface CommandExecutor {

    boolean execute(String command, Map<String, Object> parameters);

}
