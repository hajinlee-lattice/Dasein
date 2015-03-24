package com.latticeengines.madison.service;

import java.util.List;

public interface PropDataMadisonDataFlowService {

    void execute(String flowName, List<String> sourcePaths, String targetPath, String targetSchemaPath);
}
