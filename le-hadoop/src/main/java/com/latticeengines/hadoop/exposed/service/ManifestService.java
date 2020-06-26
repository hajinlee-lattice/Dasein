package com.latticeengines.hadoop.exposed.service;

public interface ManifestService {

    String getLedsVersion();

    String getLedsVersion(String pythonVersion);

    String getLedsPath();

    String getLedpStackVersion();

    String getLedpPath();

}
