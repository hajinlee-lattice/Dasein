package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;

public interface S3ImportSystemService {

    void createS3ImportSystem(String customerSpace, S3ImportSystem importSystem);

    S3ImportSystem getS3ImportSystem(String customerSpace, String name);
}
