package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;

public interface S3ImportSystemService {

    void createS3ImportSystem(String customerSpace, S3ImportSystem importSystem);

    void createDefaultImportSystem(String customerSpace);

    void updateS3ImportSystem(String customerSpace, S3ImportSystem importSystem);

    S3ImportSystem getS3ImportSystem(String customerSpace, String name);

    List<S3ImportSystem> getAllS3ImportSystem(String customerSpace);

    void updateAllS3ImportSystemPriority(String customerSpace, List<S3ImportSystem> systemList);

    boolean hasSystemMapToLatticeAccount(String customerSpace);

    boolean hasSystemMapToLatticeContact(String customerSpace);

    List<String> getAllS3ImportSystemIds(String customerSpace);
}
