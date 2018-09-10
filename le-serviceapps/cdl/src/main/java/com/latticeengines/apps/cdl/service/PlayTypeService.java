package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.PlayType;

public interface PlayTypeService {
    List<PlayType> getAllPlayTypes(String customerSpace);
}
