package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.PlayGroup;

public interface PlayGroupService {
    List<PlayGroup> getAllPlayGroups(String customerSpace);

    void deletePlayGroupsFromPlays(String customerSpace, PlayGroup playgroup);
}
