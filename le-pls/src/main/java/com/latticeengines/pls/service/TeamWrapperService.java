package com.latticeengines.pls.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.auth.HasTeamInfo;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.domain.exposed.security.User;

public interface TeamWrapperService {

    Map<String, List<String>> getDependencies(String teamId) throws Exception;

    Boolean editTeam(String teamId, GlobalTeamData globalTeamData);

    String createTeam(String createdByUser, GlobalTeamData globalTeamData);

    List<GlobalTeam> getTeamsInContext(boolean withTeamMember, boolean appendDefaultGlobalTeam);

    List<GlobalTeam> getTeamsByUserName(String username, User loginUser, boolean withTeamMember);

    List<GlobalTeam> getMyTeams(boolean withTeamMember, boolean appendDefaultGlobalTeam);

    Set<String> getMyTeamIds();

    GlobalTeam getTeamInContext(String teamId);

    void fillTeamInfo(HasTeamInfo hasTeamInfo);

    void fillTeamInfoForList(boolean inflateTeam, List<? extends HasTeamInfo> hasTeamInfos);

    void fillTeamInfo(HasTeamInfo hasTeamInfo, boolean setTeam);
}
