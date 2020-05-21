package com.latticeengines.domain.exposed.auth;

public interface HasTeamInfo extends HasTeamId {

    GlobalTeam getTeam();

    void setTeam(GlobalTeam team);

    boolean isViewOnly();

    void setViewOnly(boolean viewOnly);
}
