package com.latticeengines.datacloud.match.datasource;

import com.latticeengines.domain.exposed.datacloud.MatchClient;

public class MatchClientContextHolder {

    private static final ThreadLocal<MatchClient> clientInThread = new ThreadLocal<MatchClient>();

    public static void setMatchClient(MatchClient client) { clientInThread.set(client); }

    public static MatchClient getMatchClient() { return clientInThread.get(); }

}
