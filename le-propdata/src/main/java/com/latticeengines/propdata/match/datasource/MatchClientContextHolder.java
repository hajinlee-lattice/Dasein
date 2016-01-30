package com.latticeengines.propdata.match.datasource;

import com.latticeengines.domain.exposed.propdata.MatchClient;

public class MatchClientContextHolder {

    private static final ThreadLocal<MatchClient> clientInThread = new ThreadLocal<MatchClient>();

    public static void setMatchClient(MatchClient client) { clientInThread.set(client); }

    public static MatchClient getMatchClient() { return clientInThread.get(); }

}
