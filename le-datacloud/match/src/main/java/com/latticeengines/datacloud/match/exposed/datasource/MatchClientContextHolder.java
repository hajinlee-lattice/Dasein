package com.latticeengines.datacloud.match.exposed.datasource;

import com.latticeengines.domain.exposed.datacloud.MatchClient;

public final class MatchClientContextHolder {

    protected MatchClientContextHolder() {
        throw new UnsupportedOperationException();
    }

    private static final ThreadLocal<MatchClient> clientInThread = new ThreadLocal<MatchClient>();

    public static void setMatchClient(MatchClient client) { clientInThread.set(client); }

    public static MatchClient getMatchClient() { return clientInThread.get(); }

}
