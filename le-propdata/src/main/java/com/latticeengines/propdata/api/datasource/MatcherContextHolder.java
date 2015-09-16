package com.latticeengines.propdata.api.datasource;

public class MatcherContextHolder {

    private static final ThreadLocal<MatcherClient> clientInThread = new ThreadLocal<MatcherClient>();

    public static void setMatcherClient(MatcherClient client) { clientInThread.set(client); }

    public static MatcherClient getMatcherClient() { return clientInThread.get(); }

}
