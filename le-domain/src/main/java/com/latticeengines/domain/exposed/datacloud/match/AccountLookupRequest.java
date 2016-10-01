package com.latticeengines.domain.exposed.datacloud.match;

import java.util.ArrayList;
import java.util.List;


public class AccountLookupRequest {

    public static final String DOMAIN = "_DOMAIN_";
    public static final String DUNS = "_DUNS_";
    public static final String UNKNOWN = "NULL";

    private String version;

    private List<String> ids;

    public AccountLookupRequest(String version) {
        this.version = version;
        ids = new ArrayList<>();
    }

    public String getVersion() {
        return version;
    }

    public void addLookupPair(String domain, String duns) {
        if (domain == null) domain = UNKNOWN;
        if (duns == null) duns = UNKNOWN;
        ids.add(DOMAIN + domain + DUNS + duns);
    }

    public void addId(String id) {
        ids.add(id);
    }

    public List<String> getIds() {
        return ids;
    }
}


