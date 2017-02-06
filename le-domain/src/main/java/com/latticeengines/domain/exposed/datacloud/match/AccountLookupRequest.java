package com.latticeengines.domain.exposed.datacloud.match;

import java.util.ArrayList;
import java.util.List;


public class AccountLookupRequest {

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
        ids.add(AccountLookupEntry.buildId(domain, duns));
    }

    public void addLookupPair(String domain, String duns, String country, String state, String zipCode) {
        ids.add(AccountLookupEntry.buildIdWithLocation(domain, duns, country, state, zipCode));
    }

    public void addId(String id) {
        ids.add(id);
    }

    public List<String> getIds() {
        return ids;
    }
}


