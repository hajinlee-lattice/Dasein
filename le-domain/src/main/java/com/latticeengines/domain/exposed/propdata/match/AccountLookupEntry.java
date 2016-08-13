package com.latticeengines.domain.exposed.propdata.match;

import javax.persistence.Id;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.datafabric.DynamoHashKey;

public class AccountLookupEntry implements HasId<String> {

    public static final String DOMAIN = "_DOMAIN_";
    public static final String DUNS = "_DUNS_";

    public static final String UNKNOWN = "NULL";

    @Id
    String id = null;

    @JsonProperty("domain")
    private String domain = UNKNOWN;

    @JsonProperty("duns")
    private String duns = UNKNOWN;

    @JsonProperty("latticeAccountId")
    @DynamoHashKey(name = "AccountId")
    private String latticeAccountId = UNKNOWN;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLatticeAccountId() {
        return latticeAccountId;
    }

    public void setLatticeAccountId(String latticeAccountId) {
        this.latticeAccountId = latticeAccountId;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
        this.id = buildId(domain, duns);
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
        this.id = buildId(domain, duns);
    }

    public static String buildId(String domain, String duns) {
        return DOMAIN + domain + DUNS + duns;
    }
}


