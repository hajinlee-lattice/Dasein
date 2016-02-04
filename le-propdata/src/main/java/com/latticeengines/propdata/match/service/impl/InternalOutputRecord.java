package com.latticeengines.propdata.match.service.impl;

import com.latticeengines.domain.exposed.propdata.match.OutputRecord;

public class InternalOutputRecord extends OutputRecord {

    private String parsedDomain;

    public String getParsedDomain() {
        return parsedDomain;
    }

    public void setParsedDomain(String parsedDomain) {
        this.parsedDomain = parsedDomain;
    }
}
