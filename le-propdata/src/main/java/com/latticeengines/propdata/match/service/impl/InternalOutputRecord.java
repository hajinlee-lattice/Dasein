package com.latticeengines.propdata.match.service.impl;

import com.latticeengines.domain.exposed.propdata.match.OutputRecord;

class InternalOutputRecord extends OutputRecord {

    private String parsedDomain;

    String getParsedDomain() {
        return parsedDomain;
    }

    void setParsedDomain(String parsedDomain) {
        this.parsedDomain = parsedDomain;
    }
}
