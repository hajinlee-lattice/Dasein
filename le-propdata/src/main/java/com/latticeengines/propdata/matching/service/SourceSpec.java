package com.latticeengines.propdata.matching.service;

public interface SourceSpec {

    String getSourceName();

    String getSourceDomainColumn();

    String getSourceKey();

    String getIndexKeyColumn();

}
