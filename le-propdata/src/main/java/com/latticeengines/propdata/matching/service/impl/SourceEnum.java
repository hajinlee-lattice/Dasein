package com.latticeengines.propdata.matching.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.util.StringUtils;

import com.latticeengines.propdata.matching.service.SourceSpec;

public enum SourceEnum implements SourceSpec {

    Experian("Experian_Source", "URL", "uuid", "domain_name"), BuiltWith("BuiltWithDomain", "Domain", "uuid",
            "domain_name"), HGData("HGData_Source", "Url", "uuid", "domain_name");

    private static Map<String, SourceSpec> specMap = new HashMap<>();
    static {
        specMap.put(Experian.getSourceName().toLowerCase(), Experian);
        specMap.put(BuiltWith.getSourceName().toLowerCase(), BuiltWith);
        specMap.put(HGData.getSourceName().toLowerCase(), HGData);

    }

    private SourceEnum(String sourceName, String sourceDomainColumn, String sourceKey, String indexKeyColumn) {
        this.sourceName = sourceName;
        this.sourceDomainColumn = sourceDomainColumn;
        this.sourceKey = sourceKey;
        this.indexKeyColumn = indexKeyColumn;
    }

    private String sourceName;
    private String sourceDomainColumn;
    private String sourceKey;
    private String indexKeyColumn;

    @Override
    public String getSourceName() {
        return sourceName;
    }

    @Override
    public String getSourceDomainColumn() {
        return sourceDomainColumn;
    }

    @Override
    public String getSourceKey() {
        return sourceKey;
    }

    @Override
    public String getIndexKeyColumn() {
        return indexKeyColumn;
    }

    public static SourceSpec getConnectorSpec(String sourceName) {
        if (StringUtils.isEmpty(sourceName)) {
            return null;
        }
        return specMap.get(sourceName.trim().toLowerCase());

    }
}