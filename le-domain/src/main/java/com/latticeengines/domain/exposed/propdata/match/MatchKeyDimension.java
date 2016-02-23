package com.latticeengines.domain.exposed.propdata.match;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.util.DomainUtils;

public class MatchKeyDimension {

    private String domain;
    private String name;
    private String city;
    private String state;
    private String country;
    private String duns;
    private String latticeAccountId;

    public MatchKeyDimension(Map<MatchKey, String> keyMap, List<String> fields, List<Object> inputData) {
        Map<MatchKey, Integer> positionMap = new HashMap<>();
        for (Map.Entry<MatchKey, String> entry : keyMap.entrySet()) {
            if (fields.contains(entry.getValue())) {
                Integer pos = fields.indexOf(entry.getValue());
                positionMap.put(entry.getKey(), pos);
            }
        }

        if (positionMap.containsKey(MatchKey.Domain)) {
            domain = (String) inputData.get(positionMap.get(MatchKey.Domain));
            domain = DomainUtils.parseDomain(domain);
        }

        if (positionMap.containsKey(MatchKey.Name)) {
            name = (String) inputData.get(positionMap.get(MatchKey.Name));
        }

        if (positionMap.containsKey(MatchKey.City)) {
            city = (String) inputData.get(positionMap.get(MatchKey.City));
        }

        if (positionMap.containsKey(MatchKey.State)) {
            state = (String) inputData.get(positionMap.get(MatchKey.State));
        }

        if (positionMap.containsKey(MatchKey.Country)) {
            country = (String) inputData.get(positionMap.get(MatchKey.Country));
        }

        if (positionMap.containsKey(MatchKey.DUNS)) {
            duns = (String) inputData.get(positionMap.get(MatchKey.DUNS));
        }

        if (positionMap.containsKey(MatchKey.LatticeAccountID)) {
            latticeAccountId = (String) inputData.get(positionMap.get(MatchKey.LatticeAccountID));
        }

    }

    @MetricTag(tag = "Domain")
    public String getDomain() {
        return domain;
    }

    @MetricTag(tag = "City")
    public String getCity() {
        return city;
    }

    @MetricTag(tag = "State")
    public String getState() {
        return state;
    }

    @MetricTag(tag = "Country")
    public String getCountry() {
        return country;
    }

    @MetricTag(tag = "Name")
    public String getName() {
        return name;
    }

    @MetricTag(tag = "DUNS")
    public String getDuns() {
        return duns;
    }

    @MetricTag(tag = "LatticeAccountId")
    public String getLatticeAccountId() {
        return latticeAccountId;
    }

}
