package com.latticeengines.domain.exposed.propdata.match;

import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class MatchKeyDimension {

    private String domain;
    private String name;
    private String city;
    private String state;
    private String country;
    private String duns;
    private String latticeAccountId;

    public MatchKeyDimension(String domain, NameLocation nameLocation) {
        this.domain = domain;
        if (nameLocation != null) {
            this.name = nameLocation.getName();
            this.city = nameLocation.getCity();
            this.state = nameLocation.getState();
            this.country = nameLocation.getCountry();
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
