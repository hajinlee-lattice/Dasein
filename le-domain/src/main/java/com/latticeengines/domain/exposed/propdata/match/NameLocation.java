package com.latticeengines.domain.exposed.propdata.match;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NameLocation {

    private String name;
    private String country;
    private String state;
    private String city;

    @JsonProperty("Name")
    public String getName() {
        return name;
    }

    @JsonProperty("Name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("Country")
    public String getCountry() {
        return country;
    }

    @JsonProperty("Country")
    public void setCountry(String country) {
        this.country = country;
    }

    @JsonProperty("State")
    public String getState() {
        return state;
    }

    @JsonProperty("State")
    public void setState(String state) {
        this.state = state;
    }

    @JsonProperty("City")
    public String getCity() {
        return city;
    }

    @JsonProperty("City")
    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (that instanceof NameLocation) {
            NameLocation nameLocation = (NameLocation) that;
            return StringUtils.equals(this.name, nameLocation.name)
                    && StringUtils.equals(this.city, nameLocation.city)
                    && StringUtils.equals(this.state, nameLocation.state)
                    && StringUtils.equals(this.country, nameLocation.country);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        String toReturn = "(";
        toReturn += (name == null ? "null" : name);
        toReturn += (city == null ? "null" : city);
        toReturn += (state == null ? "null" : state);
        toReturn += (country == null ? "null" : country);
        toReturn += ")";
        return toReturn;
    }

}
