package com.latticeengines.domain.exposed.datacloud.match;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.common.exposed.util.PhoneNumberUtils;
import com.latticeengines.common.exposed.util.StringStandardizationUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Contact implements Fact, Serializable {

    private static final long serialVersionUID = -2387291509336164580L;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("PhoneNumber")
    private String phoneNumber;

    @JsonProperty("Email")
    private String email;

    public Contact normalize(String countryCode) {
        Contact normalized = new Contact();
        normalized.name = StringStandardizationUtils.getStandardString(name);
        normalized.phoneNumber = PhoneNumberUtils.getStandardPhoneNumber(phoneNumber, countryCode);
        normalized.email = DomainUtils.parseEmail(email);
        return normalized;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (that instanceof Contact) {
            Contact contact = (Contact) that;
            return StringUtils.equals(this.name, contact.name) //
                    && StringUtils.equals(this.email, contact.email) //
                    && StringUtils.equals(this.phoneNumber, contact.phoneNumber);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        String toReturn = "(" + String.join("__", //
                        String.valueOf(this.name), //
                        String.valueOf(this.email), //
                        String.valueOf(this.phoneNumber) //
                ) + ")";
        return toReturn;
    }
}
