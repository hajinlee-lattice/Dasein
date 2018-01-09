package com.latticeengines.domain.exposed.security;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class Ticket {

    private String uniqueness;
    private String randomness;
    private List<Tenant> tenants;
    private boolean mustChangePassword;
    private long passwordLastModified;
    private String authenticationRoute;
    
    public Ticket() {
        
    }
    
    public Ticket(String ticketData) {
        String[] tokens = ticketData.split("\\.");
        setUniqueness(tokens[0]);
        setRandomness(tokens[1]);
    }
    
    @JsonProperty("Uniqueness")
    public String getUniqueness() {
        return uniqueness;
    }
    
    @JsonProperty("Uniqueness")
    public void setUniqueness(String uniqueness) {
        this.uniqueness = uniqueness;
    }
    
    @JsonProperty("Randomness")
    public String getRandomness() {
        return randomness;
    }
    
    @JsonProperty("Randomness")
    public void setRandomness(String randomness) {
        this.randomness = randomness;
    }
    
    @JsonProperty("Tenants")
    public List<Tenant> getTenants() {
        return tenants;
    }
    
    @JsonProperty("Tenants")
    public void setTenants(List<Tenant> tenants) {
        this.tenants = tenants;
    }
    
    @JsonProperty("MustChangePassword")
    public boolean isMustChangePassword() {
        return mustChangePassword;
    }
    
    @JsonProperty("PasswordLastModified")
    public void setPasswordLastModified(long passwordLastModified) { this.passwordLastModified = passwordLastModified; }

    @JsonProperty("PasswordLastModified")
    public long getPasswordLastModified() {
        return passwordLastModified;
    }

    @JsonProperty("MustChangePassword")
    public void setMustChangePassword(boolean mustChangePassword) {
        this.mustChangePassword = mustChangePassword;
    }

    @JsonProperty("AuthenticationRoute")
    public String getAuthenticationRoute() {
        return authenticationRoute;
    }

    @JsonProperty("AuthenticationRoute")
    public void setAuthenticationRoute(String authenticationRoute) {
        this.authenticationRoute = authenticationRoute;
    }
    
    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
    
    @JsonIgnore
    public String getData() {
        return getUniqueness() + "." + getRandomness();
    }
}
