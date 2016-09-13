package com.latticeengines.domain.exposed.pls;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonProperty;

@Entity
@Table(name = "MARKETO_CREDENTIAL", uniqueConstraints = {
        @UniqueConstraint(columnNames = { "NAME", "TENANT_ID" }) })
public class MarketoCredential extends Credential {

    private String soapEndpoint;
    private String soapUserId;
    private String soapEncrytionKey;
    private String restEndpoint;
    private String restIdentityEnpoint;
    private String restClientId;
    private String restClientSecret;
    private Enrichment enrichment;

    @JsonProperty("SoapEndpoint")
    @Column(name = "SOAP_ENDPOINT", nullable = false)
    public String getSoapEndpoint() {
        return soapEndpoint;
    }

    @JsonProperty("SoapEndpoint")
    public void setSoapEndpoint(String soapEndpoint) {
        this.soapEndpoint = soapEndpoint;
    }

    @JsonProperty("SoapUserId")
    @Column(name = "SOAP_USER_ID", nullable = false)
    public String getSoapUserId() {
        return soapUserId;
    }

    @JsonProperty("SoapUserId")
    public void setSoapUserId(String soapUserId) {
        this.soapUserId = soapUserId;
    }

    @JsonProperty("SoapEncrytionKey")
    @Column(name = "SOAP_ENCRYTION_KEY", nullable = false)
    public String getSoapEncrytionKey() {
        return soapEncrytionKey;
    }

    @JsonProperty("SoapEncrytionKey")
    public void setSoapEncrytionKey(String soapEncrytionKey) {
        this.soapEncrytionKey = soapEncrytionKey;
    }

    @JsonProperty("RestEndpoint")
    @Column(name = "REST_ENDPOINT", nullable = false)
    public String getRestEndpoint() {
        return restEndpoint;
    }

    @JsonProperty("RestEndpoint")
    public void setRestEndpoint(String restEndpoint) {
        this.restEndpoint = restEndpoint;
    }

    @JsonProperty("RestIdentityEndpoint")
    @Column(name = "REST_IDENTITY_ENDPOINT", nullable = false)
    public String getRestIdentityEnpoint() {
        return restIdentityEnpoint;
    }

    @JsonProperty("RestIdentityEndpoint")
    public void setRestIdentityEnpoint(String restIdentityEnpoint) {
        this.restIdentityEnpoint = restIdentityEnpoint;
    }

    @JsonProperty("RestClientId")
    @Column(name = "REST_CLIENT_ID", nullable = false)
    public String getRestClientId() {
        return restClientId;
    }

    @JsonProperty("RestClientId")
    public void setRestClientId(String restClientId) {
        this.restClientId = restClientId;
    }

    @JsonProperty("RestClientSecret")
    @Column(name = "REST_CLIENT_SECRET", nullable = false)
    public String getRestClientSecret() {
        return restClientSecret;
    }

    @JsonProperty("RestClientSecret")
    public void setRestClientSecret(String restClientSecret) {
        this.restClientSecret = restClientSecret;
    }

    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "FK_ENRICHMENT_ID", nullable = false)
    @JsonProperty("Enrichment")
    public Enrichment getEnrichment() {
        return enrichment;
    }

    @JsonProperty("Enrichment")
    public void setEnrichment(Enrichment enrichment) {
        this.enrichment = enrichment;
    }

}
