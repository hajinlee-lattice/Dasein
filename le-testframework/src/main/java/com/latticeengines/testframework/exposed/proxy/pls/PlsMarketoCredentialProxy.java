package com.latticeengines.testframework.exposed.proxy.pls;

import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfig;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigSummary;

@Service("plsMarketoCredentialProxy")
public class PlsMarketoCredentialProxy extends PlsRestApiProxyBase {

    public static final String PLS_MARKETO_CREDENTIAL_URL = "/pls/marketo/credentials";
    public static final String PLS_MARKETO_SCORING_REQUESTS = "/scoring-requests";
    
    public PlsMarketoCredentialProxy() {
        super("pls/marketo/credentials");
    }

    public void createMarketoCredential(MarketoCredential marketoCredential) {
        post("Creating Marketo Crendential", constructUrl("/"), marketoCredential, MarketoCredential.class);
    }

    public List<MarketoCredential> getMarketoCredentials() {
        return getMarketoCredentials(constructUrl("/"));
    }
    
    public List<MarketoCredential> getMarketoCredentialsSimplified() {
        return getMarketoCredentials(constructUrl("/simplified"));
    }
    
    private List<MarketoCredential> getMarketoCredentials(String endpoint) {
        List response = get("Get Marketo Crendetials", endpoint, List.class);
        assertNotNull(response);
        List<MarketoCredential> credentialList = JsonUtils.convertList(response, MarketoCredential.class);
        return credentialList;
    }
    
    public MarketoCredential getMarketoCredential(Long credentialId) {
        String url = constructUrl("/{credentialId}", credentialId);
        MarketoCredential marketoCredential = get("Get Marketo Credential", url, MarketoCredential.class);
        assertNotNull(marketoCredential);
        return marketoCredential;
    }
    
    public List<ScoringRequestConfigSummary> getScoringRequestConfigs(Long credentialId) {
        String url = constructUrl("/{credentialId}"+PLS_MARKETO_SCORING_REQUESTS, credentialId);
        List response = get("Get ScoringRequestConfig Summary", url, List.class);
        assertNotNull(response);
        List<ScoringRequestConfigSummary> scoringRequestConfigList = JsonUtils.convertList(response, ScoringRequestConfigSummary.class);
        return scoringRequestConfigList;
    }
    
    public ScoringRequestConfig getScoringRequestConfig(Long credentialId, String configId) {
        String url = constructUrl("/{credentialId}"+PLS_MARKETO_SCORING_REQUESTS+"/{configId}", credentialId, configId);
        ScoringRequestConfig scoringRequestConfig = get("Get ScoringRequestConfig: ", url, ScoringRequestConfig.class);
        assertNotNull(scoringRequestConfig);
        return scoringRequestConfig;
    }
    
    public ScoringRequestConfig createScoringRequestConfig(Long credentialId, ScoringRequestConfig scoringReqConf) {
        String url = constructUrl("/{credentialId}"+PLS_MARKETO_SCORING_REQUESTS, credentialId);
        ScoringRequestConfig createdReq = post("Create ScoringRequestConfig", url, scoringReqConf, ScoringRequestConfig.class);
        return createdReq;
    }

    public void updateScoringRequestConfig(Long credentialId, ScoringRequestConfig scoreReqConf) {
        String url = constructUrl("/{credentialId}"+PLS_MARKETO_SCORING_REQUESTS+"/{configId}", credentialId, scoreReqConf.getConfigId());
        put("Update ScoringRequestConfig", url, scoreReqConf);
    }
    

}
