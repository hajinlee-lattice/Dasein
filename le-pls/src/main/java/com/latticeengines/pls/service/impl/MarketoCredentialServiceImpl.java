package com.latticeengines.pls.service.impl;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Enrichment;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.pls.MarketoMatchFieldName;
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;
import com.latticeengines.pls.entitymanager.MarketoMatchFieldEntityMgr;
import com.latticeengines.pls.service.MarketoCredentialService;
import com.latticeengines.remote.exposed.service.marketo.MarketoRestValidationService;
import com.latticeengines.remote.exposed.service.marketo.MarketoSoapService;

@Component("marketoCredentialService")
public class MarketoCredentialServiceImpl implements MarketoCredentialService {

    @Inject
    private MarketoCredentialEntityMgr marketoCredentialEntityMgr;

    @Inject
    private MarketoMatchFieldEntityMgr marketoMatchFieldEntityMgr;

    @Inject
    private MarketoRestValidationService marketoRestValidationService;

    @Inject
    private MarketoSoapService marketoSoapService;

    @Value("${pls.marketo.enrichment.webhook.url}")
    private String enrichmentWebhookUrl;

    @Override
    public void createMarketoCredential(MarketoCredential marketoCredential) {
        validateRESTAndSOAPCredentials(marketoCredential);
        marketoCredentialEntityMgr.create(marketoCredential);
    }

    @Override
    public void deleteMarketoCredentialById(String credentialId) {
        marketoCredentialEntityMgr.deleteMarketoCredentialById(credentialId);
    }

    @Override
    public MarketoCredential findMarketoCredentialById(String credentialId) {
        MarketoCredential marketoCredential = marketoCredentialEntityMgr.findMarketoCredentialById(credentialId);
        if (marketoCredential == null) {
            return null;
        }
        Enrichment enrichment = marketoCredential.getEnrichment();
        enrichment.setWebhookUrl(enrichmentWebhookUrl);
        enrichment.setTenantCredentialGUID(UuidUtils.packUuid(MultiTenantContext.getTenant().getId(), credentialId));
        List<MarketoMatchField> fields = enrichment.getMarketoMatchFields();
        Set<MarketoMatchFieldName> fieldNameSet = Sets.newHashSet(MarketoMatchFieldName.values());
        for (MarketoMatchField field : fields) {
            fieldNameSet.remove(field.getMarketoMatchFieldName());
        }
        for (MarketoMatchFieldName missingFieldName : fieldNameSet) {
            MarketoMatchField newField = new MarketoMatchField();
            newField.setMarketoMatchFieldName(missingFieldName);
            fields.add(newField);
        }
        return marketoCredential;
    }

    @Override
    public List<MarketoCredential> findAllMarketoCredentials() {
        List<MarketoCredential> marketoCredentials = marketoCredentialEntityMgr.findAllMarketoCredentials();
        for (MarketoCredential marketoCredential : marketoCredentials) {
            marketoCredential.getEnrichment().setWebhookUrl(enrichmentWebhookUrl);
            marketoCredential.getEnrichment().setTenantCredentialGUID(UuidUtils
                    .packUuid(MultiTenantContext.getTenant().getId(), Long.toString(marketoCredential.getPid())));
        }
        return marketoCredentials;
    }

    @Override
    public void updateMarketoCredentialById(String credentialId, MarketoCredential marketoCredential) {
        validateRESTAndSOAPCredentials(marketoCredential);
        marketoCredentialEntityMgr.updateMarketoCredentialById(credentialId, marketoCredential);
    }

    @Override
    public void updateCredentialMatchFields(String credentialId, List<MarketoMatchField> marketoMatchFields) {
        MarketoCredential marketoCredential = findMarketoCredentialById(credentialId);

        for (MarketoMatchField marketoMatchField : marketoMatchFields) {
            marketoMatchFieldEntityMgr.updateMarketoMatchFieldValue(marketoMatchField.getMarketoMatchFieldName(),
                    marketoMatchField.getMarketoFieldName(), marketoCredential.getEnrichment());
        }

    }

    private void validateRESTAndSOAPCredentials(MarketoCredential marketoCredential) {
        boolean restValidationResult, soapValidationResult;
        try {
            restValidationResult = marketoRestValidationService.validateMarketoRestCredentials(
                    marketoCredential.getRestIdentityEnpoint(), marketoCredential.getRestEndpoint(),
                    marketoCredential.getRestClientId(), marketoCredential.getRestClientSecret());
        } catch (LedpException e) {
            throw new LedpException(LedpCode.LEDP_18116, new String[] { e.getMessage() });
        }
        try {
            soapValidationResult = marketoSoapService.validateMarketoSoapCredentials(
                    marketoCredential.getSoapEndpoint(), marketoCredential.getSoapUserId(),
                    marketoCredential.getSoapEncryptionKey());
        } catch (LedpException e) {
            throw new LedpException(LedpCode.LEDP_18117, new String[] { e.getMessage() });
        }
        if (!restValidationResult) {
            throw new LedpException(LedpCode.LEDP_18116, new String[] { "bad REST credentials" });
        }
        if (!soapValidationResult) {
            throw new LedpException(LedpCode.LEDP_18117, new String[] { "bad SOAP credentials" });
        }
    }

}
