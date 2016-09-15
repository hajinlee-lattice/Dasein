package com.latticeengines.remote.exposed.service.marketo;

import java.util.List;

import com.latticeengines.domain.exposed.remote.marketo.LeadField;

public interface MarketoSoapService {

    boolean validateMarketoSoapCredentials(String soapEndPoint, String userId, String encryptionKey);

    List<LeadField> getLeadFields(String soapEndPoint, String userId, String encryptionKey);
}
