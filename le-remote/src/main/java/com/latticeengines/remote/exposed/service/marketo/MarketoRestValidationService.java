package com.latticeengines.remote.exposed.service.marketo;

public interface MarketoRestValidationService {

    boolean validateMarketoRestCredentials(String identityEndPoint, String restEndPoint, String clientId,
            String clientSecret);
}
