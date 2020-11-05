package com.latticeengines.security.service;

import com.latticeengines.domain.exposed.dcp.idaas.IDaaSResponse;
import com.latticeengines.domain.exposed.dcp.idaas.IDaaSUser;
import com.latticeengines.domain.exposed.dcp.idaas.InvitationLinkResponse;
import com.latticeengines.domain.exposed.dcp.idaas.ProductRequest;
import com.latticeengines.domain.exposed.dcp.idaas.RoleRequest;
import com.latticeengines.domain.exposed.dcp.idaas.SubscriberDetails;
import com.latticeengines.domain.exposed.dcp.vbo.VboCallback;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;

public interface IDaaSService {

    LoginDocument login(Credentials credentials);

    IDaaSUser getIDaaSUser(String email);

    IDaaSUser createIDaaSUser(IDaaSUser user);

    IDaaSUser updateIDaaSUser(IDaaSUser user);

    IDaaSResponse addProductAccessToUser(ProductRequest request);

    IDaaSResponse addRoleToUser(RoleRequest request);

    InvitationLinkResponse getUserInvitationLink(String email);

    void callbackWithAuth(String url, VboCallback responseBody);

    SubscriberDetails getSubscriberDetails (String subscriberNumber);

    boolean doesSubscriberNumberExist(VboRequest vboRequest);

    LoginDocument addSubscriberDetails(LoginDocument doc);
}
