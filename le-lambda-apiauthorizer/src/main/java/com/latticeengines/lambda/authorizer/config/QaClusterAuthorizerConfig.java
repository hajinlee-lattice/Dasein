package com.latticeengines.lambda.authorizer.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile(value="qacluster")
public class QaClusterAuthorizerConfig extends AuthorizerConfigBase {

    public String getApiHost() {
        return "https://testapi.lattice-engines.com";
    }

}
