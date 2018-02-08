package com.latticeengines.lambda.authorizer.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile(value="prodcluster")
public class ProdClusterAuthorizerConfig extends AuthorizerConfigBase {

    public String getApiHost() {
        return "https://api.lattice-engines.com";
    }

}