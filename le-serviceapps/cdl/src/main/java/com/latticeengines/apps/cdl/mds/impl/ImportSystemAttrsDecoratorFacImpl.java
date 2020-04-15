package com.latticeengines.apps.cdl.mds.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.mds.ImportSystemAttrsDecoratorFac;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DummyDecorator;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;

@Component("ImportSystemAttrsDecorator")
public class ImportSystemAttrsDecoratorFacImpl implements ImportSystemAttrsDecoratorFac {

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Inject
    private BatonService batonService;

    @Override
    public Decorator getDecorator(Namespace1<String> namespace) {
        String tenantId = namespace.getCoord1();
        if (StringUtils.isNotBlank(tenantId)) {
            boolean entityMatchEnabled = batonService.isEntityMatchEnabled(CustomerSpace.parse(tenantId));
            if (entityMatchEnabled) {
                boolean onlyEntityMatchGAEnabled = batonService.onlyEntityMatchGAEnabled(CustomerSpace.parse(tenantId));
                List<S3ImportSystem> s3ImportSystems =
                        s3ImportSystemService.getAllS3ImportSystem(CustomerSpace.parse(tenantId).toString());
                return new ImportSystemAttrsDecorator(s3ImportSystems, onlyEntityMatchGAEnabled);
            }
        }
        return new DummyDecorator();
    }
}
