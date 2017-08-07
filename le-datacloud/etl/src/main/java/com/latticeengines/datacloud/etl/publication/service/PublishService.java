package com.latticeengines.datacloud.etl.publication.service;

import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;

public interface PublishService<T extends PublicationConfiguration> {

    PublicationProgress publish(PublicationProgress progress, T configuration);

}
