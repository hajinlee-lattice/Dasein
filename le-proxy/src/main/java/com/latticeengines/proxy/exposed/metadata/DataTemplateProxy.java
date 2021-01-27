package com.latticeengines.proxy.exposed.metadata;

import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;

public interface DataTemplateProxy {

    String create(String customerSpace, DataTemplate dataTemplate);

}
