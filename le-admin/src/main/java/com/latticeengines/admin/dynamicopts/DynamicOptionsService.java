package com.latticeengines.admin.dynamicopts;

import com.latticeengines.domain.exposed.admin.SelectableConfigurationDocument;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;

public interface DynamicOptionsService {

    SerializableDocumentDirectory bind(SerializableDocumentDirectory sDir);

    SelectableConfigurationDocument bind(SelectableConfigurationDocument doc);

}
