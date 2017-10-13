package com.latticeengines.network.exposed.dante;

import com.latticeengines.domain.exposed.dante.DanteLeadDTO;

public interface DanteLeadInterface {
    void create(DanteLeadDTO danteLeadDTO, String customerSpace);
}
