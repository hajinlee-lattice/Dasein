package com.latticeengines.dante.service;

import com.latticeengines.domain.exposed.dante.DanteLeadDTO;

public interface DanteLeadService {
    void create(DanteLeadDTO danteLeadDTO, String customerSpace);
}
