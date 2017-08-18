package com.latticeengines.dante.service;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;

public interface DanteLeadService {
    void create(Recommendation recommendation, String customerSpace);
}
