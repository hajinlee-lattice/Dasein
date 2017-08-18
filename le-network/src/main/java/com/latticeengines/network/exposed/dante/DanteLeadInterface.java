package com.latticeengines.network.exposed.dante;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;

public interface DanteLeadInterface {
    void create(Recommendation recommendation, String customerSpace);
}
