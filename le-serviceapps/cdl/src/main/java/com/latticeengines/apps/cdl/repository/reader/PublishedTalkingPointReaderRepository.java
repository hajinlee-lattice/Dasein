package com.latticeengines.apps.cdl.repository.reader;

import java.util.List;

import org.springframework.data.jpa.repository.Query;

import com.latticeengines.apps.cdl.repository.PublishedTalkingPointRepository;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;

public interface PublishedTalkingPointReaderRepository extends PublishedTalkingPointRepository {

    @Query("SELECT new com.latticeengines.domain.exposed.cdl.TalkingPointDTO(ptp, p.displayName) "
            + "FROM PublishedTalkingPoint ptp  " + "JOIN Play p on p.name = ptp.playName" + " Where p.tenantId = ?1")
    List<TalkingPointDTO> findAllByTenantPid(long tenantPid);
}
