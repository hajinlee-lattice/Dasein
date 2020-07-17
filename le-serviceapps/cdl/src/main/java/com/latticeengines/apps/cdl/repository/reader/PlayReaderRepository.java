package com.latticeengines.apps.cdl.repository.reader;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.apps.cdl.repository.PlayRepository;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.pls.Play;

public interface PlayReaderRepository extends PlayRepository {

    @Query("SELECT p FROM Play p " + //
            "WHERE p.deleted = false " + //
            "AND p.pid in (SELECT DISTINCT c.play.pid FROM PlayLaunchChannel c " + //
            "WHERE c.isAlwaysOn = true AND c.lookupIdMap.externalSystemName = :externalSystemName " + //
            "AND c.channelConfig LIKE %:attributeSetName%)")
    List<Play> findByAlwaysOnAndAttrSetName(@Param("externalSystemName") CDLExternalSystemName externalSystemName,
                                            @Param("attributeSetName") String attributeSetName);
}
