package com.latticeengines.dellebi.entitymanager;

import java.sql.Date;
import java.util.List;

import com.latticeengines.domain.exposed.dellebi.DellEbiConfig;

public interface DellEbiConfigEntityMgr {

    String getInputFields(String type);

    String getOutputFields(String type);

    String getHeaders(String type);

    String getTargetColumns(String type);

    void initialService();

    Date getStartDate(String type);

    String getTargetTable(String type);

    Boolean getIsDeleted(String type);

    List<DellEbiConfig> getConfigs();

    DellEbiConfig getConfigByType(String type);

    Boolean getIsActive(String type);

    String getInboxPath(String type);

    String getBean(String type);

    String getFilePattern(String type);

    int getPriority(String type);

    String getFileTypesByQuartzJob(String beanName);

    DellEbiConfig getConfigByFileName(String fileName);

    String getTypeByFileName(String fileName);

    String getPostStoreProcedure(String type);
}
