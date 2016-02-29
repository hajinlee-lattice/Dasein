package com.latticeengines.dellebi.entitymanager;

import java.sql.Date;
import java.util.List;

import com.latticeengines.domain.exposed.dellebi.DellEbiConfig;

public interface DellEbiConfigEntityMgr {

    public static final String CONFIGTABLE = "DellEBI_Config";
    public static final String DellEbi_Quote = "QUOTE";
    public static final String DellEbi_OrderSummary = "OrderSummary";
    public static final String DellEbi_OrderDetail = "OrderDetail";
    public static final String DellEbi_SkuGlobal = "SkuGlobal";
    public static final String DellEbi_SkuManufacturer = "SkuManufacturer";
    public static final String DellEbi_SkuItemClassCode = "SkuItemClassCode";
    public static final String DellEbi_Channel = "Channel";

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
}
