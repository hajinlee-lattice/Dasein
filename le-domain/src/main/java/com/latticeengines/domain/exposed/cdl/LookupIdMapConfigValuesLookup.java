package com.latticeengines.domain.exposed.cdl;

import java.util.HashMap;
import java.util.Map;

public class LookupIdMapConfigValuesLookup {
    static final Map<CDLExternalSystemName, String> EXT_SYS_NAME_TO_END_DEST_ID = new HashMap<>();
    static {
        EXT_SYS_NAME_TO_END_DEST_ID.put(CDLExternalSystemName.Adobe_Audience_Mgr,
                "external_dsp_adobe_am_cooke_data_source_id");
        EXT_SYS_NAME_TO_END_DEST_ID.put(CDLExternalSystemName.AppNexus, "external_dsp_appnexus_client_id");
        EXT_SYS_NAME_TO_END_DEST_ID.put(CDLExternalSystemName.Google_Display_N_Video_360,
                "external_dsp_googledv_entity_id");
        EXT_SYS_NAME_TO_END_DEST_ID.put(CDLExternalSystemName.TradeDesk, "external_dsp_trade_desk_advertiser_id");
        EXT_SYS_NAME_TO_END_DEST_ID.put(CDLExternalSystemName.Verizon_Media, "external_dsp_verizon_oath_mdm_id");
    }

    public static boolean containsEndDestinationIdKey(CDLExternalSystemName externalSystemName) {
        return EXT_SYS_NAME_TO_END_DEST_ID.containsKey(externalSystemName);
    }

    public static String getEndDestinationIdKey(CDLExternalSystemName externalSystemName) {
        return EXT_SYS_NAME_TO_END_DEST_ID.get(externalSystemName);
    }
}
