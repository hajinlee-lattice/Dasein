package com.latticeengines.transform.v2_0_25.functions;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.exposed.metadata.ApprovedUsage;
import com.latticeengines.transform.exposed.metadata.Category;
import com.latticeengines.transform.exposed.metadata.FundamentalType;
import com.latticeengines.transform.exposed.metadata.StatisticalType;
import com.latticeengines.transform.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.metadata.TransformMetadata;
import com.latticeengines.transform.v2_0_25.common.DSUtils;

public class StdVisidbDsTitleRole implements RealTimeTransform {

    private static final long serialVersionUID = -2648663303512664149L;
    @SuppressWarnings("rawtypes")
    private static LinkedHashMap mapTitleRole = null;

    public StdVisidbDsTitleRole() {
        
    }
    
    public StdVisidbDsTitleRole(String modelPath) {

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return 0.0;

        if (StringUtils.isEmpty(String.valueOf(o)))
            return "";

        if (mapTitleRole == null) {
            // mapTitleRole = new HashMap<String, String>();
            mapTitleRole = new LinkedHashMap();
            mapTitleRole.put("Associate", "assoc");
            mapTitleRole.put("Assistant", "secret, assist");
            mapTitleRole.put("Leadership", "founder, vice, vp, evp, chief, owner, president, svp,ceo,cfo,cto,cio");
            mapTitleRole.put("Manager", "mgr, gm, supervisor, manag, lead");
            mapTitleRole.put("Director", "director, dir");
            mapTitleRole.put("Engineer", "programmer, developer, dev, engineer");
            mapTitleRole.put("Consultant", "consultant");
            mapTitleRole.put("Student_Teacher", "instructor, coach, teacher, student, faculty");
            mapTitleRole.put("Analyst", "analyst");
            mapTitleRole.put("Admin", "admin, dba");
            mapTitleRole.put("Investor_Partner", "partner, investor, board");
            mapTitleRole.put("Controller_Accountant", "controller, accountant");
            mapTitleRole.put("Specialist_Technician", "technician, specialist");
            mapTitleRole.put("Architect", "architect");
            mapTitleRole.put("Representative", "representative");
            mapTitleRole.put("Editor", "editor");
        }

        String s = (String) o;

        return DSUtils.valueReturn(s, mapTitleRole);
    }

    @Override
    public TransformMetadata getMetadata() {
        TransformMetadata metadata = new TransformMetadata();
        metadata.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        metadata.setCategory(Category.LEAD_INFORMATION);
        metadata.setFundamentalType(FundamentalType.ALPHA);
        metadata.setStatisticalType(StatisticalType.NOMINAL);
        metadata.setDescription("Title Role");
        metadata.setDisplayName("Title Role");
        metadata.setTags(Tag.INTERNAL_TRANSFORM);
        return metadata;
    }

}
