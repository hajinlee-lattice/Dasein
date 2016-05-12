package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import org.apache.commons.collections.OrderedMap;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.v2_0_25.common.DSUtils;

public class StdVisidbDsTitleRole implements RealTimeTransform {

    private static final long serialVersionUID = -2648663303512664149L;
    private static OrderedMap mapTitleRole = null;

    public StdVisidbDsTitleRole() {
        
    }
    
    public StdVisidbDsTitleRole(String modelPath) {

    }

    @SuppressWarnings("unchecked")
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
            mapTitleRole = new LinkedMap();
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
    public Attribute getMetadata() {
        Attribute attribute = new Attribute();
        attribute.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
        attribute.setCategory(Category.LEAD_INFORMATION);
        attribute.setFundamentalType(FundamentalType.ALPHA);
        attribute.setStatisticalType(StatisticalType.NOMINAL);
        attribute.setDescription("Title Role");
        attribute.setDisplayName("Title Role");
        attribute.setTags(Tag.INTERNAL_TRANSFORM);
        return attribute;
    }

}
