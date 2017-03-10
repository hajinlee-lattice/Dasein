package com.latticeengines.transform.v2_0_25.functions;

import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class AddTitleAttributesV2UnitTestNG {

    private AddTitleAttributesV2 addTitleAttributes;

    public AddTitleAttributesV2UnitTestNG() {
        URL url = ClassLoader.getSystemResource("com/latticeengines/transform/v2_0_25/functions/addTitleAttributesV2");
        addTitleAttributes = new AddTitleAttributesV2(url.getFile());
    }

    @Test(groups = "unit")
    public void transform() throws Exception {

        Map<String, Object> args = new HashMap<>();
        args.put("column1", "Title");
        args.put("column2", "DS_Title_Length");

        Map<Object, Object> valuesToCheck = new HashMap<Object, Object>();
        valuesToCheck.put("CEO", 3.0);
        valuesToCheck.put("", 0.0);
        valuesToCheck.put(null, 15.06);
        valuesToCheck.put("AStringThatIsLongerThanThirtyCharacters", 30.0);
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("AValue", 0.0);
        valuesToCheck.put(null, 1.0);
        args.put("column2", "DS_Title_IsNull");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("Engineer", 0.0);
        valuesToCheck.put(null, 0.0);
        valuesToCheck.put("Professor Bilbo", 1.0);
        valuesToCheck.put("Mr. Professor Bilbo", 1.0);
        valuesToCheck.put("Ph.D. Student", 1.0);
        valuesToCheck.put("CEO", 0.0);
        valuesToCheck.put("12.3", 0.0);
        valuesToCheck.put("PhDStudent", 0.0);
        valuesToCheck.put("PhD_Student", 1.0);
        args.put("column2", "DS_Title_IsAcademic");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("Engineer", 1.0);
        valuesToCheck.put(null, 1.0);
        valuesToCheck.put("Techfabulous", 1.0);
        valuesToCheck.put("Robotech", 0.0);
        valuesToCheck.put("web.info", 1.0);
        valuesToCheck.put("Sr. Programmer", 1.0);
        args.put("column2", "DS_Title_IsTechRelated");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("Best_Director", 1.0);
        valuesToCheck.put(null, 0.0);
        valuesToCheck.put("Dir.", 1.0);
        valuesToCheck.put("director", 1.0);
        valuesToCheck.put("indirector", 0.0);
        valuesToCheck.put("Sr. Dir_of_Something", 1.0);
        args.put("column2", "DS_Title_IsDirector");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("sriracha", 0.0);
        valuesToCheck.put(null, 1.0);
        valuesToCheck.put("SrDir.", 0.0);
        valuesToCheck.put("Sr Dir", 1.0);
        valuesToCheck.put("senior senior", 1.0);
        valuesToCheck.put("Sr. Dir_of_Something", 1.0);
        args.put("column2", "DS_Title_IsSenior");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("mvp", 0.0);
        valuesToCheck.put(null, 0.0);
        valuesToCheck.put("Vp of Rev.", 1.0);
        valuesToCheck.put("Chieffy", 0.0);
        valuesToCheck.put("Sr Chief Dr", 1.0);
        valuesToCheck.put("Ctoo", 0.0);
        valuesToCheck.put("CXO", 1.0);
        valuesToCheck.put("Pres of US", 1.0);
        valuesToCheck.put("Head_of_Tech", 1.0);
        valuesToCheck.put("Managing.Something", 0.0);
        valuesToCheck.put("A partner in crime", 0.0);
        valuesToCheck.put("A managing partner in crime", 1.0);
        args.put("column2", "DS_Title_IsVPAbove");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("Mgr.", 1.0);
        valuesToCheck.put(null, 0.0);
        valuesToCheck.put("Managerial", 1.0);
        valuesToCheck.put("ManagerOfAll", 1.0);
        valuesToCheck.put("My Manager", 1.0);
        valuesToCheck.put("Sr_Mngr", 1.0);
        valuesToCheck.put("SoMgr", 0.0);
        args.put("column2", "DS_Title_IsManager");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("Consumer", 3.1);
        valuesToCheck.put(null, 2.0);
        valuesToCheck.put("OhRetail", 2.0);
        valuesToCheck.put("Retail Price", 3.1);
        valuesToCheck.put("starship_enterprise", 4.58);
        valuesToCheck.put("CA State Government", 5.4);
        valuesToCheck.put("delete CA State Government", 5.4);
        valuesToCheck.put("delete the value", 1.11);
        args.put("column2", "DS_Title_Channel");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("Mktg", 1.59);
        valuesToCheck.put(null, 2.16);
        valuesToCheck.put("it", 3.78);
        valuesToCheck.put("infoblahtech", 3.78);
        valuesToCheck.put("tech", 1.79);
        valuesToCheck.put("sale", 0.0);
        valuesToCheck.put("human resource", 0.0);
        valuesToCheck.put("data analysis", 1.66);
        valuesToCheck.put("academical", 0.0);
        valuesToCheck.put("none consultants", 1.56);
        args.put("column2", "DS_Title_Function");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("associationbynetwork", 2.08);
        valuesToCheck.put(null, 2.07);
        valuesToCheck.put("secret lover", 1.49);
        valuesToCheck.put("mvp", 1.28);
        valuesToCheck.put("cfo", 2.54);
        valuesToCheck.put("dir", 2.9);
        valuesToCheck.put("dev", 2.07);
        valuesToCheck.put("partner in crime", 0.0);
        valuesToCheck.put("US Representative", 0.0);
        valuesToCheck.put("no no", 0.0);
        args.put("column2", "DS_Title_Role");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("district", 0.0);
        valuesToCheck.put(null, 2.01);
        valuesToCheck.put("united states", 2.01);
        valuesToCheck.put("us", 0.0);
        valuesToCheck.put("north of the great america", 0.0);
        valuesToCheck.put("southamerica", 0.0);
        valuesToCheck.put("worldwide", 6.12);
        valuesToCheck.put("western", 0.0);
        valuesToCheck.put("at home", 2.01);
        valuesToCheck.put("delete me", 0.0);
        args.put("column2", "DS_Title_Scope");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put("none", 1.0);
        valuesToCheck.put(null, 0.0);
        valuesToCheck.put("#", 1.0);
        valuesToCheck.put("9", 1.0);
        valuesToCheck.put("a", 0.0);
        valuesToCheck.put("GottaGo!", 1.0);
        valuesToCheck.put("asd", 1.0);
        args.put("column2", "DS_Title_HasUnusualChar");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put(null, 15.0);
        valuesToCheck.put("Director VP", 12.0);
        valuesToCheck.put("senior mgr", 3.0);
        valuesToCheck.put("a#", 0.0);
        valuesToCheck.put("Senior VP", 9.0);
        valuesToCheck.put("Engineer", 0.0);
        args.put("column2", "DS_Title_Level");
        checkValues(args, valuesToCheck);

        valuesToCheck.clear();
        valuesToCheck.put(null, 1.73);
        valuesToCheck.put("Director VP", 3.06);
        valuesToCheck.put("senior mgr", 3.45);
        valuesToCheck.put("a#", 0.66);
        valuesToCheck.put("Senior VP", 3.06);
        valuesToCheck.put("Engineer", 1.73);
        valuesToCheck.put("Manager, eCommerce", 3.45);
        args.put("column2", "DS_Title_Level_Categorical");
        checkValues(args, valuesToCheck);
    }

    private void checkValues(Map<String, Object> args, Map<Object, Object> valuesToCheck) {
        for (Object key : valuesToCheck.keySet()) {
            Map<String, Object> record1 = new HashMap<>();
            record1.put("Title", key);
            Object result1 = addTitleAttributes.transform(args, record1);
            assertEquals(result1, valuesToCheck.get(key));
            if (result1 != null)
                assertEquals(result1.getClass(), valuesToCheck.get(key).getClass());
        }
    }
}
