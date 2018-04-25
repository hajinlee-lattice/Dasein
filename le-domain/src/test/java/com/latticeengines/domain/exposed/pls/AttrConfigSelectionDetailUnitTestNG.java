package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class AttrConfigSelectionDetailUnitTestNG {

    private AttrConfigSelectionDetail selectionDetail = new AttrConfigSelectionDetail();

    @Test(groups = "unit")
    public void test() {
        selectionDetail.setEntity(BusinessEntity.Account);
        selectionDetail.setSelected(500L);
        selectionDetail.setLimit(500L);
        selectionDetail.setTotalAttrs(3000L);
        Map<String, AttrConfigSelectionDetail.SubcategoryDetail> subcategories = new HashMap<>();
        selectionDetail.setSubcategories(subcategories);
        AttrConfigSelectionDetail.SubcategoryDetail subcategoryDetail1 = new AttrConfigSelectionDetail.SubcategoryDetail();
        subcategoryDetail1.setSelected(100L);
        subcategoryDetail1.setTotalAttrs(200L);
        subcategoryDetail1.setHasFrozenAttrs(Boolean.TRUE);
        Map<String, AttrConfigSelectionDetail.AttrDetail> attributes1 = new HashMap<>();
        subcategoryDetail1.setAttributes(attributes1);
        AttrConfigSelectionDetail.AttrDetail attr11Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes1.put("Attr11", attr11Detail);
        attr11Detail.setDisplayName("Attr 1-1");
        attr11Detail.setDescription("Attr 1 1");
        attr11Detail.setSelected(Boolean.FALSE);

        AttrConfigSelectionDetail.AttrDetail attr12Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes1.put("Attr12", attr12Detail);
        attr12Detail.setDisplayName("Attr 1-2");
        attr12Detail.setDescription("Attr 1 2");
        attr12Detail.setSelected(Boolean.TRUE);

        AttrConfigSelectionDetail.AttrDetail attr13Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes1.put("Attr13", attr13Detail);
        attr13Detail.setDisplayName("Attr 1-3");
        attr13Detail.setDescription("Attr 1 3");
        attr13Detail.setSelected(Boolean.TRUE);
        attr13Detail.setIsFrozen(Boolean.TRUE);

        AttrConfigSelectionDetail.AttrDetail attr14Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes1.put("Attr14", attr14Detail);
        attr14Detail.setDisplayName("Attr 1-4");
        attr14Detail.setDescription("Attr 1 4");
        attr14Detail.setSelected(Boolean.FALSE);
        attr14Detail.setIsFrozen(Boolean.TRUE);

        AttrConfigSelectionDetail.SubcategoryDetail subcategoryDetail2 = new AttrConfigSelectionDetail.SubcategoryDetail();
        subcategoryDetail2.setSelected(2L);
        subcategoryDetail2.setTotalAttrs(2L);
        subcategoryDetail2.setHasFrozenAttrs(Boolean.FALSE);
        Map<String, AttrConfigSelectionDetail.AttrDetail> attributes2 = new HashMap<>();
        subcategoryDetail2.setAttributes(attributes2);
        AttrConfigSelectionDetail.AttrDetail attr21Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes2.put("Attr21", attr21Detail);
        attr21Detail.setDisplayName("Attr 2-1");
        attr21Detail.setDescription("Attr 2 1");
        attr21Detail.setSelected(Boolean.FALSE);

        AttrConfigSelectionDetail.AttrDetail attr22Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes2.put("Attr22", attr22Detail);
        attr22Detail.setDisplayName("Attr 2-2");
        attr22Detail.setDescription("Attr 2 2");
        attr22Detail.setSelected(Boolean.TRUE);

        subcategories.put("Subcategory1", subcategoryDetail1);
        subcategories.put("Subcategory2", subcategoryDetail2);

        System.out.println(selectionDetail);
    }

}
