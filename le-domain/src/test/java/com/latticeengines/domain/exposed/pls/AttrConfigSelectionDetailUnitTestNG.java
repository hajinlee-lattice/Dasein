package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.AttrDetail;

public class AttrConfigSelectionDetailUnitTestNG {

    private AttrConfigSelectionDetail selectionDetail = new AttrConfigSelectionDetail();

    @Test(groups = "unit")
    public void test() {
        selectionDetail.setSelected(500L);
        selectionDetail.setLimit(500L);
        selectionDetail.setTotalAttrs(3000L);
        List<AttrConfigSelectionDetail.SubcategoryDetail> subcategories = new ArrayList<>();
        selectionDetail.setSubcategories(subcategories);
        AttrConfigSelectionDetail.SubcategoryDetail subcategoryDetail1 = new AttrConfigSelectionDetail.SubcategoryDetail();
        subcategoryDetail1.setSubCategory("Subcategory1");
        subcategoryDetail1.setSelected(true);
        subcategoryDetail1.setTotalAttrs(200L);
        subcategoryDetail1.setHasFrozenAttrs(Boolean.TRUE);
        List<AttrDetail> attributes1 = new ArrayList<>();
        subcategoryDetail1.setAttributes(attributes1);
        AttrConfigSelectionDetail.AttrDetail attr11Detail = new AttrConfigSelectionDetail.AttrDetail();
        attr11Detail.setAttribute("attr11");
        attributes1.add(attr11Detail);
        attr11Detail.setDisplayName("Attr 1-1");
        attr11Detail.setDescription("Attr 1 1");
        attr11Detail.setSelected(Boolean.FALSE);

        AttrConfigSelectionDetail.AttrDetail attr12Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes1.add(attr12Detail);
        attr12Detail.setAttribute("attr12");
        attr12Detail.setDisplayName("Attr 1-2");
        attr12Detail.setDescription("Attr 1 2");
        attr12Detail.setSelected(Boolean.TRUE);

        AttrConfigSelectionDetail.AttrDetail attr13Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes1.add(attr13Detail);
        attr13Detail.setAttribute("Attr13");
        attr13Detail.setDisplayName("Attr 1-3");
        attr13Detail.setDescription("Attr 1 3");
        attr13Detail.setSelected(Boolean.TRUE);
        attr13Detail.setIsFrozen(Boolean.TRUE);

        AttrConfigSelectionDetail.AttrDetail attr14Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes1.add(attr14Detail);
        attr14Detail.setAttribute("Attr14");
        attr14Detail.setDisplayName("Attr 1-4");
        attr14Detail.setDescription("Attr 1 4");
        attr14Detail.setSelected(Boolean.FALSE);
        attr14Detail.setIsFrozen(Boolean.TRUE);

        AttrConfigSelectionDetail.SubcategoryDetail subcategoryDetail2 = new AttrConfigSelectionDetail.SubcategoryDetail();
        subcategoryDetail2.setSubCategory("Subcategory2");
        subcategoryDetail2.setSelected(true);
        subcategoryDetail2.setTotalAttrs(2L);
        subcategoryDetail2.setHasFrozenAttrs(Boolean.FALSE);
        List<AttrDetail> attributes2 = new ArrayList<>();
        subcategoryDetail2.setAttributes(attributes2);
        AttrConfigSelectionDetail.AttrDetail attr21Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes2.add(attr21Detail);
        attr21Detail.setAttribute("Attr21");
        attr21Detail.setDisplayName("Attr 2-1");
        attr21Detail.setDescription("Attr 2 1");
        attr21Detail.setSelected(Boolean.FALSE);

        AttrConfigSelectionDetail.AttrDetail attr22Detail = new AttrConfigSelectionDetail.AttrDetail();
        attributes2.add(attr22Detail);
        attr22Detail.setAttribute("Attr22");
        attr22Detail.setDisplayName("Attr 2-2");
        attr22Detail.setDescription("Attr 2 2");
        attr22Detail.setSelected(Boolean.TRUE);

        subcategories.add(subcategoryDetail1);
        subcategories.add(subcategoryDetail2);

        System.out.println(selectionDetail);
    }

}
