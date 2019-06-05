package com.latticeengines.apps.cdl.util;

import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.rating.CrossSellRatingQueryBuilder;
import com.latticeengines.apps.cdl.rating.RatingQueryBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.AdvancedModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public class CrossSellQueryBuilderTestNG {

    private String playJson = "{\"pid\":68559,\"id\":\"engine_cy_gzmx_rlu3lyekgiamyg\",\"displayName\":\"Repeat Purchase - 2019/06/04 22:18:07\",\"note\":null,\"type\":\"CROSS_SELL\",\"status\":\"INACTIVE\",\"deleted\":false,\"segment\":{\"name\":\"segment1559686857301\",\"display_name\":\"PLS-13551\",\"created_by\":\"jlmehta@lattice-engines.com\",\"updated\":1559686889000,\"created\":1559686865000,\"deleted\":false,\"is_master_segment\":false,\"account_raw_restriction\":{\"logicalRestriction\":{\"operator\":\"AND\",\"restrictions\":[{\"bucketRestriction\":{\"bkt\":{\"Lbl\":\"Yes\",\"Cnt\":363,\"Id\":1,\"Txn\":{\"PrdId\":\"829Xt1N7sZkyKbaL0cILdmbqfzuEyOQt\",\"Time\":{\"Cmp\":\"BETWEEN_DATE\",\"Vals\":[],\"Period\":\"Date\"},\"Negate\":false}},\"ignored\":false,\"attr\":\"PurchaseHistory.AM_829Xt1N7sZkyKbaL0cILdmbqfzuEyOQt__EVER__HP\"}},{\"bucketRestriction\":{\"bkt\":{\"Lbl\":\"USA\",\"Cnt\":3015,\"Id\":1,\"Cmp\":\"EQUAL\",\"Vals\":[\"USA\"]},\"ignored\":false,\"attr\":\"Account.Country\"}}]}},\"contact_raw_restriction\":{\"logicalRestriction\":{\"operator\":\"AND\",\"restrictions\":[]}},\"account_restriction\":{\"restriction\":{\"logicalRestriction\":{\"operator\":\"AND\",\"restrictions\":[{\"bucketRestriction\":{\"bkt\":{\"Lbl\":\"Yes\",\"Cnt\":363,\"Id\":1,\"Txn\":{\"PrdId\":\"829Xt1N7sZkyKbaL0cILdmbqfzuEyOQt\",\"Time\":{\"Cmp\":\"BETWEEN_DATE\",\"Vals\":[],\"Period\":\"Date\"},\"Negate\":false}},\"ignored\":false,\"attr\":\"PurchaseHistory.AM_829Xt1N7sZkyKbaL0cILdmbqfzuEyOQt__EVER__HP\"}},{\"bucketRestriction\":{\"bkt\":{\"Lbl\":\"USA\",\"Cnt\":3015,\"Id\":1,\"Cmp\":\"EQUAL\",\"Vals\":[\"USA\"]},\"ignored\":false,\"attr\":\"Account.Country\"}}]}}},\"contact_restriction\":{\"restriction\":{\"logicalRestriction\":{\"operator\":\"AND\",\"restrictions\":[]}}}},\"created\":1559686688000,\"updated\":1559686911000,\"createdBy\":\"jlmehta@lattice-engines.com\",\"lastRefreshedDate\":1550572886000,\"advancedRatingConfig\":{\"cross_sell\":{\"modelingStrategy\":\"CROSS_SELL_REPEAT_PURCHASE\"}},\"bucketMetadata\":null,\"latest_iteration\":{\"AI\":{\"pid\":1710265,\"id\":\"ai_gf8jl6j0rxowcw17kypm3a\",\"iteration\":1,\"created\":1559686688000,\"updated\":1559686933000,\"createdBy\":\"jlmehta@lattice-engines.com\",\"derived_from_rating_model\":null,\"ratingmodel_attributes\":null,\"predictionType\":\"PROPENSITY\",\"modelingJobId\":null,\"modelingJobStatus\":\"Pending\",\"trainingSegment\":null,\"advancedModelingConfig\":{\"cross_sell\":{\"targetProducts\":[\"DjFaP8bjrIbYWNmV4j4Q5FzSMWfeoP\",\"BDB9iyFDMyzFBNHe8JISyy5ud1h6T\"],\"modelingStrategy\":\"CROSS_SELL_REPEAT_PURCHASE\",\"filters\":{\"PURCHASED_BEFORE_PERIOD\":{\"configName\":\"PURCHASED_BEFORE_PERIOD\",\"criteria\":\"PRIOR_ONLY\",\"value\":2}}}},\"modelSummaryId\":null}},\"scoring_iteration\":null,\"published_iteration\":null,\"justCreated\":true,\"counts\":null}";

    @Test(groups = "unit")
    public void testMain() {
        RatingEngine re = JsonUtils.deserialize(playJson, RatingEngine.class);

        RatingQueryBuilder builder = CrossSellRatingQueryBuilder.getCrossSellRatingQueryBuilder(re,
                (AIModel) re.getLatestIteration(), ModelingQueryType.TRAINING, "Month", 209, null);

        AdvancedModelingConfig config = new CustomEventModelingConfig();
        System.out.println(JsonUtils.serialize(config));

        EventFrontEndQuery efeq = builder.build();
        int i = 0;
    }
}
