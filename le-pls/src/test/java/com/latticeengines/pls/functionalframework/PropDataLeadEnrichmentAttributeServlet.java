package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.springframework.http.MediaType;

import com.latticeengines.common.exposed.converter.KryoHttpMessageConverter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.KryoUtils;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.StatisticalType;
import com.latticeengines.domain.exposed.metadata.Tag;

public class PropDataLeadEnrichmentAttributeServlet extends HttpServlet {

    private static final long serialVersionUID = -6068276954643550403L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String header = req.getHeader(HttpHeaders.ACCEPT);
        if (KryoHttpMessageConverter.KRYO_VALUE.equals(header)) {
            returnKryoResponse(resp);
        } else {
            returnJsonResponse(resp);
        }
    }

    private void returnKryoResponse(HttpServletResponse resp) throws IOException {
        resp.setContentType(KryoHttpMessageConverter.KRYO_VALUE);

        List<ColumnMetadata> columns = Arrays.asList(getFirstColumn(), getSecondColumn());
        KryoUtils.write(resp.getOutputStream(), columns);

        resp.setStatus(HttpStatus.SC_OK);
    }

    private void returnJsonResponse(HttpServletResponse resp) throws IOException {
        resp.setContentType(MediaType.APPLICATION_JSON_VALUE);

        List<ColumnMetadata> columns = Arrays.asList(getFirstColumn(), getSecondColumn());
        String json = JsonUtils.serialize(columns);
        resp.getWriter().write(json);

        resp.setStatus(HttpStatus.SC_OK);
    }

    private ColumnMetadata getFirstColumn() {
        ColumnMetadata column = new ColumnMetadata();
        column.setColumnName("TechIndicator_AddThis");
        column.setDisplayName("Add This");
        column.setDataType("NVARCHAR(50)");
        column.setMatchDestination("BuiltWith_Pivoted_Source");
        column.setTagList(Collections.singletonList(Tag.EXTERNAL));
        column.setFundamentalType(FundamentalType.BOOLEAN);
        column.setStatisticalType(StatisticalType.ORDINAL);
        column.setDescription("Tech Indicator Add This");
        column.setApprovedUsageList(Arrays.asList(ApprovedUsage.MODEL));
        column.setCategory(Category.WEBSITE_PROFILE);
        return column;
    }

    private ColumnMetadata getSecondColumn() {
        ColumnMetadata column = new ColumnMetadata();
        column = new ColumnMetadata();
        column.setColumnName("TechIndicator_RemoveThis");
        column.setDisplayName("Remove This");
        column.setDataType("NVARCHAR(100)");
        column.setMatchDestination("HGData");
        column.setTagList(Collections.singletonList(Tag.EXTERNAL));
        column.setFundamentalType(FundamentalType.ALPHA);
        column.setStatisticalType(StatisticalType.INTERVAL);
        column.setDescription("Tech Indicator Remove This");
        column.setApprovedUsageList(Collections.singletonList(ApprovedUsage.MODEL_MODELINSIGHTS));
        column.setCategory(Category.WEBSITE_PROFILE);
        return column;
    }
}
