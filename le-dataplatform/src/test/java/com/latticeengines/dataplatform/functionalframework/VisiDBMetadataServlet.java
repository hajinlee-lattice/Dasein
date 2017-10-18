package com.latticeengines.dataplatform.functionalframework;

import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpStatus;

import com.latticeengines.domain.exposed.dataplatform.visidb.GetQueryMetaDataColumnsResponse;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

public class VisiDBMetadataServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    
    private List<String> colNames;
    private List<Integer> dataTypes;
    
    public VisiDBMetadataServlet(String[] colNames, Integer[] dataTypes) {
        this.colNames = Arrays.<String>asList(colNames);
        this.dataTypes = Arrays.<Integer>asList(dataTypes);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        resp.setContentType("application/json");
        GetQueryMetaDataColumnsResponse metadata = new GetQueryMetaDataColumnsResponse();
        metadata.setStatus(3);
        List<ModelingMetadata.AttributeMetadata> metadataCols = new ArrayList<>();
        
        for (int i = 0; i < colNames.size(); i++) {
            String colName = colNames.get(i);
            Integer dataType = dataTypes.get(i);
            ModelingMetadata.AttributeMetadata m = new ModelingMetadata.AttributeMetadata();
            m.setColumnName(colName);
            
            if (dataType >= Types.NUMERIC && dataType <= Types.DOUBLE) {
                m.setDisplayDiscretizationStrategy(getDisplayDiscretizationStrategy());
                m.setStatisticalType("ratio");
            } else if (new Random().nextInt(2) == 0) {
                m.setStatisticalType("nominal");
            }
            
            metadataCols.add(m);
        }
        metadata.setMetadata(metadataCols);
        System.out.println(metadata.toString());
        resp.getWriter().write(metadata.toString());
        resp.setStatus(HttpStatus.SC_OK);
    }
    
    private String getDisplayDiscretizationStrategy() {
        Random r = new Random();
        int value = r.nextInt(3);
        
        switch (value) {
        case 0:
            return "{\"linear\":{\"minSamples\": 100, \"stepSize\": 10, \"minFreq\": 0.005, \"maxBuckets\": 7, \"maxPercentile\": 1, \"minValue\": 1900}}";
        case 1:
            return "{\"geometric\":{\"minValue\": 10, \"minSamples\": 100, \"minFreq\": 0, \"maxBuckets\": 7, \"maxPercentile\": 1, \"multiplierList\": [2.5, 2, 2]}}";
        case 2:
            return "{\"standard\":{\"numBins\":10, \"minSamples\":100, \"minFreq\":0, \"maxPercentile\":1, \"maxBuckets\":7}}";
        }
        return null; 
    }

}
