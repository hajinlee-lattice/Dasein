package com.latticeengines.pls.end2end.oneoff;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.codehaus.plexus.util.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class FixProdDataComposition extends PlsFunctionalTestNGBase {

    @Autowired
    private Configuration conf;

    @Autowired
    private MetadataProxy metadataProxy;

    private String fileName = "dc.txt" + System.currentTimeMillis();

    @BeforeClass(groups = "manual")
    public void setup() throws Exception {
    }

    @Test(groups = "manual")
    public void fixDataComposition() throws Exception {
        conf.setInt("dfs.replication", 3);
        List<String> customers = Arrays.asList("Halladay_POC.Halladay_POC.Production", "LP3_Sales.LP3_Sales.Production",
                "LP3_Taylor.LP3_Taylor.Production");

        for (String c : customers) {
            List<String> paths = HdfsUtils.getFilesForDirRecursive(conf, "/user/s-analytics/customers/" + c + "/data",
                    new HdfsFileFilter() {

                        @Override
                        public boolean accept(FileStatus file) {
                            try {
                                if (HdfsUtils.fileExists(conf, file.getPath() + ".bak")) {
                                    FileUtils.writeStringToFile(new File(fileName), "Skip path:" + file.getPath()
                                            + "\n", true);
                                    return false;
                                }
                                return file.getPath().getName().equals("datacomposition.json")
                                        && file.getPath().getParent().getName()
                                                .matches(".*With_UserRefinedAttributes-.*-Metadata");
                            } catch (IOException e) {
                                try {
                                    FileUtils.writeStringToFile(new File(fileName), ExceptionUtils.getFullStackTrace(e)
                                            + "\n", true);
                                } catch (IOException e1) {
                                    e1.printStackTrace();
                                }
                            }
                            return false;
                        }

                    });

            for (int i = 0; i < paths.size(); i++) {
                String path = paths.get(i);
                System.out.println(path);
                String eventTableName = StringUtils.substringBeforeLast(new Path(path).getParent().getName(),
                        "UserRefinedAttributes") + "UserRefinedAttributes";
                String customerSpace = new Path(path).getParent().getParent().getParent().getName();
                Table table = metadataProxy.getTable(customerSpace, eventTableName);

                fixRTSAttribute(table.getAttributes());

                Map.Entry<Map<String, FieldSchema>, List<TransformDefinition>> transforms = table
                        .getRealTimeTransformationMetadata();

                DataComposition dataComposition = new DataComposition();
                dataComposition.fields = transforms.getKey();
                dataComposition.transforms = transforms.getValue();

                FileUtils.writeStringToFile(new File(fileName),
                        "->>>>>>>>>>>>>>>>>>>>>>>Starting to update datacomposition.json file at " + path + "\n", true);
                String dc = JsonUtils.serialize(dataComposition);
                HdfsUtils.moveFile(conf, path, path + ".bak");
                HdfsUtils.writeToFile(conf, path, dc);
                FileUtils.writeStringToFile(new File(fileName),
                        "->>>>>>>>>>>>>>>>>>>>>>>Successfully updated datacomposition.json file at " + path + "\n",
                        true);
            }
        }
    }

    private void fixRTSAttribute(List<Attribute> attributes) {
        for (Attribute attr : attributes) {
            for (TransformDefinition transform : TransformationPipeline.getTransforms(TransformationGroup.ALL)) {
                if (transform.output.equals(attr.getName())) {
                    attr.setRTSAttribute(true);
                    break;
                }
            }
        }

    }
}
