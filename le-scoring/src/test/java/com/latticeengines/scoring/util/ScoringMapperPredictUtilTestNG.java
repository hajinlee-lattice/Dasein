package com.latticeengines.scoring.util;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoreOutput;

public class ScoringMapperPredictUtilTestNG {

    private static final String modelID = "2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json";

    @Test(groups = "unit")
    public void testProcessScoreFiles() throws IOException, ParseException {
        // copy over the score.txt file to the current directory
        URL scoreUrl = ClassLoader
                .getSystemResource("com/latticeengines/scoring/results/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.jsonscoringoutputfile-0.txt");
        File dest = new File(System.getProperty("user.dir")
                + "/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.jsonscoringoutputfile-0.txt");
        try {
            FileUtils.copyURLToFile(scoreUrl, dest);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // make up a leadInputRecordMap
        HashMap<String, ArrayList<String>> leadInputRecordMap = new HashMap<String, ArrayList<String>>();
        ArrayList<String> list = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            list.add(i + "");
        }

        leadInputRecordMap.put(modelID, list);

        // parseModelFile
        URL url = ClassLoader
                .getSystemResource("com/latticeengines/scoring/models/2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json");
        String fileName = url.getFile();
        Path path = new Path(fileName);
        HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
        ScoringMapperTransformUtil.parseModelFiles(models, path);

        // make up a modelIdMap
        HashMap<String, String> modelIdMap = new HashMap<String, String>();
        modelIdMap.put(modelID, modelID);

        ArrayList<ScoreOutput> resultList = null;
        resultList = ScoringMapperPredictUtil.processScoreFiles(leadInputRecordMap, models, modelIdMap, 1000);

        ArrayList<ScoreOutput> expectedResultList = new ArrayList<>();
        ScoreOutput result1 = new ScoreOutput("18f446f1-747b-461e-9160-c995c3876ed4", "Highest",
                4.88519256666, modelID, 100, 0.05822784810126582, 0.0777755757027, 6);
        ScoreOutput result2 = new ScoreOutput("47358ca2-a549-4765-a7f7-a7637a565343", "Highest",
                4.88519256666, modelID, 100, 0.05822784810126582, 0.0394015516631, 6);
        ScoreOutput result3 = new ScoreOutput("4821a01c-5a4c-4633-9122-5d050c064d43", "Highest",
                2.65499596014, modelID, 98, 0.03164556962025317, 0.0267911548364, 3);
        ScoreOutput result4 = new ScoreOutput("50d2fcf4-3dbb-46cf-80a3-c1ac96106b07", "Highest",
                2.12399676811, modelID, 95, 0.02531645569620253, 0.0242481348343, 3);
        ScoreOutput result5 = new ScoreOutput("510c48cd-4672-4b91-ad3b-3f904b100913", "High",
                1.76999730676, modelID, 91, 0.02109704641350211, 0.022526096794, 2);
        ScoreOutput result6 = new ScoreOutput("936a6661-c745-4922-a32b-bde68ada894d", "High",
                1.69919741449, modelID, 84, 0.020253164556962026, 0.0189333568863, 2);
        ScoreOutput result7 = new ScoreOutput("93d16654-72db-4ca5-adb5-64e12ef54215", "High",
                1.5778261706, modelID, 81, 0.018806509945750453, 0.0175358841357, 2);
        ScoreOutput result8 = new ScoreOutput("baf39fe9-a184-4a83-9399-45208560dbe4", "High",
                1.48679773768, modelID, 75, 0.017721518987341773, 0.0148266449465, 2);
        ScoreOutput result9 = new ScoreOutput("cd7de65c-b2af-42a5-85af-491cf8503747", "Medium",
                1.15302681698, modelID, 65, 0.013743218806509945, 0.01244724204371, 1);
        ScoreOutput result10 = new ScoreOutput("fd6be1aa-95aa-45b2-adbb-3125a01acf84", "Medium",
                1.06199838406, modelID, 62, 0.012658227848101266, 0.01185827291902, 1);
        expectedResultList.add(result1);
        expectedResultList.add(result2);
        expectedResultList.add(result3);
        expectedResultList.add(result4);
        expectedResultList.add(result5);
        expectedResultList.add(result6);
        expectedResultList.add(result7);
        expectedResultList.add(result8);
        expectedResultList.add(result9);
        expectedResultList.add(result10);
        assertTrue(resultListsAreSame(expectedResultList, resultList));

        // delete the score.txt file to the current directory
        dest.delete();
    }

    private boolean resultListsAreSame(ArrayList<ScoreOutput> list1, ArrayList<ScoreOutput> list2) {
        boolean isSame = true;
        for (int i = 0; i < list1.size(); i++) {
            ScoreOutput result1 = list1.get(i);
            boolean hasMatchingResult = false;
            for (int j = 0; j < list2.size(); j++) {
                ScoreOutput result2 = list2.get(j);
                if (resultIsSame(result1, result2)) {
                    hasMatchingResult = true;
                    break;
                }
            }
            if (!hasMatchingResult) {
                System.out.println(result1 + " does not have matching result");
                isSame = false;
                break;
            }
        }
        return isSame;
    }

    @Test(groups = "unit")
    public void testWriteToOutputFile() throws IllegalArgumentException, Exception {
        ArrayList<ScoreOutput> expectedResultList = new ArrayList<ScoreOutput>();
        ScoreOutput result1 = new ScoreOutput("18f446f1-747b-461e-9160-c995c3876ed4", "Highest",
                4.88519256666, modelID, 100, 0.05822784810126582, 0.0777755757027, 6);
        ScoreOutput result2 = new ScoreOutput("47358ca2-a549-4765-a7f7-a7637a565343", "Highest",
                4.88519256666, modelID, 100, 0.05822784810126582, 0.0394015516631, 6);
        ScoreOutput result3 = new ScoreOutput("4821a01c-5a4c-4633-9122-5d050c064d43", "Highest",
                2.65499596014, modelID, 98, 0.03164556962025317, 0.0267911548364, 3);
        ScoreOutput result4 = new ScoreOutput("50d2fcf4-3dbb-46cf-80a3-c1ac96106b07", "Highest",
                2.12399676811, modelID, 95, 0.02531645569620253, 0.0242481348343, 3);
        ScoreOutput result5 = new ScoreOutput("510c48cd-4672-4b91-ad3b-3f904b100913", "High",
                1.76999730676, modelID, 91, 0.02109704641350211, 0.022526096794, 2);
        expectedResultList.add(result1);
        expectedResultList.add(result2);
        expectedResultList.add(result3);
        expectedResultList.add(result4);
        expectedResultList.add(result5);

        // create a temp folder
        String tempOutputPath = "/user/s-analytics/customers/ScoringMapperPredictUtilUnitTestNG/scoring";
        try {
            if (HdfsUtils.fileExists(new Configuration(), tempOutputPath)) {
                HdfsUtils.rmdir(new Configuration(), tempOutputPath);
            }
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        ScoringMapperPredictUtil.writeToOutputFile(expectedResultList, new Configuration(), tempOutputPath);

        // Deserialize
        FileSystem fs = FileSystem.get(new Configuration());
        List<String> fileList = null;
        try {
            fileList = HdfsUtils.getFilesForDir(new Configuration(), tempOutputPath);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        assertTrue(fileList.size() == 1, "The fileList should only have one element.");
        InputStream is = fs.open(new Path(fileList.get(0)));
        File file = new File("temp.avro");
        FileUtils.copyInputStreamToFile(is, file);

        SpecificDatumReader<ScoreOutput> reader = new SpecificDatumReader<ScoreOutput>(
                ScoreOutput.class);
        DataFileReader<ScoreOutput> dataFileReader = new DataFileReader<ScoreOutput>(file, reader);
        ArrayList<ScoreOutput> generatedResultList = new ArrayList<ScoreOutput>();
        ScoreOutput result = null;
        System.out.println("print out the ScoreOutputs");
        while (dataFileReader.hasNext()) {
            result = dataFileReader.next();
            System.out.println(result);
            generatedResultList.add(result);
        }
        assertTrue(expectedResultList.size() == generatedResultList.size(),
                "The resultLists should have the same size.");
        assertTrue(resultListsAreSame(expectedResultList, generatedResultList),
                "The resultLists should have be the same.");

        // delete the temp folder and the temp file
        dataFileReader.close();
        try {
            HdfsUtils.rmdir(new Configuration(), tempOutputPath);
            file.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean resultIsSame(ScoreOutput result1, ScoreOutput result2) {
        double eps = 1e-6;
        boolean isSame = true;
        if (!compareTwoCharSequences(result1.getLeadID(), result2.getLeadID())) {
            isSame = false;
        }
        if (!compareTwoCharSequences(result1.getBucketDisplayName(), result2.getBucketDisplayName())) {
            isSame = false;
        }
        if ((result1.getLift() - result2.getLift()) >= eps) {
            isSame = false;
        }
        if (!compareTwoCharSequences(result1.getPlay_Display_Name(), result2.getPlay_Display_Name())) {
            isSame = false;
        }
        if (result1.getPercentile() != result2.getPercentile()) {
            isSame = false;
        }
        if ((result1.getProbability() - result2.getProbability()) >= eps) {
            isSame = false;
        }
        if ((result1.getRawScore() - result2.getRawScore()) >= eps) {
            isSame = false;
        }
        if (result1.getScore() != result2.getScore()) {
            isSame = false;
        }
        return isSame;
    }

    private boolean compareTwoCharSequences(CharSequence charSequence1, CharSequence charSequence2) {
        boolean same = true;
        if (charSequence1.length() != charSequence2.length()) {
            same = false;
        } else {
            for (int i = 0; i < charSequence1.length(); i++) {
                if (charSequence1.charAt(i) != charSequence1.charAt(i)) {
                    same = false;
                }
            }
        }
        return same;
    }

    @Test(groups = "unit")
    public void testEvaluate() throws IOException, InterruptedException {
        // copy over the test scoring.py file to the current directory
        URL scoreUrl = ClassLoader.getSystemResource("com/latticeengines/scoring/python/scoring.py");
        File dest = new File(System.getProperty("user.dir") + "/scoring.py");
        try {
            FileUtils.copyURLToFile(scoreUrl, dest);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // make up models
        HashMap<String, JSONObject> models = new HashMap<String, JSONObject>();
        models.put("model1", null);
        models.put("model2", null);
        String returnedStr = "";
        try {
            returnedStr = ScoringMapperPredictUtil.evaluate(models);
        } catch (LedpException e) {
            assertTrue(e.getCode() == LedpCode.LEDP_200011);
        }
        System.out.println("returnedStr is " + returnedStr) ;

        // delete the score.txt file to the current directory
        dest.delete();
    }
}
