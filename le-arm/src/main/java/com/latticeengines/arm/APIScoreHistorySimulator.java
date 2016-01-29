package com.latticeengines.arm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;

// Score request
//      #records
//      processing time
//
//      tenantId
//      record type
//      Model ID
//      Model Name

// Score Record
//      score
//      processing time
//
//      tenantId
//      domain
//      record type
//      Model ID
//      Model Name

public class APIScoreHistorySimulator {
    private static final String DBNAME = "testscoring";
    private static final String DBURL = "http://bodcdevvmtrx33.dev.lattice.local:8086";
    private static final String ROOT = "root";
    Map<Integer, String> tenantMap = new HashMap<>();
    Map<Integer, Map<Integer, String>> modelMap = new HashMap<>();

    public static void main(String[] args) {
        new APIScoreHistorySimulator().run();
        // System.out.println("Done!");
    }

    private void setup() {
        tenantMap.put(1, "qlik");
        tenantMap.put(2, "alfresco");
        tenantMap.put(3, "mulesoft");
        tenantMap.put(4, "nutanix");
        tenantMap.put(5, "dice");
        tenantMap.put(6, "alteryx");
        tenantMap.put(7, "2checkout");
        tenantMap.put(8, "demandbase");
        tenantMap.put(9, "informatica");
        tenantMap.put(10, "spiceworks");

        int modelIndex = 1;
        for (int key : tenantMap.keySet()) {
            Map<Integer, String> map = new HashMap<>();
            map.put(modelIndex++, "US Lead Scoring");
            map.put(modelIndex++, "EU Lead Scoring");
            modelMap.put(key, map);
        }
    }

    public void run() {
        setup();

        InfluxDB influx = InfluxDBFactory.connect(DBURL, ROOT, ROOT);

        while (true) {
            for (int tenantId : tenantMap.keySet()) {
                long processingTime = ThreadLocalRandom.current().nextLong(3000, 10000);
                long numRecords = ThreadLocalRandom.current().nextLong(1, 11);

                Map<Integer, String> models = modelMap.get(tenantId);

                Iterator<Integer> modelIdIter = models.keySet().iterator();
                int chosenModelId = modelIdIter.next();
                if (ThreadLocalRandom.current().nextInt(0, 2) > 0) {
                    chosenModelId = modelIdIter.next();
                }

                BatchPoints batchPoints = generatePoints(numRecords, processingTime, chosenModelId,
                        models.get(chosenModelId), tenantId, tenantMap.get(tenantId));
                influx.write(batchPoints);
                System.out.println(batchPoints.toString());
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public BatchPoints generatePoints(long numRecords, long processingTime, int modelId, String modelName,
            int tenantId, String tenantName) {
        BatchPoints batchPoints = BatchPoints.database(DBNAME) //
                .tag("recordType", "contacts") //
                .tag("modelId", String.valueOf(modelId)) //
                .tag("modelName", modelName) //
                .tag("tenantId", String.valueOf(tenantId)) //
                .tag("tenantName", tenantName) //
                .tag("releaseVersion", "2.0.22") //
                .tag("environment", "Mock") //
                .tag("service", "ScoringAPI") //
                .retentionPolicy("default") //
                .consistency(ConsistencyLevel.ALL).build();
        Point requestPoint = Point.measurement("scoreRequest") //
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS) //
                .field("records", numRecords) //
                .field("processingTime", processingTime) //
                .build();
        batchPoints.point(requestPoint);

        for (int i = 0; i < numRecords; i++) {
            Point recordPoint = Point.measurement("scoreRecord") //
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS) //
                    .field("score", ThreadLocalRandom.current().nextLong(5, 99)) //
                    .field("processingTime", Math.max(processingTime -= 1000L, 2000L)) //
                    .build();
            batchPoints.point(recordPoint);
        }

        return batchPoints;
    }

    public void queryTest() {
        InfluxDB influx = InfluxDBFactory.connect(DBURL, ROOT, ROOT);
        Query query = new Query("SELECT * from cpu", DBNAME);
        QueryResult qResult = influx.query(query);

        for (Result result : qResult.getResults()) {
            System.out.println(result.toString());
        }
    }
}
