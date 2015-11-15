package com.latticeengines.dataplatform.runtime.mapreduce.sampling;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ReservoirSampleUnitTestNG {

    private static final int TOTAL_ELEMENTS = 1000000;
    private static final int[] SAMPLE_SIZES = new int[] { 1000, 10000, 50000, 100000, 500000 };
    private static final double PERCENTAGE_OF_1 = 0.8f;

    
    private int[] allElements = new int[TOTAL_ELEMENTS];
    
    static class ReservoirElement {
        ReservoirElement(Integer element, Double key) {
            this.element = element;
            this.key = key;
        }
        Integer element;
        Double key;
    }
    
    
    @BeforeClass(groups = "unit")
    public void setup() {
        Random random = new Random();
        
        for (int i = 0; i < TOTAL_ELEMENTS; i++) {
            allElements[i] = random.nextDouble() <= PERCENTAGE_OF_1 ? 1 : 0;
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test(groups = "unit")
    public void reservoirSampling() {
        int numSampleSizes = SAMPLE_SIZES.length;
        PriorityQueue<ReservoirElement>[] reservoirs = new PriorityQueue[numSampleSizes];
        Comparator<ReservoirElement> comparator = new Comparator<ReservoirElement>() {

            @Override
            public int compare(ReservoirElement o1, ReservoirElement o2) {
                return o1.key.compareTo(o2.key);
            }

        };
        
        for (int i = 0; i < numSampleSizes; i++) {
            reservoirs[i] = new PriorityQueue<ReservoirElement>(SAMPLE_SIZES[i], comparator);
        }
        Random random = new Random();
        for (Integer element : allElements) {
            double weight = 1.0f;
            double r = Math.pow(random.nextDouble(), 1 / weight);
            ReservoirElement reservoirElement  = new ReservoirElement(element, r);
            
            
            for (int i = 0; i < numSampleSizes; i++) {
                reservoirs[i].offer(reservoirElement);
                if (reservoirs[i].size() > SAMPLE_SIZES[i]) {
                    reservoirs[i].remove();
                }
            }
        }
        
        for (PriorityQueue<ReservoirElement> reservoir : reservoirs) {
            printDistribution(reservoir);
        }
    }
    
    private void printDistribution(PriorityQueue<ReservoirElement> reservoir) {
        int num1 = 0;
        int num0 = 0;
        
        for (ReservoirElement el : reservoir) {
            if (el.element == 1) {
                num1++;
            } else {
                num0++;
            }
        }
        
        System.out.println("Reservoir size = " + reservoir.size());
        System.out.println("#1s = " + num1);
        System.out.println("#0s = " + num0);
    }
}
