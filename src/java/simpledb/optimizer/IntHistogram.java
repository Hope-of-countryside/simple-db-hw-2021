package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private double bucketSize;
    private int[] histogram;

    private int totalValue;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets; // 10000
        this.max = max; // 100
        this.min = min; // 0
        this.histogram = new int[buckets];
        this.bucketSize = 1.0 * (max - min + 1) / buckets; // + 1 is important, because 0 ~ 100 has 101 number
//        if ((max - min) % buckets == 0) {
//            this.bucketSize = (max - min) / buckets;
//        } else {
//            this.bucketSize = (max - min) / (buckets - 1); // 2
//        }
        this.totalValue = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        histogram[getTargetBucket(v)]++;
        this.totalValue++;
    }

    public int getTargetBucket(int v) {
        // 0 100 10000
        return (int) Math.floor((v - min) * 1.0 / bucketSize % buckets);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op) {
            case LIKE:
            case EQUALS:
                return processEquals(v);
            case GREATER_THAN:
                return processGreaterThan(v);
            case LESS_THAN:
                return processLessThan(v);
            case LESS_THAN_OR_EQ:
                return processLessThanOrEqual(v);
            case GREATER_THAN_OR_EQ:
                return processGreaterThanOrEqual(v);
            case NOT_EQUALS:
                return processNotEquals(v);
        }
        return -1.0;
    }

    private double processEquals(int v) {
        if (v < this.min || v > this.max) {
            return 0.0;
        }

        return 1.0 * this.histogram[getTargetBucket(v)] / this.totalValue;
    }

    private double processGreaterThan(int v) {
        if (v < this.min) {
            return 1.0;
        }
        if (v >= this.max) {
            return 0;
        }
        double totalTargetValue = 0;
        int targetBucket = getTargetBucket(v);
        for (int i = targetBucket + 1; i < this.buckets; i++) {
            totalTargetValue += this.histogram[i];
        }
        double rightValueOfBucket = 1.0 * this.histogram[targetBucket] * (bucketSize - (v - min) % this.bucketSize) / this.bucketSize;
        return (totalTargetValue + rightValueOfBucket) / this.totalValue;
    }

    private double processLessThan(int v) {
        if (v <= this.min) {
            return 0;
        }
        if (v > this.max) {
            return 1.0;
        }
        double totalTargetValue = 0;
        int targetBucket = getTargetBucket(v);
        for (int i = 0; i < targetBucket; i++) {
            totalTargetValue += this.histogram[i];
        }
        double leftValueOfBucket = 1.0 * this.histogram[targetBucket] * ((v - min) % this.bucketSize) / this.bucketSize;
        return (totalTargetValue + leftValueOfBucket) / this.totalValue;
    }

    private double processLessThanOrEqual(int v) {
        if (v < this.min) {
            return 0;
        }
        if (v >= this.max) {
            return 1.0;
        }
        return processLessThan(v) + processEquals(v);
    }

    private double processGreaterThanOrEqual(int v) {
        if (v <= this.min) {
            return 1.0;
        }
        if (v > this.max) {
            return 0;
        }
        return processGreaterThan(v) + processEquals(v);
    }

    private double processNotEquals(int v) {
        if (v < this.min || v > this.max) {
            return 1.0;
        }
//        System.out.println("processNotEquals: v: " + v + " " + this.toString());
//        System.out.println("target bucket: " + getTargetBucket(v)  +" target bucket height " + this.histogram[getTargetBucket(v)]);
        return 1.0 - 1.0 * this.histogram[getTargetBucket(v)] / this.totalValue;

    }


    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return " IntHistogram min " + this.min + " max " + this.max + " buckets " + this.buckets + " bucket size " + this.bucketSize + " total " + this.totalValue + " histogram " + Arrays.toString(this.histogram);
    }
}
