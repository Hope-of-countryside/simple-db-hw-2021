package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableid;
    private DbFile file;
    private int ioCostPerPage;
    private HashMap<Integer, IntHistogram> intHistogramHashMap;
    private HashMap<Integer, ArrayList<Integer>> minMaxHashMap;    // size of list is 2, arr[0] represent min value, arr[1] represent max value

    private HashMap<Integer, StringHistogram> stringHistogramHashMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        intHistogramHashMap = new HashMap<>();
        stringHistogramHashMap = new HashMap<>();
        minMaxHashMap = new HashMap<>();
        for (int i = 0; i < this.file.getTupleDesc().getItemSize(); i++) {
            TupleDesc.TDItem item = this.file.getTupleDesc().getTupleItems().get(i);
            if (item.fieldType.equals(Type.STRING_TYPE)) {
                insertStringHistogram(i);
            } else if (item.fieldType.equals(Type.INT_TYPE)) {
                insertMinMaxHashMap(i);
            } else {
                throw new RuntimeException("wrong filed type");
            }
        }
        processMinMax();
        insertIntHistogram();
        processValue();
    }

    private void processMinMax() {
        DbFileIterator iterator = this.file.iterator(null);
        try {
            iterator.open();
            while (iterator.hasNext()) {
                Tuple t = iterator.next();
                for (Map.Entry<Integer, ArrayList<Integer>> entry : minMaxHashMap.entrySet()) {
                    int currentFiledValue = ((IntField) t.getField(entry.getKey())).getValue();
                    entry.getValue().set(0, Math.min(entry.getValue().get(0), currentFiledValue));//min
                    entry.getValue().set(1, Math.max(entry.getValue().get(1), currentFiledValue));//max
                }
            }
            iterator.close();
        } catch (DbException | TransactionAbortedException e) {
            throw new RuntimeException("process min max failed");
        }

    }

    private void processValue() {
        DbFileIterator iterator = this.file.iterator(null);
        try {
            iterator.open();
            while (iterator.hasNext()) {
                Tuple t = iterator.next();
                for (Map.Entry<Integer, IntHistogram> entry : intHistogramHashMap.entrySet()) {
                    entry.getValue().addValue(((IntField) t.getField(entry.getKey())).getValue());
                }
                for (Map.Entry<Integer, StringHistogram> entry : stringHistogramHashMap.entrySet()) {
                    entry.getValue().addValue(((StringField) t.getField(entry.getKey())).getValue());
                }
            }
            iterator.close();
        } catch (DbException | TransactionAbortedException e) {
            throw new RuntimeException("process min max failed");
        }

    }

    private void insertMinMaxHashMap(int i) {
        minMaxHashMap.put(i, new ArrayList<Integer>() {{
            add(Integer.MAX_VALUE);
            add(Integer.MIN_VALUE);
        }});
    }

    private void insertIntHistogram() {
        for (Map.Entry<Integer, ArrayList<Integer>> entry : minMaxHashMap.entrySet()) {
            IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, entry.getValue().get(0), entry.getValue().get(1));
            this.intHistogramHashMap.put(entry.getKey(), intHistogram);
        }
    }

    private void insertStringHistogram(int i) {
        StringHistogram s = new StringHistogram(NUM_HIST_BINS);
        this.stringHistogramHashMap.put(i, s);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.file.numPages() * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * totalTuples());
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (constant.getType().equals(Type.STRING_TYPE)) {
            StringHistogram s = stringHistogramHashMap.get(field);
            StringField stringField = (StringField) constant;
            return s.estimateSelectivity(op, stringField.getValue());
        } else if (constant.getType().equals(Type.INT_TYPE)) {
            IntHistogram s = intHistogramHashMap.get(field);
            IntField intField = (IntField) constant;
            return s.estimateSelectivity(op, intField.getValue());
        } else {
            throw new RuntimeException("wrong type");
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        DbFileIterator iterator = this.file.iterator(null);
        int total = 0;
        try {
            iterator.open();
            while (iterator.hasNext()) {
                iterator.next();
                total++;
            }
            iterator.close();
        } catch (TransactionAbortedException | DbException e) {
            throw new RuntimeException(e);
        }
        return total;
    }

}
