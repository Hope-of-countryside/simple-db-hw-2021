package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  private int gbfield;
  private Type gbfieldtype;
  private int afield;
  private Op what;
  private TupleDesc td;
  // no group
  private ArrayList<Tuple> resultTuples;

  private int avgCurrentSum = 0;
  private int avgCurrentTupleNum = 0;
  private int sumCurrentSum = 0;
  private int countCurrentTupleNum = 0;
  // with group
  private HashMap<Field, Tuple> groupedAggregateResult;
  private HashMap<Field, Integer> avgGroupCurrentSum;
  private HashMap<Field, Integer> avgGroupCurrentTupleNum;

  /**
   * Aggregate constructor
   *
   * @param gbfield     the 0-based index of the group-by field in the tuple, or
   *                    NO_GROUPING if there is no grouping
   * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
   *                    if there is no grouping
   * @param afield      the 0-based index of the aggregate field in the tuple
   * @param what        the aggregation operator
   */

  public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    // some code goes here
    this.gbfield = gbfield;
    this.gbfieldtype = gbfieldtype;
    this.afield = afield;
    this.what = what;
    if (gbfield == NO_GROUPING) {
      td = new TupleDesc(new Type[] {Type.INT_TYPE});
      resultTuples = new ArrayList<>();
    } else {
      td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
      groupedAggregateResult = new HashMap<>();
      avgGroupCurrentSum = new HashMap<>();
      avgGroupCurrentTupleNum = new HashMap<>();
    }
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the
   * constructor
   *
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  public void mergeTupleIntoGroup(Tuple tup) {
    // some code goes here
    if (gbfield == NO_GROUPING) {
      // no group
      switch (what) {
        case MIN:
          minWithoutGrouping(tup);
          break;
        case MAX:
          maxWithoutGrouping(tup);
          break;
        case AVG:
          avgWithoutGrouping(tup);
          break;
        case SUM:
          sumWithoutGrouping(tup);
          break;
        case COUNT:
          countWithoutGrouping(tup);
          break;
      }
    } else {
      // with group
      switch (what) {
        case MIN:
          minWithGrouping(tup);
          break;
        case MAX:
          maxWithGrouping(tup);
          break;
        case AVG:
          avgWithGrouping(tup);
          break;
        case SUM:
          sumWithGrouping(tup);
          break;
        case COUNT:
          countWithGrouping(tup);
          break;
      }
    }
  }

  /**
   * Create a OpIterator over group aggregate results.
   *
   * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
   * if using group, or a single (aggregateVal) if no grouping. The
   * aggregateVal is determined by the type of aggregate specified in
   * the constructor.
   */
  public OpIterator iterator() {
    // some code goes here
    if (gbfield == NO_GROUPING) {
      return new TupleIterator(td, resultTuples);
    }
    return new TupleIterator(td, groupedAggregateResult.values());
  }

  private void minWithoutGrouping(Tuple tup) {
    if (resultTuples.isEmpty()) {
      Tuple min = new Tuple(td);
      min.setField(0, tup.getField(afield));
      resultTuples.add(min);
    } else if (tup.getField(afield)
        .compare(Predicate.Op.LESS_THAN, resultTuples.get(0).getField(afield))) {
      resultTuples.get(0).setField(0, tup.getField(afield));
    }
  }

  private void maxWithoutGrouping(Tuple tup) {
    if (resultTuples.isEmpty()) {
      Tuple max = new Tuple(td);
      max.setField(0, tup.getField(afield));
      resultTuples.add(max);
    } else if (tup.getField(afield)
        .compare(Predicate.Op.GREATER_THAN, resultTuples.get(0).getField(afield))) {
      resultTuples.get(0).setField(0, tup.getField(afield));
    }
  }

  private void avgWithoutGrouping(Tuple tup) {
    avgCurrentSum += ((IntField) tup.getField(afield)).getValue();
    avgCurrentTupleNum++;
    Tuple avg = new Tuple(td);
    avg.setField(0, new IntField(avgCurrentSum / avgCurrentTupleNum));
    if (resultTuples.isEmpty()) {
      resultTuples.add(avg);
    } else {
      resultTuples.set(0, avg);
    }
  }

  private void sumWithoutGrouping(Tuple tup) {
    sumCurrentSum += ((IntField) tup.getField(afield)).getValue();
    Tuple sum = new Tuple(td);
    sum.setField(0, new IntField(sumCurrentSum));
    if (resultTuples.isEmpty()) {
      resultTuples.add(sum);
    } else {
      resultTuples.set(0, sum);
    }
  }

  private void countWithoutGrouping(Tuple tup) {
    countCurrentTupleNum++;
    Tuple count = new Tuple(td);
    count.setField(0, new IntField(countCurrentTupleNum));
    if (resultTuples.isEmpty()) {
      resultTuples.add(count);
    } else {
      resultTuples.set(0, count);
    }
  }

  private void minWithGrouping(Tuple tup) {
    Field groupValue = tup.getField(this.gbfield);
    Field aggregateValue = tup.getField(this.afield);
    if (!groupedAggregateResult.containsKey(groupValue)) {
      Tuple min = new Tuple(this.td);
      min.setField(0, groupValue);
      min.setField(1, aggregateValue);
      groupedAggregateResult.put(groupValue, min);
    } else if (aggregateValue.compare(Predicate.Op.LESS_THAN,
        groupedAggregateResult.get(groupValue).getField(1))) {
      groupedAggregateResult.get(groupValue).setField(1, aggregateValue);
    }
  }

  private void maxWithGrouping(Tuple tup) {
    Field groupValue = tup.getField(this.gbfield);
    Field aggregateValue = tup.getField(this.afield);
    if (!groupedAggregateResult.containsKey(groupValue)) {
      Tuple max = new Tuple(this.td);
      max.setField(0, groupValue);
      max.setField(1, aggregateValue);
      groupedAggregateResult.put(groupValue, max);
    } else if (aggregateValue.compare(Predicate.Op.GREATER_THAN,
        groupedAggregateResult.get(groupValue).getField(1))) {
      groupedAggregateResult.get(groupValue).setField(1, aggregateValue);
    }
  }

  private void avgWithGrouping(Tuple tup) {
    Field groupValue = tup.getField(this.gbfield);
    Field aggregateValue = tup.getField(this.afield);
    avgGroupCurrentTupleNum.putIfAbsent(groupValue, 0);
    avgGroupCurrentSum.putIfAbsent(groupValue, 0);
    avgGroupCurrentTupleNum.put(groupValue, avgGroupCurrentTupleNum.get(groupValue) + 1);
    avgGroupCurrentSum.put(groupValue,
        avgGroupCurrentSum.get(groupValue) + ((IntField) aggregateValue).getValue());
    if (!groupedAggregateResult.containsKey(groupValue)) {
      Tuple avg = new Tuple(td);
      avg.setField(0, groupValue);
      avg.setField(1, aggregateValue);
      groupedAggregateResult.put(groupValue, avg);
      return;
    }
    groupedAggregateResult.get(groupValue)
        .setField(1, new IntField(
            avgGroupCurrentSum.get(groupValue) / avgGroupCurrentTupleNum.get(groupValue)));
  }

  private void sumWithGrouping(Tuple tup) {
    Field groupValue = tup.getField(this.gbfield);
    Field aggregateValue = tup.getField(this.afield);
    if (!groupedAggregateResult.containsKey(groupValue)) {
      Tuple sum = new Tuple(td);
      sum.setField(0, groupValue);
      sum.setField(1, aggregateValue);
      groupedAggregateResult.put(groupValue, sum);
      return;
    }
    groupedAggregateResult.get(groupValue)
        .setField(1, new IntField(
            ((IntField) groupedAggregateResult.get(groupValue).getField(1)).getValue()
                + ((IntField) aggregateValue).getValue()));
  }

  private void countWithGrouping(Tuple tup) {
    Field groupValue = tup.getField(this.gbfield);
    if (!groupedAggregateResult.containsKey(groupValue)) {
      Tuple count = new Tuple(td);
      count.setField(0, groupValue);
      count.setField(1, new IntField(1));
      groupedAggregateResult.put(groupValue, count);
      return;
    }
    groupedAggregateResult.get(groupValue)
        .setField(1, new IntField(
            ((IntField) groupedAggregateResult.get(groupValue).getField(1)).getValue() + 1));
  }

  public TupleDesc getTupleDesc() {
    return td;
  }
}
