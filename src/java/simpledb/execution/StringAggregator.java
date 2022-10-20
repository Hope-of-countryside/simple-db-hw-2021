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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  private int gbfield;
  private Type gbfieldtype;
  private int afield;
  private Op what;
  private TupleDesc td;
  // no group
  private ArrayList<Tuple> resultTuples;
  private int countCurrentTupleNum = 0;

  // with group
  private HashMap<Field, Tuple> groupedAggregateResult;

  /**
   * Aggregate constructor
   *
   * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
   * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
   * @param afield      the 0-based index of the aggregate field in the tuple
   * @param what        aggregation operator to use -- only supports COUNT
   * @throws IllegalArgumentException if what != COUNT
   */

  public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    // some code goes here
    if (what != Op.COUNT) {
      throw new IllegalArgumentException("what != COUNT");
    }
    this.afield = afield;
    this.gbfield = gbfield;
    this.gbfieldtype = gbfieldtype;
    this.what = what;
    if (gbfield == NO_GROUPING) {
      td = new TupleDesc(new Type[] {Type.INT_TYPE});
      resultTuples = new ArrayList<>();
    } else {
      td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
      groupedAggregateResult = new HashMap<>();
    }
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the constructor
   *
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  public void mergeTupleIntoGroup(Tuple tup) {
    // what only equals to COUNT
    if (gbfield == NO_GROUPING) {
      countWithoutGrouping(tup);
    } else {
      countWithGrouping(tup);
    }
  }

  /**
   * Create a OpIterator over group aggregate results.
   *
   * @return a OpIterator whose tuples are the pair (groupVal,
   * aggregateVal) if using group, or a single (aggregateVal) if no
   * grouping. The aggregateVal is determined by the type of
   * aggregate specified in the constructor.
   */
  public OpIterator iterator() {
    if (gbfield == NO_GROUPING) {
      return new TupleIterator(td, resultTuples);
    }
    return new TupleIterator(td, groupedAggregateResult.values());
  }

  private void countWithoutGrouping(Tuple tup) {
    countCurrentTupleNum++;
    Tuple count = new Tuple(td);
    count.setField(0, new IntField(countCurrentTupleNum));
    resultTuples.set(0, count);
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
