package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import simpledb.common.Type;
import simpledb.storage.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private RecordId rid;
    private ArrayList<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *           the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.td = td;
        this.fields = new ArrayList<>();
        for (TDItem t : td.getTupleItems()) {
            if (t.fieldType == Type.INT_TYPE) {
                this.fields.add(new IntField(0));
            } else if (t.fieldType == Type.STRING_TYPE) {
                this.fields.add(new StringField("", 0));
            }
        }
    }

    /**
     * merge tuples into one tuple
     * @param tps
     */
    public Tuple(Tuple... tps) {
        TupleDesc[] descs = new TupleDesc[tps.length];
        fields = new ArrayList<>();
        for(int i = 0; i < tps.length; i++) {
            fields.addAll(tps[i].fields);
            descs[i] = tps[i].getTupleDesc();
        }
        td = new TupleDesc(descs);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *          index of the field to change. It must be a valid index.
     * @param f
     *          new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        this.fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *          field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return this.fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (Field f : this.fields) {
            sb.append(f.toString()).append("\t");
        }
        return sb.toString().substring(0, sb.length() - 1);
        // throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *         An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        return this.fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        this.td = td;
        for (TDItem t : td.getTupleItems()) {
            if (t.fieldType == Type.INT_TYPE) {
                this.fields.add(new IntField(0));
            } else if (t.fieldType == Type.STRING_TYPE) {
                this.fields.add(new StringField("", 0));
            }
        }
    }

}
