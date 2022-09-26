package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || obj.getClass() != getClass())
                return false;
            TDItem desc = (TDItem) obj;
            return this.fieldName == desc.fieldName && this.fieldType == desc.fieldType;
        }
    }

    private static final long serialVersionUID = 1L;
    private ArrayList<TDItem> tupleDescItems;
    private int tupleDescSize;

    /**
     * @return
     *         An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return tupleDescItems.iterator();
    }

    public ArrayList<TDItem> getTupleItems() {
        return tupleDescItems;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *                array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *                array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        tupleDescItems = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            tupleDescItems.add(new TDItem(typeAr[i], fieldAr[i]));
            tupleDescSize += typeAr[i].getLen();
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *               array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tupleDescItems = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            tupleDescItems.add(new TDItem(typeAr[i], null));
            tupleDescSize += typeAr[i].getLen();
        }

    }

    /**
     * variable-length arguments Constructor.
     */
    public TupleDesc(TupleDesc... descs) {
        tupleDescItems = new ArrayList<>();
        for (TupleDesc desc : descs) {
            this.tupleDescSize += desc.getSize();
            this.tupleDescItems.addAll(desc.getTupleItems());
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tupleDescItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *          index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= tupleDescItems.size()) {
            throw new NoSuchElementException();
        }
        return tupleDescItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *          The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= tupleDescItems.size()) {
            throw new NoSuchElementException();
        }
        return tupleDescItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *             name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *                                if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < tupleDescItems.size(); i++) {
            String currName = tupleDescItems.get(i).fieldName;
            if (currName != null && currName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return tupleDescSize;
    }

    public int getItemSize() {
        return tupleDescItems.size();
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        return new TupleDesc(td1, td2);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *          the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TupleDesc desc = (TupleDesc) o;
        if (getItemSize() != desc.getItemSize()) {
            return false;
        }
        for (int i = 0; i < getItemSize(); i++) {
            if (getTupleItems().get(i).fieldType != desc.getTupleItems().get(i).fieldType) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int h = 0;
        for (TDItem desc : tupleDescItems) {
            h += desc.fieldType.ordinal();
        }
        return h;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        if (getItemSize() == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for(TDItem item : tupleDescItems) {
            sb.append(item.fieldType).append("(").append(item.fieldName).append("), ");
        }
        return sb.toString().substring(0, sb.length() - 2);
    }
}
