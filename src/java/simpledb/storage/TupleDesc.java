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
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    public class TupleItr implements Iterator<TDItem> {
        int cursor = 0; // index of next element to return
        @Override
        public boolean hasNext() {
            return cursor != tdItems.length;
        }

        @Override
        public TDItem next() {
            int i = cursor;
            if(i >= tdItems.length) {
                throw new NoSuchElementException();
            }
            cursor = i+1;
            return tdItems[i];
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TupleItr();
    }

    private static final long serialVersionUID = 1L;
    private static final String UNNAMED = "unnamed";
    private TDItem[] tdItems;
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        int l1 = typeAr.length;
        int l2 = fieldAr.length;
        if(l1 != l2) {
            throw new IllegalArgumentException("参数长度不一致");
        }
        tdItems = new TDItem[l1];
        for(int i = 0; i < tdItems.length;i++) {
            tdItems[i] = new TDItem(typeAr[i],fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        int l1 = typeAr.length;
        this.tdItems = new TDItem[l1];
        for(int i = 0; i < tdItems.length;i++) {
            tdItems[i] = new TDItem(typeAr[i],UNNAMED);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i > tdItems.length) {
            throw new NoSuchElementException();
        }
        return this.tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i > tdItems.length) {
            throw new NoSuchElementException();
        }
        return this.tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i = 0 ;i < this.tdItems.length;i++) {
            if(tdItems[i].fieldName.equals(name)){
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
        int size = 0;
        for(int i = 0; i < tdItems.length;i++){
            size += tdItems[i].fieldType.getLen();
        }
        return size;
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
        int l1 = td1.getLength();
        int l2 = td2.getLength();
        String[] names = new String[l1+l2];
        Type[] types = new Type[l1+l2];
        for(int i = 0; i < l1;i++){
            names[i] = td1.getFieldName(i);
            types[i] = td1.getFieldType(i);
        }
        for(int i = 0; i < l2;i++){
            names[i+l1] = td2.getFieldName(i);
            types[i+l1] = td2.getFieldType(i);
        }
        return new TupleDesc(types,names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(o == null) {
            return false;
        }
        if(this == o){
            return true;
        }
        if(getClass() != o.getClass()) {
            return false;
        }
        TupleDesc t = (TupleDesc) o;
        if(t.tdItems.length != this.tdItems.length){
            return false;
        }
        for(int i = 0 ;i < t.tdItems.length;i++){
            if(t.getFieldType(i) != this.getFieldType(i)){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder ans = new StringBuilder();
        for(int i = 0; i < tdItems.length;i++){
            ans.append(tdItems[i].toString()).append(",");
        }
        ans.deleteCharAt(ans.length()-1);
        return ans.toString();
    }

    public int getLength(){
        return this.tdItems.length;
    }
}
