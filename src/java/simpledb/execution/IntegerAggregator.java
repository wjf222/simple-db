package simpledb.execution;

import net.sf.antcontrib.logic.Switch;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.AggregatorOperator.AggOperatorFactory;
import simpledb.execution.AggregatorOperator.AggregatorOperator;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final int afield;
    private final Op what;
    private final Map<Field, AggregatorOperator> map;
    private final TupleDesc td;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.what = what;
        this.map = new HashMap<>();
        if(this.gbfield == NO_GROUPING){
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        AggregatorOperator aggregatorOperator;
        if(this.gbfield == NO_GROUPING){
            aggregatorOperator = map.getOrDefault(null, AggOperatorFactory.createOperator(what));
        }else {
            aggregatorOperator = map.getOrDefault(tup.getField(gbfield),AggOperatorFactory.createOperator(what));
        }
        Field f = tup.getField(afield);
        int value = Integer.parseInt(f.toString());
        aggregatorOperator.insert(value);
        if(this.gbfield == NO_GROUPING){
            map.put(null,aggregatorOperator);
        }else {
            map.put(tup.getField(gbfield),aggregatorOperator);
        }
    }
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private Iterator<Field> it;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.it = map.keySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return this.it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Field f = this.it.next();
                Tuple tuple = new Tuple(td);
                if(gbfield == NO_GROUPING){
                    tuple.setField(0,new IntField(map.get(f).get()));
                    return tuple;
                }
                tuple.setField(0,f);
                tuple.setField(1,new IntField(map.get(f).get()));
                return tuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.it = map.keySet().iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                this.it = null;
            }
        };
    }

}
