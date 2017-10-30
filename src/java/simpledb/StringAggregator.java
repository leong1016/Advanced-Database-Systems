package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> count;
    private String gbfieldname = null;
    
    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT)
            throw new IllegalArgumentException();
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        count = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfieldname == null)
            gbfieldname = tup.getTupleDesc().getFieldName(gbfield);
        
        Field gb = null;
        if (gbfield != Aggregator.NO_GROUPING) {
            gb = tup.getField(gbfield);
        }
        
        if (count.containsKey(gb)) {
            count.put(gb, count.get(gb) + 1);
        } else {
            count.put(gb, 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        if (gbfield == Aggregator.NO_GROUPING) {
            Type[] types = new Type[] {Type.INT_TYPE};
            String[] strings = new String[] {what.toString()};
            TupleDesc td = new TupleDesc(types, strings); 
            ArrayList<Tuple> tuples = new ArrayList<>();
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(count.get(null)));
            tuples.add(tuple);
            return new TupleIterator(td, tuples);
        } else {
            Type[] types = new Type[] {gbfieldtype, Type.INT_TYPE};
            String[] strings = new String[] {gbfieldname, what.toString()};
            TupleDesc td = new TupleDesc(types, strings); 
            ArrayList<Tuple> tuples = new ArrayList<>();
            for (Field field : count.keySet()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, field);
                tuple.setField(1, new IntField(count.get(field)));
                tuples.add(tuple);
            }
            return new TupleIterator(td, tuples);
        }
    }

}
