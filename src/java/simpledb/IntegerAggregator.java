package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> result;
    private HashMap<Field, Integer> count;
    private String gbfieldname = null;
    
    private static final long serialVersionUID = 1L;

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
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.result = new HashMap<>();
        this.count = new HashMap<>();
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
        if (gbfield != Aggregator.NO_GROUPING && gbfieldname == null)
            gbfieldname = tup.getTupleDesc().getFieldName(gbfield);
        
        Integer value = ((IntField) tup.getField(afield)).getValue();
        Field gb = null;
        if (gbfield != Aggregator.NO_GROUPING) {
            gb = tup.getField(gbfield);
        }

        if (!result.containsKey(gb)) {
            result.put(gb, value);
        } else {
            switch (what) {
            case MIN:
                if (result.get(gb) > value)
                    result.put(gb, value);
                break;
            case MAX:
                if (result.get(gb) < value)
                    result.put(gb, value);
                break;
            case SUM:
            case AVG:
                result.put(gb, result.get(gb) + value);
                break;
            }
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
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        if (gbfield == Aggregator.NO_GROUPING) {
            Type[] types = new Type[] {Type.INT_TYPE};
            String[] strings = new String[] {what.toString()};
            TupleDesc td = new TupleDesc(types, strings); 
            Tuple tuple = new Tuple(td);
            switch (what) {
            case COUNT:
                tuple.setField(0, new IntField(count.get(null)));
                break;
            case AVG:
                tuple.setField(0, new IntField(result.get(null) / count.get(null)));
                break;
            default:
                tuple.setField(0, new IntField(result.get(null)));
                break;
            }
            ArrayList<Tuple> tuples = new ArrayList<>();
            tuples.add(tuple);
            return new TupleIterator(td, tuples);
        } else {
            Type[] types = new Type[] {gbfieldtype, Type.INT_TYPE};
            String[] strings = new String[] {gbfieldname, what.toString()};
            TupleDesc td = new TupleDesc(types, strings); 
            ArrayList<Tuple> tuples = new ArrayList<>();
            for (Field field : count.keySet()) {
                Tuple tuple = new Tuple(td);
                switch (what) {
                case COUNT:
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(count.get(field)));
                    break;
                case AVG:
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(result.get(field) / count.get(field)));
                    break;
                default:
                    tuple.setField(0, field);
                    tuple.setField(1, new IntField(result.get(field)));
                    break;
                }
                tuples.add(tuple);
            }
            return new TupleIterator(td, tuples);
        }
    }

}
