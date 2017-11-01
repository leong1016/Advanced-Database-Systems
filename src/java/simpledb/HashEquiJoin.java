package simpledb;

import java.lang.reflect.Array;
import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {
    
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private HashMap<Field, ArrayList<Tuple>> hash1 = new HashMap<>();
    private HashMap<Field, ArrayList<Tuple>> hash2 = new HashMap<>();
    private ArrayList<Tuple> result = new ArrayList<>();
    private TupleIterator iterator;
    
    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
        while (child1.hasNext()) {
            Tuple tuple = child1.next();
            Field field = tuple.getField(p.getField1());
            if (hash1.containsKey(field)) {
                ArrayList<Tuple> list = hash1.get(field);
                list.add(tuple);
                hash1.put(field, list);
            } else {
                ArrayList<Tuple> list = new ArrayList<>();
                list.add(tuple);
                hash1.put(field, list);
            }
        }
        while (child2.hasNext()) {
            Tuple tuple = child2.next();
            Field field = tuple.getField(p.getField2());
            if (hash2.containsKey(field)) {
                ArrayList<Tuple> list = hash2.get(field);
                list.add(tuple);
                hash2.put(field, list);
            } else {
                ArrayList<Tuple> list = new ArrayList<>();
                list.add(tuple);
                hash2.put(field, list);
            }
        }
        if (hash1.size() < hash2.size()) {
            for (Field field : hash1.keySet()) {
                if (!hash2.containsKey(field)) {
                    continue;
                }
                ArrayList<Tuple> list1 = hash1.get(field);
                ArrayList<Tuple> list2 = hash2.get(field);
                for (Tuple tuple1 : list1) {
                    for (Tuple tuple2 : list2) {
                        Field field1 = tuple1.getField(p.getField1());
                        Field field2 = tuple2.getField(p.getField2());
                        if (field1.equals(field2)) {
                            Tuple tuple = mergeTuple(tuple1, tuple2);
                            result.add(tuple);
                        }
                    }
                }
            }
        } else {
            for (Field field : hash2.keySet()) {
                if (!hash1.containsKey(field)) {
                    continue;
                }
                ArrayList<Tuple> list1 = hash1.get(field);
                ArrayList<Tuple> list2 = hash2.get(field);
                for (Tuple tuple1 : list1) {
                    for (Tuple tuple2 : list2) {
                        Field field1 = tuple1.getField(p.getField1());
                        Field field2 = tuple2.getField(p.getField2());
                        if (field1.equals(field2)) {
                            Tuple tuple = mergeTuple(tuple1, tuple2);
                            result.add(tuple);
                        }
                    }
                }
            }
        }
        iterator = new TupleIterator(getTupleDesc(), result);
        iterator.open();
    }

    private Tuple mergeTuple (Tuple t1, Tuple t2) {
        
        int i = 0;
        Tuple t = new Tuple(getTupleDesc());
        Iterator<Field> iterator1 = t1.fields();
        while (iterator1.hasNext()) {
            t.setField(i++, iterator1.next());
        }
        Iterator<Field> iterator2 = t2.fields();
        while (iterator2.hasNext()) {
            t.setField(i++, iterator2.next());
        }
        return t;
    }
    
    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
        iterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        iterator.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] {child1, child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }
    
}
