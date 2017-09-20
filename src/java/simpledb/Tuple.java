package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.omg.PortableServer.THREAD_POLICY_ID;

import com.sun.jndi.url.iiopname.iiopnameURLContextFactory;

import simpledb.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private TupleDesc td;
    private RecordId rid;
    private List<Field> fields = new ArrayList<>();
    
    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.td = td;
        Iterator<TDItem> iterator = td.iterator();
        while (iterator.hasNext()) {
            TDItem item = iterator.next();
            if (item.fieldType.equals(Type.INT_TYPE)) {
                fields.add(new IntField(0));
            } else if (item.fieldType.equals(Type.STRING_TYPE)) {
                fields.add(new StringField(null, 256));
            }
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
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
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i >= 0 && i < fields.size() && fields.get(i).getType().equals(f.getType())) {
            fields.set(i, f);            
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i >= 0 && i < fields.size()) {
            return fields.get(i);
        } else {
            throw new IllegalArgumentException();
        }
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
        String result = fields.get(0).toString();
        for (int i = 1; i < fields.size(); i++) {
            result += "\t" + fields.get(i).toString();
        }
        return result;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new TupleIterator();
    }
    
    private class TupleIterator implements Iterator<Field> {

        int i = 0;
        int n = fields.size();
        
        @Override
        public boolean hasNext() {
            return i < n;
        }

        @Override
        public Field next() {
            return fields.get(i++);
        }
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.td = td;
        fields.clear();
        Iterator<TDItem> iterator = td.iterator();
        while (iterator.hasNext()) {
            TDItem item = iterator.next();
            if (item.fieldType.equals(Type.INT_TYPE)) {
                fields.add(new IntField(0));
            } else if (item.fieldType.equals(Type.STRING_TYPE)) {
                fields.add(new StringField(null, 256));
            }
        }
    }
}
