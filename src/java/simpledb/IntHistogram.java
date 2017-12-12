package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int total;
    private int buck_size;
    
    private int[] count;
    
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	    // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        count = new int[buckets+1];
        this.total = 0;
        this.buck_size = (int) Math.ceil((max - min + 1) / (double) buckets);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = (v - min) / buck_size;
        count[index]++;
        total++;
    }
    
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	    // some code goes here
        
        if(v < min) {
            if(op.equals(Predicate.Op.EQUALS)) {
                return 0.0;
            }
            if(op.equals(Predicate.Op.NOT_EQUALS)) {
                return 1.0;
            }
            if(op.equals(Predicate.Op.GREATER_THAN)) {
                return 1.0;      
            }
            if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
                return 1.0; 
            }
            if(op.equals(Predicate.Op.LESS_THAN)) {
                return 0.0;
            }
            if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
                return 0.0;
            }
        }
        
        if(v > max) {
            if(op.equals(Predicate.Op.EQUALS)) {
                return 0.0;
            }
            if(op.equals(Predicate.Op.NOT_EQUALS)) {
                return 1.0;
            }
            if(op.equals(Predicate.Op.GREATER_THAN)) {
                return 0.0;      
            }
            if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
                return 0.0; 
            }
            if(op.equals(Predicate.Op.LESS_THAN)) {
                return 1.0;
            }
            if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
                return 1.0;
            }
        }
        
        int index = (v - min) / buck_size;
        double ans = (count[index] / (double) (buck_size));
        if(op.equals(Predicate.Op.EQUALS)) {
            return ans / total;
        }
        if(op.equals(Predicate.Op.NOT_EQUALS)) {
            return 1 - (ans / total);
        }
        if(op.equals(Predicate.Op.GREATER_THAN)) {
            ans *= buck_size - v + min + (index * buck_size) - 1;
            for(int i = index + 1; i < count.length; i++)
                ans += count[i];
            return ans / total;
        }
        if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            if(v + 1 != min + ((index+1) * buck_size))
                ans *= buck_size - v + min + (index * buck_size);
            for(int i = index + 1; i < count.length; i++)
                ans += count[i];
            return ans / total;
        }
        if(op.equals(Predicate.Op.LESS_THAN)) {
            ans *= v - min - (index*buck_size);
            for(int i = 0; i < index; i++)
                ans += count[i];
            return ans / total;
        }
        if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            if(v != min + (index * buck_size))
                ans *= v - min - (index*buck_size) + 1;
            for(int i = 0; i < index; i++)
                ans += count[i];
            return ans / total;
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String s = "";
        for(int i = 0; i < this.count.length; i++) {
            s += i + ": " + count[i] + "\n";
        }
        s += "total: " + total + "\n";
        s += "bucket_size: " + buck_size + "\n";
        s += "buckets: " + buckets + "\n";
        return s;
    }
}
