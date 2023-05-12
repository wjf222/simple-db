package simpledb.execution.AggregatorOperator;

public class MaxOperator implements AggregatorOperator{
    Integer max = null;
    @Override
    public void insert(int val) {
        if(max == null) max = val;
        else max = max > val ? max:val;
    }

    @Override
    public int get() {
        return max;
    }
}
