package simpledb.execution.AggregatorOperator;

public class MinOperator implements AggregatorOperator{
    Integer min = null;
    @Override
    public void insert(int val) {
        if(min == null) min = val;
        else min = min > val ? val:min;
    }

    @Override
    public int get() {
        return min;
    }
}
