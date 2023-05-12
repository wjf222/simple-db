package simpledb.execution.AggregatorOperator;

public class SumOperator implements AggregatorOperator{
    int sum;
    @Override
    public void insert(int val) {
        sum += val;
    }

    @Override
    public int get() {
        return sum;
    }
}
