package simpledb.execution.AggregatorOperator;

public class CountOperator implements AggregatorOperator{
    int count;
    @Override
    public void insert(int val) {
        count++;
    }

    @Override
    public int get() {
        return count;
    }
}
