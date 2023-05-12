package simpledb.execution.AggregatorOperator;

public class AvgOperator implements AggregatorOperator{
    int sum;
    int count;
    @Override
    public void insert(int val) {
        sum +=val;
        count++;
    }

    @Override
    public int get() {
        return sum/count;
    }
}
