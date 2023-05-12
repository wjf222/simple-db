package simpledb.execution.AggregatorOperator;

// 隐藏聚合操作提底层的具体实现
public interface AggregatorOperator {
    void insert(int val);
    int get();
}
