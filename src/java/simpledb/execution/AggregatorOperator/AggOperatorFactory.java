package simpledb.execution.AggregatorOperator;

import simpledb.execution.Aggregator;

public class AggOperatorFactory {
    public static AggregatorOperator createOperator(Aggregator.Op op){
        AggregatorOperator opera = null;
        switch (op){
            case MIN:
                opera = new MinOperator();
                break;
            case MAX:
                opera = new MaxOperator();
                break;
            case SUM:
                opera = new SumOperator();
                break;
            case COUNT:
                opera = new CountOperator();
                break;
            case AVG:
                opera = new AvgOperator();
                break;
            default:
                throw new UnsupportedOperationException("不支持该操作");
        }
        return opera;
    }
}
