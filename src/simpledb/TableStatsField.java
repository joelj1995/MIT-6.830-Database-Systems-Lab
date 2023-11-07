package simpledb;

public class TableStatsField {

    public TableStatsField(Type type) {
        this.type = type;
    }

    public void processValueForRange(Field v) {
        if (type == Type.INT_TYPE) {
            var i = (IntField)v;
            var intVal = i.getValue();
            min = Math.min(min, intVal);
            max = Math.max(max, intVal);
        }
    }

    public void commitRange(int numBuckets) {
        if (type == Type.INT_TYPE) {
            intHist = new IntHistogram(numBuckets, min, max);
        }
        else {
            strHist = new StringHistogram(numBuckets);
        }
    }

    public void processValueForStats(Field v) {
        if (type == Type.INT_TYPE) {
            intHist.addValue(((IntField)v).getValue());
        }
        else {
            strHist.addValue(((StringField)v).getValue());
        }
    }

    public double estimateSelectivity(Predicate.Op op, Field constant) {
        if (type == Type.INT_TYPE) {
            return intHist.estimateSelectivity(op, ((IntField)constant).getValue());
        }
        else {
            return strHist.estimateSelectivity(op, ((StringField)constant).getValue());
        }
    } 

    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;

    private IntHistogram intHist;
    private StringHistogram strHist;
    
    private final Type type;
}
