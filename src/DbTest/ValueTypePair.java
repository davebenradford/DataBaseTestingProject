package DbTest;

/**
 * ValueTypePair Class
 * 
 * @author radfordd
 * 
 * ValueTypePair consists of a value as a Double type and a Data Type for the 
 * value associated with an Int value. 0 represents a Double primitive type and 
 * a 1 represents an Integer primitive type.
 */
class ValueTypePair {
    private double value;
    private int type;
    public ValueTypePair() {
        value = 0.0;
        type = -1;
    }
    public ValueTypePair(double v, int t) {
        value = v;
        if(t == 0) {
            type = 0;
        }
        else {
            type = 1;
        }
    }
    public void setValueType(double v, int t) {
        value = v;
        if(t == 0) {
            type = 0;
        }
        else {
            type = 1;
        }
    }
    public int getPairValueAsInt() {
        if(type == 1) {
            return (int) value;
        }
        else {
            return -1;
        }
    }
    public double getPairValueAsDouble() {
        return value;
    }
    public int getPairType() {
        return type;
    }
    public ValueTypePair getPair() {
        return this;
    }
}
