package DbTest;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ValueTypePair Class
 * 
 * @author radfordd
 * 
 * ValueTypePair consists of a value as a Double type and a Data Type for the 
 * value associated with an Int value.
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
