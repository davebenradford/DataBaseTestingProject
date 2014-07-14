package DbTest;
/**
 * IdValuePair Class
 * 
 * @author radfordd
 */
class IdValuePair {
    private int id;
    private double value;
    public IdValuePair() {
        id = 0;
        value = 0.0;
    }
    public IdValuePair(int i, double p) {
        id = i;
        value = p;
    }
    public void setIdValue(int i, double p) {
        id = i;
        value = p;
    }
    public int getPairId() {
        return id;
    }
    public double getPairValue() {
        return value;
    }
    public IdValuePair getPair() {
        return this;
    }
}
