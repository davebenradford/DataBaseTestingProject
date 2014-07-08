package DbTest;
/**
 * IdPercentPair Class
 * 
 * @author radfordd
 */
class IdPercentPair {
    private int id;
    private double percent;
    public IdPercentPair() {
        id = 0;
        percent = 0.0;
    }
    public IdPercentPair(int i, double p) {
        id = i;
        percent = p;
    }
    public void setNameType(int i, double p) {
        id = i;
        percent = p;
    }
    public int getPairId() {
        return id;
    }
    public double getPairPercent() {
        return percent;
    }
    public IdPercentPair getPair() {
        return this;
    }
}
