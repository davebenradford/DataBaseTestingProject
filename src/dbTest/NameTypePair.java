package dbTest;
/**
 * NameTypePair Class
 * 
 * @author radfordd
 */
class NameTypePair {
    private String name;
    private int type;
    public NameTypePair() {
        name = "";
        type = 0;
    }
    public NameTypePair(String n, int t) {
        name = n;
        type = t;
    }
    public void setNameType(String n, int t) {
        name = n;
        type = t;
    }
    public String getPairName() {
        return name;
    }
    public int getPairType() {
        return type;
    }
    public NameTypePair getPair() {
        return this;
    }
}
