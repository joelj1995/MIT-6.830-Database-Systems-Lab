package simpledb;

public class DbConfig {

    public DbConfig(boolean steal, boolean force) {
        this.steal = steal;
        this.force = force;
    }
    
    public boolean steal() { return steal; }
    public boolean force() { return force; }
    public static Integer lockWaitMs = 10;
    public static Integer maxLockWaitMs = 300;

    private final boolean steal;
    private final  boolean force;
}
