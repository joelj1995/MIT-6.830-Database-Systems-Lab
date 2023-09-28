package simpledb;

public enum LockMode {
    NL() {},
    IS() {},
    IX() {},
    S(),
    SIX(),
    X()
}
