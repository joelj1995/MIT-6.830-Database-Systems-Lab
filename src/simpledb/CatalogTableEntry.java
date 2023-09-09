package simpledb;

/*
 * Joel: Value object for a table entry in the catalog
 */
public class CatalogTableEntry {
    
    public CatalogTableEntry(DbFile file, String name, String pkeyField) {
        this.file = file;
        this.name = name;
        this.pKeyField = pkeyField;
    }

    public DbFile getFile() { return file; }

    public String getName() { return name; }

    public String getPKeyField() { return pKeyField; }

    public TupleDesc getTupleDesc() { return file.getTupleDesc(); }

    private final DbFile file;
    private final String name;
    private final String pKeyField;
}
