package simpledb;

import static org.junit.Assert.assertEquals;

import java.util.NoSuchElementException;

import junit.framework.Assert;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

public class CatalogTest extends SimpleDbTestBase {
    private static String name = "test";
	private String nameThisTestRun;
    
    @Before public void addTables() throws Exception {
        Database.getCatalog().clear();
		nameThisTestRun = SystemTestUtil.getUUID();
        Database.getCatalog().addTable(new SkeletonFile(-1, Utility.getTupleDesc(2)), nameThisTestRun);
        Database.getCatalog().addTable(new SkeletonFile(-2, Utility.getTupleDesc(2)), name);
    }

    /**
     * Unit test for Catalog.getTupleDesc()
     */
    @Test public void getTupleDesc() throws Exception {
        TupleDesc expected = Utility.getTupleDesc(2);
        TupleDesc actual = Database.getCatalog().getTupleDesc(-1);

        assertEquals(expected, actual);
    }

    /**
     * Unit test for Catalog.getTableId()
     */
    @Test public void getTableId() {
        assertEquals(-2, Database.getCatalog().getTableId(name));
        assertEquals(-1, Database.getCatalog().getTableId(nameThisTestRun));
        
        try {
            Database.getCatalog().getTableId(null);
            Assert.fail("Should not find table with null name");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
        
        try {
            Database.getCatalog().getTableId("foo");
            Assert.fail("Should not find table with name foo");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
    }

    /**
     * Unit test for Catalog.getDbFile()
     */
    @Test public void getDbFile() throws Exception {
        DbFile f = Database.getCatalog().getDbFile(-1);

        // NOTE(ghuo): we try not to dig too deeply into the DbFile API here; we
        // rely on HeapFileTest for that. perform some basic checks.
        assertEquals(-1, f.getId());
    }

    /*
     * Joel: Unit test for Catalog.clear()
     */
    @Test public void testClear() {
        Database.getCatalog().clear();
        try {
            assertEquals(-2, Database.getCatalog().getTableId(name));
            Assert.fail("Should not find table after clearing the catalog.");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
    }

    /*
     * Joel: Unit test for Catalog.getPrimaryKey();
     */
    @Test public void testGetPrimaryKey() {
        var expectedPrimaryKey = "fookey";
        Database.getCatalog().addTable(new SkeletonFile(-3, Utility.getTupleDesc(2)), "testTable", expectedPrimaryKey);
        assertEquals(expectedPrimaryKey, Database.getCatalog().getPrimaryKey(-3));
    }

    /*
     * Joel: Unit test for Catalog.getTableName()
     */
    @Test public void testGetTableName() {
        assertEquals(nameThisTestRun, Database.getCatalog().getTableName(-1));
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(CatalogTest.class);
    }
}

