package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        int combinedLength = td1.numFields() + td2.numFields();
        Type[] combinedTypeAr = new Type[combinedLength];
        String[] combinedFieldAr = new String[combinedLength];
        for (int i = 0; i < combinedLength; i++) {
            if (i < td1.numFields()) {
                combinedTypeAr[i] = td1.getType(i);
                combinedFieldAr[i] = td1.getFieldName(i);
            }
            else {
                combinedTypeAr[i] = td2.getType(i - td1.numFields());
                combinedFieldAr[i] = td2.getFieldName(i - td1.numFields());
            }
        }
        return new TupleDesc(combinedTypeAr, combinedFieldAr);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("Length of types does not match length of field names.");
        }
        this.typeAr = typeAr;
        this.fieldAr = fieldAr;
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.typeAr = typeAr;
        this.fieldAr = new String[typeAr.length];
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return typeAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return fieldAr[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        int i = 0;
        for (String field : fieldAr) {
            if (field == null) continue;
            if (field.equals(name)) {
                return i;
            }
            i++;
        }
        throw new NoSuchElementException(name);
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        return typeAr[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int result = 0;
        for (Type type : typeAr) {
            result += type.getLen();
        }
        return result;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o instanceof TupleDesc)) return false;
        TupleDesc td = (TupleDesc)o;
        if (td.numFields() != this.numFields()) return false;
        for (int i = 0; i < this.numFields(); i++) {
            if (this.getType(i) != td.getType(i)) return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        String result = "";
        for (int i = 0; i < this.numFields(); i++) {
            switch (this.getType(i)) {
                case INT_TYPE:
                    result += "INT";
                    break;
                case STRING_TYPE:
                    result += "STRING";
                    break;
            }
            String name = this.getFieldName(i);
            if (name != null) {
                result += "(" + name + ")";
            }
            else {
                result += "()";
            }
            if (i < this.numFields() - 1) {
                result += ", ";
            }
        }
        return result;
    }

    /**
     * Joel: Clone the current TupleDesc but prefix the field names
     */
    public TupleDesc withPrefix(String prefix) {
        String[] prefixedFieldAr = new String[numFields()];
        for (int i = 0; i < numFields(); i++) {
            prefixedFieldAr[i] = prefix + "." + fieldAr[i];
        }
        return new TupleDesc(this.typeAr, prefixedFieldAr);
    }

    private Type[] typeAr; 
    private String[] fieldAr;
}
