package at.sfischer.synth.db.model;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.List;

/**
 * Represents a column in a database table, including its metadata and references.
 * <p>
 * This class stores information about the column's name, type, constraints such as
 * primary key, uniqueness, and auto-increment, as well as foreign key references
 * to other columns.
 * </p>
 */
public class Column {

    private final Table table;

    private final ColumnDefinition columnDefinition;

    private Column reference;

    private boolean isAutoIncrement;

    private boolean isPrimaryKey;

    private boolean isUnique;

    /**
     * Constructs a new Column instance associated with a given table and
     * its SQL definition.
     *
     * @param table the Table this column belongs to
     * @param columnDefinition the JSQLParser ColumnDefinition object representing
     *                         this column's SQL definition
     */
    public Column(Table table, ColumnDefinition columnDefinition) {
        this.table = table;
        this.columnDefinition = columnDefinition;
        processColumnDefinition();
    }

    private void processColumnDefinition(){
        List<String> specs = this.columnDefinition.getColumnSpecs();
        if(specs == null){
            return;
        }

        for (int i = 0; i < specs.size(); i++) {
            String spec = specs.get(i);
            if (spec.equalsIgnoreCase("AUTO_INCREMENT")) {
                isAutoIncrement = true;
                continue;
            }
            if (spec.equalsIgnoreCase("PRIMARY") && i + 1 < specs.size() && specs.get(i + 1).equalsIgnoreCase("KEY")) {
                isPrimaryKey = true;
                i++;
                continue;
            }
            if (spec.equalsIgnoreCase("UNIQUE")) {
                isUnique = true;
            }
        }
    }

    protected ColumnDefinition getColumnDefinition() {
        return columnDefinition;
    }

    /**
     * Returns the table this column belongs to.
     *
     * @return the parent Table
     */
    public Table getTable() {
        return table;
    }

    /**
     * Returns the name of this column.
     *
     * @return the column name
     */
    public String getName(){
        return this.columnDefinition.getColumnName();
    }

    /**
     * Returns the SQL data type of this column.
     *
     * @return the column's data type as a String
     */
    public String getType(){
        return this.columnDefinition.getColDataType().getDataType();
    }

    /**
     * Indicates whether this column is auto-incremented.
     *
     * @return true if auto-incremented, false otherwise
     */
    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    /**
     * Indicates whether this column is part of the primary key.
     *
     * @return true if this column is a primary key, false otherwise
     */
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    /**
     * Indicates whether this column has a UNIQUE constraint.
     *
     * @return true if unique, false otherwise
     */
    public boolean isUnique() {
        return isUnique;
    }

    /**
     * Returns the column this column references (foreign key), if any.
     *
     * @return the referenced Column, or null if none
     */
    public Column getReference() {
        return reference;
    }

    protected void setReference(Column reference) {
        this.reference = reference;
    }
}
