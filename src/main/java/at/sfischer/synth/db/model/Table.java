package at.sfischer.synth.db.model;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ForeignKeyIndex;
import net.sf.jsqlparser.statement.create.table.Index;

import java.util.*;

/**
 * Represents a database table parsed from a SQL CREATE TABLE statement.
 * <p>
 * Holds the table's name, columns, and their definitions.
 * This class provides access to the columns and can be used to analyze
 * table dependencies based on foreign key references.
 * </p>
 */
public class Table {

    private final CreateTable createTableStatement;

    private final LinkedHashMap<String, Column> columns;

    /**
     * Constructs a Table object from a parsed SQL CREATE TABLE statement.
     * <p>
     * This constructor initializes the columns map by creating a {@link Column}
     * instance for each column definition in the CREATE TABLE statement.
     * </p>
     *
     * @param createTableStatement the {@link CreateTable} object representing
     *                             the SQL CREATE TABLE statement
     */
    public Table(CreateTable createTableStatement) {
        this.createTableStatement = createTableStatement;
        this.columns = new LinkedHashMap<>();
        createTableStatement.getColumnDefinitions().forEach(col -> {
            this.columns.put(col.getColumnName(), new Column(this, col));
        });
    }

    /**
     * Returns the SQL CREATE TABLE statement for this table as a string.
     *
     * @return the CREATE TABLE statement representing this table
     */
    public String getCreateTableStatement(){
        return this.createTableStatement.toString();
    }

    /**
     * Returns the schema name of this table, if specified in the CREATE TABLE statement.
     *
     * @return the schema name, or null if no schema is defined
     */
    public String getSchemaName(){
        net.sf.jsqlparser.schema.Table tableName = createTableStatement.getTable();
        return tableName.getSchemaName();
    }

    /**
     * Returns the name of this table.
     *
     * @return the table name
     */
    public String getName(){
        return this.createTableStatement.getTable().getName();
    }

    /**
     * Returns all columns of this table as a list.
     *
     * @return a list of columns
     */
    public List<Column> getColumns() {
        return new LinkedList<>(columns.values());
    }

    /**
     * Returns a column by its name.
     *
     * @param name the name of the column
     * @return the Column object, or null if no column with the given name exists
     */
    public Column getColumn(String name) {
        return columns.get(name);
    }

    /**
     * Resolves foreign key references for this table using a map of all tables.
     * <p>
     * This method processes both explicit {@link Index} definitions that are foreign keys
     * and inline column-level "REFERENCES" specifications. For each foreign key, it links
     * the referencing {@link Column} in this table to the referenced {@link Column} in
     * the target table.
     * </p>
     *
     * @param tables a map of table names to {@link Table} objects representing all tables
     *               in the schema. The map is used to find the referenced tables and columns.
     */
    public void resolveReferences(LinkedHashMap<String, Table> tables) {
        List<Index> indices = createTableStatement.getIndexes();
        if(indices !=null){
            indices.forEach(idx ->{
                if (idx instanceof ForeignKeyIndex fkIdx) {
                    List<String> colNames = idx.getColumnsNames();
                    String tabName = fkIdx.getTable().getName();
                    List<String> refColNames = fkIdx.getReferencedColumnNames();
                    Table referecedTable = tables.get(tabName);
                    for (int i = 0; i < colNames.size(); i++) {
                        Column col = this.columns.get(colNames.get(i));
                        Column refCol = referecedTable.columns.get(refColNames.get(i));
                        col.setReference(refCol);
                    }
                }
            });
        }
        for (Column column : getColumns()) {
            List<String> specs = column.getColumnDefinition().getColumnSpecs();
            if (specs == null) {
                continue;
            }

            for (int i = 0; i < specs.size(); i++) {
                if ("REFERENCES".equalsIgnoreCase(specs.get(i))) {
                    String tabName = specs.get(i + 1);

                    String referencedColumn = null;
                    if (i + 2 < specs.size() && specs.get(i + 2).startsWith("(") && specs.get(i + 2).endsWith(")")) {
                        referencedColumn = specs.get(i + 2).substring(1, specs.get(i + 2).length() - 1);
                    }

                    Table referecedTable = tables.get(tabName);
                    Column refCol = referecedTable.columns.get(referencedColumn);
                    column.setReference(refCol);
                }
            }
        }
    }

    /**
     * Generates a SQL SELECT statement to retrieve all columns from this table.
     *
     * @return a SQL string in the form of "SELECT col1, col2, ... FROM tableName;"
     */
    public String generateSelectAll() {
        String columnList = String.join(", ", columns.keySet());
        return "SELECT " + columnList + " FROM " + getName() + ";";
    }

    /**
     * Generates a SQL SELECT statement to retrieve a random subset of rows from this table.
     *
     * @param limit the maximum number of rows to retrieve
     * @return a SQL string in the form of
     *         "SELECT col1, col2, ... FROM tableName ORDER BY RANDOM() LIMIT limit;"
     */
    public String generateSelectRandom(int limit) {
        String columnList = String.join(", ", columns.keySet());
        return "SELECT " + columnList + " FROM " + getName() +
               " ORDER BY RANDOM() LIMIT " + limit + ";";
    }

    /**
     * Generates a SQL SELECT statement to count the total number of rows in this table.
     *
     * @return a SQL string in the form of "SELECT COUNT(*) FROM tableName"
     */
    public String generateCountSelect() {
        return "SELECT COUNT(*) FROM " + getName();
    }
}
