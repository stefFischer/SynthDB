package at.sfischer.synth.db.generation.values;

import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.DBSchema;
import at.sfischer.synth.db.model.InsertStatement;
import at.sfischer.synth.db.model.Table;
import net.sf.jsqlparser.JSQLParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Utility class for creating database tables and populating data.
 * <p>
 * This class contains static methods to handle schema creation and table data insertion
 * in the correct order, taking into account foreign key dependencies between tables.
 * </p>
 */
public class TableFiller {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableFiller.class);

    /**
     * Creates all tables in the given {@link DBSchema} on the provided {@link Connection}.
     * <p>
     * Tables are created in dependency order based on foreign key relationships to
     * ensure that referenced tables are created before referencing tables.
     * </p>
     *
     * @param schema the {@link DBSchema} containing all tables to create
     * @param connection the {@link Connection} to the target database
     * @throws SQLException if a database access error occurs or a SQL statement fails
     */
    public static void createSchema(DBSchema schema, Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()){
            Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
            List<Table> insertions = DBSchema.computeInsertionOrder(tableDependencies);
            for (Table insertion : insertions) {
                stmt.execute(insertion.getCreateTableStatement());
            }
        }
    }

    /**
     * Populates all tables in the given {@link DBSchema} with generated data.
     * <p>
     * This method automatically determines the insertion order of tables based on
     * foreign key dependencies and fills each table with the specified number of rows.
     * </p>
     *
     * @param schema the {@link DBSchema} containing all tables to fill
     * @param connection the {@link Connection} to the target database
     * @param insertDataGeneration the strategy for generating data for each table
     * @param targetRowNumber the desired number of rows to generate for each table
     * @param dependentExampleNumber the number of dependent rows to generate for referenced tables
     * @return a map from each {@link Table} to the list of {@link InsertStatement} objects generated for that table
     * @throws SQLException if a database access error occurs or a SQL statement fails
     */
    public static Map<Table, List<InsertStatement>> fillSchema(DBSchema schema, Connection connection, InsertDataGeneration insertDataGeneration, int targetRowNumber, int dependentExampleNumber) throws SQLException {
        return fillSchema(schema, connection, insertDataGeneration, targetRowNumber, dependentExampleNumber, null);
    }

    /**
     * Populates all tables in the given {@link DBSchema} with generated data,
     * with optional progress tracking.
     * <p>
     * This method automatically determines the insertion order of tables based on
     * foreign key dependencies and fills each table with the specified number of rows.
     * A {@link TableFillerProgressListener} can be provided to track progress.
     * </p>
     *
     * @param schema the {@link DBSchema} containing all tables to fill
     * @param connection the {@link Connection} to the target database
     * @param insertDataGeneration the strategy for generating data for each table
     * @param targetRowNumber the desired number of rows to generate for each table
     * @param dependentExampleNumber the number of dependent rows to generate for referenced tables
     * @param listener optional listener for tracking progress, can be null
     * @return a map from each {@link Table} to the list of {@link InsertStatement} objects generated for that table
     * @throws SQLException if a database access error occurs or a SQL statement fails
     */
    public static Map<Table, List<InsertStatement>> fillSchema(DBSchema schema, Connection connection, InsertDataGeneration insertDataGeneration, int targetRowNumber, int dependentExampleNumber, TableFillerProgressListener listener) throws SQLException {
        Map<Table, List<InsertStatement>> insertStatements = new LinkedHashMap<>();
        Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
        List<Table> insertions = DBSchema.computeInsertionOrder(tableDependencies);
        if(listener != null){
            listener.setTotalTables(insertions.size());
        }
        for (Table insertion : insertions) {
            insertStatements.put(insertion,
                    fillTable(insertion, tableDependencies, connection, insertDataGeneration, targetRowNumber, dependentExampleNumber, listener)
            );
        }

        return insertStatements;
    }

    /**
     * Populates selected tables in the given {@link DBSchema} with generated data,
     * using a map of table names to target row numbers.
     * <p>
     * This method respects foreign key dependencies between tables, determines the
     * insertion order automatically, and fills each specified table with the desired
     * number of rows. Tables not listed in {@code tableTargetRowNumbers} will be skipped.
     * </p>
     *
     * @param schema the {@link DBSchema} containing all tables
     * @param connection the {@link Connection} to the target database
     * @param insertDataGeneration the strategy for generating data for each table
     * @param tableTargetRowNumbers a map from table names to the desired number of rows to generate
     * @param dependentExampleNumber the number of dependent rows to generate for referenced tables
     * @return a map from each {@link Table} to the list of {@link InsertStatement} objects generated for that table
     * @throws SQLException if a database access error occurs or a SQL statement fails
     */
    public static Map<Table, List<InsertStatement>> fillSchema(DBSchema schema, Connection connection, InsertDataGeneration insertDataGeneration, Map<String, Integer> tableTargetRowNumbers, int dependentExampleNumber) throws SQLException {
        return fillSchema(schema, connection, insertDataGeneration, tableTargetRowNumbers, dependentExampleNumber, null);
    }

    /**
     * Populates selected tables in the given {@link DBSchema} with generated data,
     * using a map of table names to target row numbers, with optional progress tracking.
     * <p>
     * This method respects foreign key dependencies between tables, determines the
     * insertion order automatically, and fills each specified table with the desired
     * number of rows. Tables not listed in {@code tableTargetRowNumbers} will be skipped.
     * A {@link TableFillerProgressListener} can be provided to track progress.
     * </p>
     *
     * @param schema the {@link DBSchema} containing all tables
     * @param connection the {@link Connection} to the target database
     * @param insertDataGeneration the strategy for generating data for each table
     * @param tableTargetRowNumbers a map from table names to the desired number of rows to generate
     * @param dependentExampleNumber the number of dependent rows to generate for referenced tables
     * @param listener optional listener for tracking progress, can be null
     * @return a map from each {@link Table} to the list of {@link InsertStatement} objects generated for that table
     * @throws SQLException if a database access error occurs or a SQL statement fails
     */
    public static Map<Table, List<InsertStatement>> fillSchema(DBSchema schema, Connection connection, InsertDataGeneration insertDataGeneration, Map<String, Integer> tableTargetRowNumbers, int dependentExampleNumber, TableFillerProgressListener listener) throws SQLException {
        if(listener != null){
            listener.setTotalTables(tableTargetRowNumbers.size());
        }

        Map<Table, List<InsertStatement>> insertStatements = new LinkedHashMap<>();
        Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
        List<Table> insertions = DBSchema.computeInsertionOrder(tableDependencies);
        for (Table insertion : insertions) {
            Integer target = tableTargetRowNumbers.get(insertion.getName());
            if(target == null){
                continue;
            }

            insertStatements.put(insertion,
                    fillTable(insertion, tableDependencies, connection, insertDataGeneration, target, dependentExampleNumber, listener)
            );
        }

        return insertStatements;
    }

    /**
     * Populates a single table with generated data until the target row number is reached.
     * <p>
     * This method automatically handles dependent table values for foreign key relationships
     * and executes the generated INSERT statements on the provided {@link Connection}.
     * </p>
     *
     * @param table the {@link Table} to populate
     * @param tableDependencies a map of tables to the set of tables that depend on them; can be null
     * @param connection the {@link Connection} to the database where data will be inserted
     * @param insertDataGeneration the strategy for generating insert statements for the table
     * @param targetRowNumber the desired total number of rows in the table after insertion
     * @param dependentExampleNumber the number of example rows to use from dependent tables for foreign key generation
     * @return a list of {@link InsertStatement} objects representing the inserted rows
     * @throws SQLException if a database access error occurs
     */
    public static List<InsertStatement> fillTable(Table table, Map<Table, Set<Table>> tableDependencies, Connection connection, InsertDataGeneration insertDataGeneration, int targetRowNumber, int dependentExampleNumber) throws SQLException {
        return fillTable(table, tableDependencies, connection, insertDataGeneration, targetRowNumber, dependentExampleNumber, null);
    }

    /**
     * Populates a single table with generated data until the target row number is reached,
     * with optional progress tracking via a {@link TableFillerProgressListener}.
     * <p>
     * This method automatically handles dependent table values for foreign key relationships
     * and executes the generated INSERT statements on the provided {@link Connection}.
     * It will repeatedly attempt to generate and execute inserts until the table reaches
     * {@code targetRowNumber}, skipping failed inserts while logging errors.
     * </p>
     *
     * @param table the {@link Table} to populate
     * @param tableDependencies a map of tables to the set of tables that depend on them; can be null
     * @param connection the {@link Connection} to the database where data will be inserted
     * @param insertDataGeneration the strategy for generating insert statements for the table
     * @param targetRowNumber the desired total number of rows in the table after insertion
     * @param dependentExampleNumber the number of example rows to use from dependent tables for foreign key generation
     * @param listener an optional {@link TableFillerProgressListener} to track progress, can be null
     * @return a list of {@link InsertStatement} objects representing the inserted rows
     * @throws SQLException if a database access error occurs
     */
    public static List<InsertStatement> fillTable(Table table, Map<Table, Set<Table>> tableDependencies, Connection connection, InsertDataGeneration insertDataGeneration, int targetRowNumber, int dependentExampleNumber, TableFillerProgressListener listener) throws SQLException {
        if(tableDependencies == null){
            tableDependencies = new LinkedHashMap<>();
        }

        List<InsertStatement> insertStatements = new LinkedList<>();
        try (Statement stmt = connection.createStatement()) {
            long count = getRowCount(connection, table);
            if(listener != null){
                listener.nextTable(table, targetRowNumber - count);
                if(listener.getTotalTables() <= 0){
                    listener.setTotalTables(1);
                }
            }
            while (count < targetRowNumber) {
                try {
                    String insertStatement = insertDataGeneration.generateInsertStatement(
                            table,
                            count,
                            getTableValues(table, connection, dependentExampleNumber),
                            getTableValues(tableDependencies.get(table), connection, dependentExampleNumber)
                    );

                    LOGGER.debug("Insert statement generated: \"{}\"", insertStatement);

                    InsertStatement insert = InsertStatement.parseInsertStatement(table, insertStatement);
                    if (insert == null) {
                        continue;
                    }

                    stmt.execute(insert.generateInsertStatement());
                    insertStatements.add(insert);

                    if(listener != null){
                        listener.rowGenerated();
                    }
                    LOGGER.debug("Insert statement stored: \"{}\"", insert.generateInsertStatement());
                } catch (JSQLParserException | SQLException e) {
                    LOGGER.debug("Error processing SQL.", e);
                    continue;
                }

                count = getRowCount(connection, table);
            }
        }

        return insertStatements;
    }

    /**
     * Inserts a list of pre-generated {@link InsertStatement} objects into the database
     * in the correct order based on table dependencies.
     * <p>
     * The method first groups the insert statements by table according to the insertion
     * order computed from the schema's table dependencies, ensuring that tables are
     * populated in an order that respects foreign key constraints.
     * </p>
     *
     * @param schema the {@link DBSchema} containing table definitions and dependencies
     * @param insertStatements the list of {@link InsertStatement} objects to insert; can be null
     * @param connection the {@link Connection} to the database where data will be inserted
     * @throws SQLException if a database access error occurs
     * @throws IllegalStateException if an insert statement references a table not present in the insertion order
     */
    public static void insertData(DBSchema schema, List<InsertStatement> insertStatements, Connection connection) throws SQLException {
        if(insertStatements == null){
            return;
        }
        Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
        List<Table> insertionOrder = DBSchema.computeInsertionOrder(tableDependencies);
        Map<Table, List<InsertStatement>> groupedByTable = new LinkedHashMap<>();
        for (Table table : insertionOrder) {
            groupedByTable.put(table, new ArrayList<>());
        }
        for (InsertStatement stmt : insertStatements) {
            Table table = stmt.getTable();
            List<InsertStatement> list = groupedByTable.get(table);
            if (list != null) {
                list.add(stmt);
            } else {
                throw new IllegalStateException("Table not in insertion order: " + table.getName());
            }
        }

        try (Statement stmt = connection.createStatement()) {
            for (Map.Entry<Table, List<InsertStatement>> entry : groupedByTable.entrySet()) {
                for (InsertStatement insertStatement : entry.getValue()) {
                    stmt.execute(insertStatement.generateInsertStatement());
                }
            }
        }
    }

    /**
     * Retrieves random values from a collection of tables.
     * <p>
     * For each table in the given collection, a limited number of rows is selected
     * randomly and returned as a list of column-value mappings.
     * </p>
     *
     * @param tables the collection of {@link Table} objects to retrieve values from; can be null
     * @param connection the {@link Connection} to the database
     * @param rowLimit the maximum number of rows to retrieve per table
     * @return a map where the key is a {@link Table} and the value is a list of rows,
     *         each row represented as a map from {@link Column} to its corresponding value
     * @throws SQLException if a database access error occurs
     */
    public static Map<Table, List<Map<Column, Object>>> getTableValues(Collection<Table> tables, Connection connection, int rowLimit) throws SQLException {
        if(tables == null){
            return new LinkedHashMap<>();
        }

        Map<Table, List<Map<Column, Object>>> valuesMap = new LinkedHashMap<>();
        for (Table table : tables) {
            valuesMap.put(table,getTableValues(table, connection, rowLimit));
        }
        return valuesMap;
    }

    /**
     * Retrieves random values from a single table.
     * <p>
     * A limited number of rows is selected randomly from the table, and each row
     * is returned as a map of column-value pairs.
     * </p>
     *
     * @param table the {@link Table} to retrieve values from
     * @param connection the {@link Connection} to the database
     * @param rowLimit the maximum number of rows to retrieve
     * @return a list of rows, each row represented as a map from {@link Column} to its corresponding value
     * @throws SQLException if a database access error occurs
     */
    public static List<Map<Column, Object>> getTableValues(Table table, Connection connection, int rowLimit) throws SQLException {
        List<Map<Column, Object>> values = new LinkedList<>();
        String sql = table.generateSelectRandom(rowLimit);
        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Map<Column, Object> columnValues = new LinkedHashMap<>();
                for (Column column : table.getColumns()) {
                    Object value = rs.getObject(column.getName());
                    columnValues.put(column, value);
                }
                values.add(columnValues);
            }
        }

        return values;
    }

    /**
     * Retrieves the total number of rows in a given table.
     * <p>
     * Executes a SQL COUNT query generated by the {@link Table#generateCountSelect()} method
     * and returns the resulting row count.
     * </p>
     *
     * @param connection the {@link Connection} to the database
     * @param table the {@link Table} to count rows from
     * @return the number of rows in the table; returns 0 if the table is empty
     * @throws SQLException if a database access error occurs
     */
    public static long getRowCount(Connection connection, Table table) throws SQLException {
        String sql = table.generateCountSelect();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }
}
