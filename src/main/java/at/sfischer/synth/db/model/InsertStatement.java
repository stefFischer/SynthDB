package at.sfischer.synth.db.model;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.UpdateSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents an SQL INSERT statement for a specific table, along with the values to insert.
 * <p>
 * This class can either wrap an existing {@link net.sf.jsqlparser.statement.insert.Insert} object
 * or be constructed directly from a table and row data. Each row is represented as a map from
 * {@link Column} to its corresponding value.
 * </p>
 */
public class InsertStatement {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertStatement.class);

    private final Table table;

    private final Insert insert;

    private final List<Map<Column, Object>> rows;

    /**
     * Constructs an InsertStatement for the given table and list of row values.
     * <p>
     * Each row in the list is represented as a map from columns to their respective values.
     * This constructor does not use an existing {@link Insert} object.
     * </p>
     *
     * @param table the table into which the rows will be inserted
     * @param rows  the data for each row as a list of column-value mappings
     */
    public InsertStatement(Table table, List<Map<Column, Object>> rows) {
        this.table = table;
        this.insert = null;
        this.rows = rows;
    }

    private InsertStatement(Table table, Insert insert) {
        this.table = table;
        this.insert = insert;
        this.rows = new LinkedList<>();
        initRows();
    }

    private void initRows(){
        try {
            List<Column> columns = new ArrayList<>();
            if(this.insert.getColumns() != null){
                for (net.sf.jsqlparser.schema.Column column : this.insert.getColumns()) {
                    Column col = table.getColumn(column.getColumnName());
                    columns.add(col);
                }
                ExpressionList<?> expressionList = this.insert.getValues().getExpressions();
                processRows(columns, expressionList);
            } else {
                Map<Column, Object> row = new LinkedHashMap<>();
                for (UpdateSet updateSet : this.insert.getSetUpdateSets()) {
                    String columnName = updateSet.getColumn(0).getColumnName();
                    Expression expression = updateSet.getValue(0);
                    Column column = table.getColumn(columnName);
                    row.put(column, processExpression(column, expression));
                }
                if(!row.isEmpty()){
                    this.rows.add(row);
                }
            }
        } catch (Throwable t){
            throw new IllegalStateException("Could not parse insert statement.", t);
        }
    }

    private void processRows(List<Column> columns, ExpressionList<?> expressionList){
        Map<Column, Object> row = new LinkedHashMap<>();
        int i = 0;
        for (Expression expression : expressionList) {
            if (expression instanceof ExpressionList<?> exprList) {
                processRows(columns, exprList);
            } else {
                row.put(columns.get(i), processExpression(columns.get(i), expression));
            }

            i++;
        }

        if(!row.isEmpty()){
            this.rows.add(row);
        }
    }

    private Object processExpression(Column column, Expression expression){
        Object rawValue = getRawExpressionValue(expression);
        if(rawValue instanceof String){
            switch (column.getType().toUpperCase()){
                case "TINYINT":
                case "SMALLINT":
                case "MEDIUMINT":
                case "INT":
                case "INTEGER":
                case "BIGINT":
                    return Long.parseLong((String) rawValue);
                case "FLOAT":
                case "DOUBLE":
                case "REAL":
                    return Double.parseDouble((String) rawValue);
            }
        }

        return rawValue;
    }

    private Object getRawExpressionValue(Expression expression){
        switch (expression){
            case BooleanValue booleanValue -> {
                return booleanValue.getValue();
            }
            case LongValue longValue -> {
                return longValue.getValue();
            }
            case DoubleValue doubleValue -> {
                return doubleValue.getValue();
            }
            case HexValue hexValue -> {
                return hexValue.getValue();
            }
            case StringValue stringValue -> {
                return stringValue.getValue();
            }
            case TimeValue timeValue -> {
                return timeValue.getValue();
            }
            case DateValue dateValue -> {
                return dateValue.getValue();
            }
            case NullValue _ -> {
                return "NULL";
            }
            default -> throw new UnsupportedOperationException("Unsupported values type: " + expression.getClass());
        }
    }

    /**
     * Parses a single SQL INSERT statement string into an {@link InsertStatement} object
     * associated with the given {@link Table}.
     * <p>
     * The statement string should be a valid SQL INSERT statement matching the table's schema.
     * The method uses JSQLParser to parse the statement and extract column-value mappings.
     * </p>
     *
     * @param table           the {@link Table} the INSERT statement targets
     * @param insertStatement the SQL INSERT statement string to parse
     * @return an {@link InsertStatement} representing the parsed statement
     * @throws JSQLParserException if the statement cannot be parsed
     */
    public static InsertStatement parseInsertStatement(Table table, String insertStatement) throws JSQLParserException {
        List<Statement> statements = CCJSqlParserUtil.parseStatements(insertStatement);
        if(statements == null){
            return null;
        }

        try {
            for (Statement stmt : statements) {
                if (stmt instanceof Insert insert) {
                    return new InsertStatement(table, insert);
                }
            }
        } catch (Exception e) {
            LOGGER.debug("Could not process insert statement: {}", insertStatement, e);
        }

        return null;
    }

    /**
     * Parses multiple SQL INSERT statements from a string into a list of {@link InsertStatement} objects.
     * <p>
     * Each statement in the input string should be a valid SQL INSERT statement for a table
     * defined in the provided {@link DBSchema}. The method uses JSQLParser to parse the statements
     * and extract column-value mappings for each table.
     * </p>
     *
     * @param schema           the {@link DBSchema} containing the table definitions
     * @param insertStatements a string containing one or more SQL INSERT statements
     * @return a {@link List} of {@link InsertStatement} objects representing the parsed statements
     * @throws JSQLParserException if any statement cannot be parsed
     */
    public static List<InsertStatement> parseInsertStatements(DBSchema schema, String insertStatements) throws JSQLParserException {
        List<InsertStatement> inserts = new LinkedList<>();
        List<Statement> statements = CCJSqlParserUtil.parseStatements(insertStatements);
        if(statements == null){
            return null;
        }

        for (Statement stmt : statements) {
            if (stmt instanceof Insert insert) {
                String tableName = insert.getTable().getName();
                Table table = schema.getTable(tableName);
                if(table == null){
                    LOGGER.debug("Could not find table: {}", tableName);
                    continue;
                }

                try {
                    inserts.add(new InsertStatement(table, insert));
                } catch (Exception e) {
                    LOGGER.debug("Could not process insert statement: {}", insert, e);
                }
            }
        }

        return inserts;
    }

    /**
     * Parses multiple SQL INSERT statements from a {@link Reader} into a list of {@link InsertStatement} objects.
     * <p>
     * Each statement read from the {@link Reader} should be a valid SQL INSERT statement for a table
     * defined in the provided {@link DBSchema}. This method uses JSQLParser to parse the statements
     * and extract column-value mappings for each table.
     * </p>
     *
     * @param schema the {@link DBSchema} containing the table definitions
     * @param reader a {@link Reader} providing the SQL INSERT statements
     * @return a {@link List} of {@link InsertStatement} objects representing the parsed statements
     * @throws JSQLParserException if any statement cannot be parsed
     * @throws IOException          if an I/O error occurs reading from the {@link Reader}
     */
    public static List<InsertStatement> parseInsertStatements(DBSchema schema, Reader reader) throws JSQLParserException {
        String content;
        try (BufferedReader br = new BufferedReader(reader)) {
            content = br.lines().collect(Collectors.joining(System.lineSeparator()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return parseInsertStatements(schema, content);
    }

    /**
     * Returns the {@link Table} associated with this INSERT statement.
     *
     * @return the table this insert statement applies to
     */
    public Table getTable() {
        return table;
    }

    /**
     * Returns the list of rows to be inserted. Each row is represented as a {@link Map} from
     * {@link Column} to the corresponding value.
     *
     * @return the rows to be inserted
     */
    public List<Map<Column, Object>> getRows() {
        return rows;
    }

    /**
     * Merges multiple {@link InsertStatement} objects into a single {@link InsertStatement}.
     * <p>
     * All statements must target the same table and have the same set of non-auto-increment columns.
     * Auto-increment columns are ignored when comparing column sets.
     * </p>
     *
     * @param statements the collection of {@link InsertStatement} objects to merge
     * @return a new {@link InsertStatement} containing all rows from the input statements,
     *         or {@code null} if the input collection is {@code null} or empty
     * @throws IllegalArgumentException if the statements target different tables or have
     *                                  incompatible column sets
     */
    public static InsertStatement mergeStatements(Collection<InsertStatement> statements){
        if (statements == null || statements.isEmpty()) {
            return null;
        }

        InsertStatement first = statements.iterator().next();
        Table table = first.getTable();
        Set<Column> columns = first.getRows().isEmpty()
                ? Collections.emptySet()
                : first.getRows().getFirst().keySet();

        List<Map<Column, Object>> rows = new LinkedList<>();
        for (InsertStatement stmt : statements) {
            if (!stmt.getTable().equals(table)) {
                throw new IllegalArgumentException("Cannot merge InsertStatements for different tables");
            }

            for (Map<Column, Object> row : stmt.getRows()) {
                Set<Column> rowColumns = row.keySet();
                if (!rowColumns.equals(columns)) {
                    throw new IllegalArgumentException("Cannot merge InsertStatements with different columns");
                }
            }

            rows.addAll(stmt.getRows());
        }

        return new InsertStatement(table, rows);
    }

    /**
     * Generates a SQL {@code INSERT} statement representing all rows in this {@link InsertStatement}.
     * <p>
     * Auto-increment columns are omitted from the column list and values.
     * Values are properly formatted based on their type:
     * <ul>
     *     <li>{@code null} values are rendered as {@code NULL}</li>
     *     <li>Strings, {@link java.sql.Date}, and {@link java.sql.Time} values are quoted and
     *         single quotes are escaped</li>
     *     <li>Boolean values are rendered as {@code TRUE} or {@code FALSE}</li>
     *     <li>Other types are rendered using {@link Object#toString()}</li>
     * </ul>
     * Multiple rows are separated by commas and formatted with line breaks for readability.
     * </p>
     *
     * @return a {@link String} containing the complete SQL {@code INSERT} statement
     */
    public String generateInsertStatement() {
        StringBuilder sb = new StringBuilder();

        List<Column> columnList = table.getColumns();
        String columnNames = columnList.stream()
                .map(Column::getName)
                .collect(Collectors.joining(", "));

        sb.append("INSERT INTO ")
                .append(table.getName())
                .append(" (")
                .append(columnNames)
                .append(") VALUES ");

        if(rows.size() > 1){
            sb.append("\n\t");
        }

        List<String> valueRows = new ArrayList<>();
        for (Map<Column, Object> row : rows) {
            List<String> values = new ArrayList<>();
            for (Column col : columnList) {
                Object value = row.get(col);
                if (value == null || "NULL".equals(value)) {
                    values.add("NULL");
                } else if (value instanceof String || value instanceof java.sql.Date || value instanceof java.sql.Time) {
                    values.add("'" + value.toString().replace("'", "''") + "'"); // escape single quotes
                } else if (value instanceof Boolean) {
                    values.add(((Boolean) value) ? "TRUE" : "FALSE");
                } else {
                    values.add(value.toString());
                }
            }
            valueRows.add("(" + String.join(", ", values) + ")");
        }

        sb.append(String.join(",\n\t", valueRows)).append(";");
        return sb.toString();
    }

    /**
     * Sets the values for an auto-increment column in the rows of this insert statement.
     * <p>
     * The first row in {@link #rows} receives the {@code lastGeneratedValue} returned
     * by the database. Each subsequent row receives incremented values in order.
     * <p>
     * For example, if the database returned 10 for a three-row insert, the values
     * assigned will be 10, 11, 12 for the first, second, and third row respectively.
     *
     * @param autoIncrementColumn the column representing the auto-increment key
     * @param lastGeneratedValue  the value returned by the database for the insert
     */
    public void setAutoIncrementValuesIncrementing(Column autoIncrementColumn, long lastGeneratedValue) {
        if (autoIncrementColumn == null || rows.isEmpty()) {
            return;
        }

        long currentValue = lastGeneratedValue;
        for (Map<Column, Object> row : rows) {
            row.put(autoIncrementColumn, currentValue);
            currentValue++;
        }
    }
}
