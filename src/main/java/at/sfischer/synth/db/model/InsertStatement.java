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

public class InsertStatement {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertStatement.class);

    private final Table table;

    private final Insert insert;

    private final List<Map<Column, Object>> rows;

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

    public static List<InsertStatement> parseInsertStatements(DBSchema schema, Reader reader) throws JSQLParserException {
        String content;
        try (BufferedReader br = new BufferedReader(reader)) {
            content = br.lines().collect(Collectors.joining(System.lineSeparator()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return parseInsertStatements(schema, content);
    }

    public Table getTable() {
        return table;
    }

    public List<Map<Column, Object>> getRows() {
        return rows;
    }

    public static InsertStatement mergeStatements(Collection<InsertStatement> statements){
        if (statements == null || statements.isEmpty()) {
            return null;
        }

        InsertStatement first = statements.iterator().next();
        Table table = first.getTable();
        Set<Column> columns = first.getRows().isEmpty()
                ? Collections.emptySet()
                : first.getRows().getFirst().keySet();
        columns.removeIf(Column::isAutoIncrement);

        List<Map<Column, Object>> rows = new LinkedList<>();
        for (InsertStatement stmt : statements) {
            if (!stmt.getTable().equals(table)) {
                throw new IllegalArgumentException("Cannot merge InsertStatements for different tables");
            }

            for (Map<Column, Object> row : stmt.getRows()) {
                Set<Column> rowColumns = row.keySet();
                rowColumns.removeIf(Column::isAutoIncrement);
                if (!rowColumns.equals(columns)) {
                    throw new IllegalArgumentException("Cannot merge InsertStatements with different columns");
                }
            }

            rows.addAll(stmt.getRows());
        }

        return new InsertStatement(table, rows);
    }

    public String generateInsertStatement() {
        StringBuilder sb = new StringBuilder();

        List<Column> columnList = table.getColumns();
        columnList.removeIf(Column::isAutoIncrement);
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

}
