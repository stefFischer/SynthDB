package at.sfischer.synth.db.generation.values;

import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.DBSchema;
import at.sfischer.synth.db.model.InsertStatement;
import at.sfischer.synth.db.model.Table;
import io.swagger.v3.oas.annotations.links.Link;
import net.sf.jsqlparser.JSQLParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class TableFiller {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableFiller.class);

    public static void createSchema(DBSchema schema, Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()){
            Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
            List<Table> insertions = DBSchema.computeInsertionOrder(tableDependencies);
            for (Table insertion : insertions) {
                stmt.execute(insertion.getCreateTableStatement());
            }
        }
    }

    public static Map<Table, List<InsertStatement>> fillSchema(DBSchema schema, Connection connection, InsertDataGeneration insertDataGeneration, int targetRowNumber, int dependentExampleNumber) throws SQLException {
        return fillSchema(schema, connection, insertDataGeneration, targetRowNumber, dependentExampleNumber, null);
    }

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

    public static Map<Table, List<InsertStatement>> fillSchema(DBSchema schema, Connection connection, InsertDataGeneration insertDataGeneration, Map<String, Integer> tableTargetRowNumbers, int dependentExampleNumber) throws SQLException {
        return fillSchema(schema, connection, insertDataGeneration, tableTargetRowNumbers, dependentExampleNumber, null);
    }

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

    public static List<InsertStatement> fillTable(Table table, Map<Table, Set<Table>> tableDependencies, Connection connection, InsertDataGeneration insertDataGeneration, int targetRowNumber, int dependentExampleNumber) throws SQLException {
        return fillTable(table, tableDependencies, connection, insertDataGeneration, targetRowNumber, dependentExampleNumber, null);
    }

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
