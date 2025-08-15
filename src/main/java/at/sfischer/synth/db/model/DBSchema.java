package at.sfischer.synth.db.model;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.stream.Collectors;

public class DBSchema {

    private final LinkedHashMap<String, Table> tables;

    private DBSchema() {
        this.tables = new LinkedHashMap<>();
    }

    public static DBSchema parseSchema(String ddl) throws JSQLParserException {
        DBSchema schema = new DBSchema();
        List<Statement> statements = CCJSqlParserUtil.parseStatements(ddl);
        for (Statement stmt : statements) {
            // CREATE TABLE
            if (stmt instanceof CreateTable createTable) {
                Table table = new Table(createTable);
                schema.tables.put(table.getName(), table);
            }

            // TODO Support "ALTER TABLE".

            // TODO Support "DROP TABLE".
        }

        for (Table table : schema.tables.values()) {
            table.resolveReferences(schema.tables);
        }

        return schema;
    }

    public static DBSchema parseSchema(Reader reader) throws JSQLParserException {
        String content;
        try (BufferedReader br = new BufferedReader(reader)) {
            content = br.lines().collect(Collectors.joining(System.lineSeparator()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return parseSchema(content);
    }

    public List<Table> getTables() {
        return new LinkedList<>(tables.values());
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public Map<Table, Set<Table>> getTableDependencies(){
        Map<Table, Set<Table>> tableDependencies = new HashMap<>();
        for (Table table : getTables()) {
            Set<Table> referencedTables = tableDependencies.computeIfAbsent(table, k -> new HashSet<>());
            for (Column column : table.getColumns()) {
                Column ref = column.getReference();
                if (ref != null) {
                    Table targetTable = ref.getTable();
                    referencedTables.add(targetTable);
                }
            }
        }

        return tableDependencies;
    }

    public static List<Table> computeInsertionOrder(Map<Table, Set<Table>> dependencies) {
        List<Table> result = new ArrayList<>();
        while(result.size() < dependencies.size()){
            dependencies.forEach((k, v) -> {
                if(result.contains(k)){
                    return;
                }

                if(v == null || v.isEmpty()){
                    result.add(k);
                    return;
                }

                if(new HashSet<>(result).containsAll(v)){
                    result.add(k);
                }
            });
        }

        return result;
    }

    public static List<Table> computeDropOrder(Map<Table, Set<Table>> dependencies) {
        List<Table> insertions = computeInsertionOrder(dependencies);
        return insertions.reversed();
    }


}
