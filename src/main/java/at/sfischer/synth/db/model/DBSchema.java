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

    /**
     * Parses a SQL DDL string containing {@code CREATE TABLE} statements and
     * returns a {@link DBSchema} object representing the database schema.
     *
     * @param ddl the SQL DDL string containing the {@code CREATE TABLE} statements
     * @return a {@link DBSchema} representing the parsed schema
     * @throws JSQLParserException if the DDL cannot be parsed correctly
     */
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

    /**
     * Parses SQL DDL from a {@link Reader} containing {@code CREATE TABLE} statements
     * and returns a {@link DBSchema} object representing the database schema.
     *
     * @param reader the {@link Reader} providing the SQL DDL input
     * @return a {@link DBSchema} representing the parsed schema
     * @throws JSQLParserException if the DDL cannot be parsed correctly
     */
    public static DBSchema parseSchema(Reader reader) throws JSQLParserException {
        String content;
        try (BufferedReader br = new BufferedReader(reader)) {
            content = br.lines().collect(Collectors.joining(System.lineSeparator()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return parseSchema(content);
    }

    /**
     * Returns a list of all tables in the schema.
     * <p>
     * The returned list is a copy of the internal table collection, so modifying it
     * does not affect the underlying schema.
     *
     * @return a {@link List} of {@link Table} objects representing the tables
     *         in the schema
     */
    public List<Table> getTables() {
        return new LinkedList<>(tables.values());
    }

    /**
     * Retrieves a table from the schema by its name.
     *
     * @param name the name of the table to retrieve
     * @return the {@link Table} object with the given name, or {@code null} if
     *         no table with that name exists in the schema
     */
    public Table getTable(String name) {
        return tables.get(name);
    }

    /**
     * Computes the dependencies between tables in this schema.
     * <p>
     * For each table, this method determines which other tables it references
     * via foreign key columns. The result is a map where each key is a table,
     * and the associated value is the set of tables that the key table depends on.
     * </p>
     *
     * @return a {@link Map} mapping each table to the set of tables it references
     */
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

    /**
     * Computes a valid insertion order for tables based on their dependencies.
     * <p>
     * This method takes a map of table dependencies, where each key is a table
     * and the corresponding value is the set of tables that must be inserted
     * before it (i.e., tables it depends on via foreign keys). It returns a list
     * of tables in an order such that all referenced tables are inserted before
     * a table that depends on them.
     * </p>
     * <p>
     * Note: This method assumes that the dependency graph is acyclic. If there
     * are cycles in the dependencies, this method may not terminate or may
     * produce incomplete results.
     * </p>
     *
     * @param dependencies a {@link Map} mapping each table to the set of tables it depends on
     * @return a {@link List} of tables in a valid insertion order
     */
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

    /**
     * Computes a valid drop order for tables based on their dependencies.
     * <p>
     * Tables that depend on others (via foreign keys) must be dropped before
     * the tables they reference. This method leverages {@link #computeInsertionOrder(Map)}
     * and reverses the result to produce a safe drop order.
     * </p>
     *
     * @param dependencies a {@link Map} mapping each table to the set of tables it depends on
     * @return a {@link List} of tables in a valid drop order
     */
    public static List<Table> computeDropOrder(Map<Table, Set<Table>> dependencies) {
        List<Table> insertions = computeInsertionOrder(dependencies);
        return insertions.reversed();
    }
}
