package at.sfischer.synth.db;

import at.sfischer.synth.db.generation.values.InsertDataGenerationOllama;
import at.sfischer.synth.db.generation.values.TableFiller;
import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.DBSchema;
import at.sfischer.synth.db.model.InsertStatement;
import at.sfischer.synth.db.model.Table;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class DBFillTests implements DockerOllamaTests {

    private static final InsertDataGenerationOllama INSERT_DATA_GENERATION_OLLAMA = new InsertDataGenerationOllama(URL, MODEL);

    @Test
    public void singleMySQLTableFillTest() throws Exception {
        DatabaseType type = DatabaseType.MySQL;
        String ddl = """
            CREATE TABLE employee (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            )
        """;
        int targetRowNumber = 5;
        int fewShotExamples = 2;

        DBSchema schema = DBSchema.parseSchema(ddl);
        testTableFilling(type, schema, targetRowNumber, null, fewShotExamples);
    }

    @Test
    public void singleMySQLTableFillAutoIncTest() throws Exception {
        DatabaseType type = DatabaseType.MySQL;
        String ddl = """
            CREATE TABLE employee (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(50)
            )
        """;
        int targetRowNumber = 5;
        int fewShotExamples = 2;

        DBSchema schema = DBSchema.parseSchema(ddl);

        testTableFilling(type, schema, targetRowNumber, null, fewShotExamples);
    }

    @Test
    public void singlePostgreSQLTableFillTest() throws Exception {
        DatabaseType type = DatabaseType.PostgreSQL;
        String ddl = """
            CREATE TABLE employee (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            )
        """;
        int targetRowNumber = 5;
        int fewShotExamples = 2;

        DBSchema schema = DBSchema.parseSchema(ddl);

        testTableFilling(type, schema, targetRowNumber, null, fewShotExamples);
    }

    @Test
    public void multipleMySQLTablesFillTest() throws Exception {
        DatabaseType type = DatabaseType.MySQL;
        String ddl = """
                    CREATE TABLE employee (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        first_name VARCHAR(50) NOT NULL,
                        last_name VARCHAR(50) NOT NULL,
                        hire_date DATE NOT NULL,
                        department_id INT REFERENCES department(id)
                    );
                
                    CREATE TABLE department (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        name VARCHAR(100) NOT NULL UNIQUE,
                        location VARCHAR(100)
                    );
                """;

        Map<String, Integer> tableTargetRowNumbers = Map.of(
                "employee", 15,
                "department", 10
        );
        int fewShotExamples = 2;

        DBSchema schema = DBSchema.parseSchema(ddl);
        testTableFilling(type, schema, tableTargetRowNumbers, null, fewShotExamples);
    }

    @Test
    public void multipleMySQLTablesFillWithExamplesTest() throws Exception {
        DatabaseType type = DatabaseType.MySQL;
        String ddl = """
            CREATE TABLE employee (
                id INT PRIMARY KEY AUTO_INCREMENT,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                hire_date DATE NOT NULL,
                department_id INT REFERENCES department(id)
            );

            CREATE TABLE department (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL UNIQUE,
                location VARCHAR(100)
            );
        """;

        String exampleData = """
            INSERT INTO employee (first_name, last_name, hire_date, department_id) VALUES
                ('Astrid', 'Kovach', '2009-06-19', 2),
                ('Liam', 'Mendez', '2018-02-01', 1);

            INSERT INTO department (id, name, location) VALUES
                (1, 'Sales', 'New York'),
                (2, 'Marketing', 'New York');
        """;

        Map<String, Integer> tableTargetRowNumbers = Map.of(
                "employee", 10,
                "department", 5
        );
        int fewShotExamples = 2;

        DBSchema schema = DBSchema.parseSchema(ddl);
        List<InsertStatement> exampleDataStatements = InsertStatement.parseInsertStatements(schema, exampleData);

        testTableFilling(type, schema, tableTargetRowNumbers, exampleDataStatements, fewShotExamples);
    }

    public static void testTableFilling(DatabaseType type, DBSchema schema, int targetRowNumber, List<InsertStatement> exampleDataStatements, int fewShotExamples) throws SQLException {
        List<InsertStatement> insertStatements = new LinkedList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=" + type.name());
             Statement stmt = conn.createStatement()) {

            TableFiller.createSchema(schema, conn);
            TableFiller.insertData(schema, exampleDataStatements, conn);

            Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
            List<Table> insertions = DBSchema.computeInsertionOrder(tableDependencies);

            Map<Table, List<InsertStatement>> rawStatements = TableFiller.fillSchema(schema, conn, INSERT_DATA_GENERATION_OLLAMA, targetRowNumber, fewShotExamples);

            rawStatements.forEach((_, inserts) -> {
                inserts.forEach(statement -> System.out.println(statement.generateInsertStatement()));
                InsertStatement merged = InsertStatement.mergeStatements(inserts);
                insertStatements.add(merged);
                System.out.println(merged.generateInsertStatement());
            });


            for (Table insertion : insertions) {
                long count = checkTableRows(insertion, stmt);
                assertTrue(count >= targetRowNumber, "Wrong count for table: " + insertion.getName());
            }
        }

        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test2;MODE=" + type.name());
             Statement stmt = conn.createStatement()){
            TableFiller.createSchema(schema, conn);
            TableFiller.insertData(schema, exampleDataStatements, conn);
            TableFiller.insertData(schema, insertStatements, conn);

            Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
            List<Table> insertions = DBSchema.computeInsertionOrder(tableDependencies);
            for (Table insertion : insertions) {
                long count = checkTableRows(insertion, stmt);
                assertTrue(count >= targetRowNumber  , "Wrong count for table: " + insertion.getName());
            }
        }
    }

    public static void testTableFilling(DatabaseType type, DBSchema schema, Map<String, Integer> tableTargetRowNumbers, List<InsertStatement> exampleDataStatements, int fewShotExamples) throws SQLException {
        List<InsertStatement> insertStatements = new LinkedList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=" + type.name());
             Statement stmt = conn.createStatement()) {

            TableFiller.createSchema(schema, conn);
            TableFiller.insertData(schema, exampleDataStatements, conn);

            Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
            List<Table> insertions = DBSchema.computeInsertionOrder(tableDependencies);

            Map<Table, List<InsertStatement>> rawStatements = TableFiller.fillSchema(schema, conn, INSERT_DATA_GENERATION_OLLAMA, tableTargetRowNumbers, fewShotExamples);

            rawStatements.forEach((_, inserts) -> {
                inserts.forEach(statement -> System.out.println(statement.generateInsertStatement()));
                InsertStatement merged = InsertStatement.mergeStatements(inserts);
                insertStatements.add(merged);
                System.out.println(merged.generateInsertStatement());
            });


            for (Table insertion : insertions) {
                long count = checkTableRows(insertion, stmt);
                assertTrue(count >= tableTargetRowNumbers.get(insertion.getName())  , "Wrong count for table: " + insertion.getName());
            }
        }

        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test2;MODE=" + type.name())){
            TableFiller.createSchema(schema, conn);
            TableFiller.insertData(schema, exampleDataStatements, conn);
            TableFiller.insertData(schema, insertStatements, conn);

            Statement stmt = conn.createStatement();

            Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
            List<Table> insertions = DBSchema.computeInsertionOrder(tableDependencies);
            for (Table insertion : insertions) {
                long count = checkTableRows(insertion, stmt);
                assertTrue(count >= tableTargetRowNumbers.get(insertion.getName()), "Wrong count for table: " + insertion.getName());
            }
        }
    }

    private static long checkTableRows(Table table, Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery(table.generateSelectAll());

        StringBuilder header = new StringBuilder();
        for (Column column : table.getColumns()) {
            header.append(column.getName()).append(" : ");
        }
        System.out.println(header);

        long count = 0;
        while (rs.next()) {
            count++;
            StringBuilder results = new StringBuilder();
            for (Column column : table.getColumns()) {
                Object value = rs.getObject(column.getName());
                results.append(value).append(" : ");
            }

            System.out.println(results);
        }

        return count;
    }
}
