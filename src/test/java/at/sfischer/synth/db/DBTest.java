package at.sfischer.synth.db;

import at.sfischer.synth.db.generation.values.InsertDataGenerationOllama;
import at.sfischer.synth.db.generation.values.TableFiller;
import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.DBSchema;
import at.sfischer.synth.db.model.InsertStatement;
import at.sfischer.synth.db.model.Table;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class DBTest implements DockerOllamaTests {

    private static final InsertDataGenerationOllama INSERT_DATA_GENERATION_OLLAMA = new InsertDataGenerationOllama(URL, MODEL);

    @Test
    public void singleMySQLTableFillTest() throws Exception {
        int targetRowNumber = 5;

        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=MySQL");
            Statement stmt = conn.createStatement()) {

            String ddl = """
                CREATE TABLE employee (
                    id INT PRIMARY KEY,
                    name VARCHAR(50)
                )
            """;

            DBSchema schema = DBSchema.parseSchema(ddl);
            Table table = schema.getTable("employee");
            stmt.execute(table.getCreateTableStatement());

            List<InsertStatement> insertStatements = TableFiller.fillTable(table, null, conn, INSERT_DATA_GENERATION_OLLAMA, targetRowNumber, 5);

            insertStatements.forEach(statement -> System.out.println(statement.generateInsertStatement()));

            InsertStatement merged = InsertStatement.mergeStatements(insertStatements);
            System.out.println(merged.generateInsertStatement());

            long count = checkTableRows(table, stmt);
            assertTrue(count >= targetRowNumber  , "Wrong count for table: " + table.getName());
        }
    }

    @Test
    public void singleMySQLTableFillAutoIncTest() throws Exception {
        int targetRowNumber = 5;

        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=MySQL");
             Statement stmt = conn.createStatement()) {

            String ddl = """
                CREATE TABLE employee (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(50)
                )
            """;

            DBSchema schema = DBSchema.parseSchema(ddl);
            Table table = schema.getTable("employee");
            stmt.execute(table.getCreateTableStatement());

            List<InsertStatement> insertStatements = TableFiller.fillTable(table, null, conn, INSERT_DATA_GENERATION_OLLAMA, targetRowNumber, 5);

            insertStatements.forEach(statement -> System.out.println(statement.generateInsertStatement()));

            InsertStatement merged = InsertStatement.mergeStatements(insertStatements);
            System.out.println(merged.generateInsertStatement());

            long count = checkTableRows(table, stmt);
            assertTrue(count >= targetRowNumber  , "Wrong count for table: " + table.getName());
        }
    }

    @Test
    public void singlePostgreSQLTableFillTest() throws Exception {
        int targetRowNumber = 5;

        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=PostgreSQL");
             Statement stmt = conn.createStatement()) {

            String ddl = """
                CREATE TABLE employee (
                    id INT PRIMARY KEY,
                    name VARCHAR(50)
                )
            """;

            DBSchema schema = DBSchema.parseSchema(ddl);
            Table table = schema.getTable("employee");
            stmt.execute(table.getCreateTableStatement());

            List<InsertStatement> insertStatements = TableFiller.fillTable(table, null, conn, INSERT_DATA_GENERATION_OLLAMA, targetRowNumber, 5);

            insertStatements.forEach(statement -> System.out.println(statement.generateInsertStatement()));

            InsertStatement merged = InsertStatement.mergeStatements(insertStatements);
            System.out.println(merged.generateInsertStatement());

            long count = checkTableRows(table, stmt);
            assertTrue(count >= targetRowNumber  , "Wrong count for table: " + table.getName());
        }
    }

    @Test
    public void multipleMySQLTablesFillTest() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=MySQL");
             Statement stmt = conn.createStatement()) {

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
                    name VARCHAR(100) NOT NULL,
                    location VARCHAR(100)
                );
            """;

            Map<String, Integer> tableTargetRowNumbers = Map.of(
                    "employee", 10,
                    "department", 5
            );

            DBSchema schema = DBSchema.parseSchema(ddl);

            TableFiller.createSchema(schema, conn);

            Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
            List<Table> insertions = DBSchema.computeInsertionOrder(tableDependencies);

            Map<Table, List<InsertStatement>> insertStatements = TableFiller.fillSchema(schema, conn, INSERT_DATA_GENERATION_OLLAMA, tableTargetRowNumbers, 5);

            insertStatements.forEach((table, inserts) -> {
                inserts.forEach(statement -> System.out.println(statement.generateInsertStatement()));
                InsertStatement merged = InsertStatement.mergeStatements(inserts);
                System.out.println(merged.generateInsertStatement());
            });


            for (Table insertion : insertions) {
                long count = checkTableRows(insertion, stmt);
                assertTrue(count >= tableTargetRowNumbers.get(insertion.getName())  , "Wrong count for table: " + insertion.getName());
            }
        }
    }

    @Test
    public void multipleMySQLTablesFillWithExamplesTest() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=MySQL");
             Statement stmt = conn.createStatement()) {

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
                    name VARCHAR(100) NOT NULL,
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

            DBSchema schema = DBSchema.parseSchema(ddl);

            TableFiller.createSchema(schema, conn);

            List<InsertStatement> exampleDataStatements = InsertStatement.parseInsertStatements(schema, exampleData);
            TableFiller.insertData(schema, exampleDataStatements, conn);

            Map<Table, Set<Table>> tableDependencies = schema.getTableDependencies();
            List<Table> insertions = DBSchema.computeInsertionOrder(tableDependencies);

            Map<Table, List<InsertStatement>> insertStatements = TableFiller.fillSchema(schema, conn, INSERT_DATA_GENERATION_OLLAMA, tableTargetRowNumbers, 5);

            insertStatements.forEach((table, inserts) -> {
                inserts.forEach(statement -> System.out.println(statement.generateInsertStatement()));
                InsertStatement merged = InsertStatement.mergeStatements(inserts);
                System.out.println(merged.generateInsertStatement());
            });


            for (Table insertion : insertions) {
                long count = checkTableRows(insertion, stmt);
                assertTrue(count >= tableTargetRowNumbers.get(insertion.getName())  , "Wrong count for table: " + insertion.getName());
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
