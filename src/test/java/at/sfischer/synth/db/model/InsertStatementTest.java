package at.sfischer.synth.db.model;

import at.sfischer.synth.db.Utils;
import kotlin.Pair;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class InsertStatementTest {

    private static Table employeeTable;

    private static Table employeeTableAutoIncrement;

    @BeforeAll
    public static void setup() throws JSQLParserException {
        String employeeTableCreate = """
                CREATE TABLE employee (
                    id INT PRIMARY KEY,
                    name VARCHAR(50)
                )
            """;
        DBSchema schema = DBSchema.parseSchema(employeeTableCreate);
        employeeTable = schema.getTable("employee");

        String employeeTableCreate2 = """
                CREATE TABLE employee (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(50)
                )
            """;
        DBSchema schema2 = DBSchema.parseSchema(employeeTableCreate2);
        employeeTableAutoIncrement = schema2.getTable("employee");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void parseInsertStatement1() throws JSQLParserException {
        String insertStatement = "INSERT INTO employee (id, name) VALUES (2, 'Jane Smith')";
        InsertStatement insert = InsertStatement.parseInsertStatement(employeeTable, insertStatement);
        assertNotNull(insert);

        List<Map<Column, Object>> rowsToInsert = insert.getRows();
        List<Map<Column, Object>> expected = List.of(
                Utils.linkedMap(
                        new Pair<>(employeeTable.getColumn("id"), 2L),
                        new Pair<>(employeeTable.getColumn("name"), "Jane Smith")
                )
        );

        assertThat(rowsToInsert)
                .containsAll(expected);

        String expectedStatement = "INSERT INTO employee (id, name) VALUES (2, 'Jane Smith');";
        String actualStatement = insert.generateInsertStatement();
        assertEquals(expectedStatement, actualStatement);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void parseInsertStatement2() throws JSQLParserException {
        String insertStatement = "INSERT INTO employee SET id = '1', name = 'John Doe' ;";
        InsertStatement insert = InsertStatement.parseInsertStatement(employeeTable, insertStatement);
        assertNotNull(insert);

        List<Map<Column, Object>> rowsToInsert = insert.getRows();
        List<Map<Column, Object>> expected = List.of(
                Utils.linkedMap(
                        new Pair<>(employeeTable.getColumn("id"), 1L),
                        new Pair<>(employeeTable.getColumn("name"), "John Doe")
                )
        );

        assertThat(rowsToInsert)
                .containsAll(expected);

        String expectedStatement = "INSERT INTO employee (id, name) VALUES (1, 'John Doe');";
        String actualStatement = insert.generateInsertStatement();
        assertEquals(expectedStatement, actualStatement);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void parseInsertStatementTwoValues() throws JSQLParserException {
        String insertStatement = "INSERT INTO employee (id, name) VALUES  (1, 'John Doe'), (2, 'Jane Smith')";
        InsertStatement insert = InsertStatement.parseInsertStatement(employeeTable, insertStatement);
        assertNotNull(insert);

        List<Map<Column, Object>> rowsToInsert = insert.getRows();
        List<Map<Column, Object>> expected = List.of(
                Utils.linkedMap(
                        new Pair<>(employeeTable.getColumn("id"), 1L),
                        new Pair<>(employeeTable.getColumn("name"), "John Doe")
                ),
                Utils.linkedMap(
                        new Pair<>(employeeTable.getColumn("id"), 2L),
                        new Pair<>(employeeTable.getColumn("name"), "Jane Smith")
                )
        );

        assertThat(rowsToInsert)
                .containsAll(expected);

        String expectedStatement = "INSERT INTO employee (id, name) VALUES \n\t(1, 'John Doe'),\n\t(2, 'Jane Smith');";
        String actualStatement = insert.generateInsertStatement();
        assertEquals(expectedStatement, actualStatement);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void parseInsertStatementAutoInc() throws JSQLParserException {
        String insertStatement = "INSERT INTO employee (id, name) VALUES (2, 'Jane Smith')";
        InsertStatement insert = InsertStatement.parseInsertStatement(employeeTableAutoIncrement, insertStatement);
        assertNotNull(insert);

        List<Map<Column, Object>> rowsToInsert = insert.getRows();
        List<Map<Column, Object>> expected = List.of(
                Utils.linkedMap(
                        new Pair<>(employeeTableAutoIncrement.getColumn("id"), 2L),
                        new Pair<>(employeeTableAutoIncrement.getColumn("name"), "Jane Smith")
                )
        );

        assertThat(rowsToInsert)
                .containsAll(expected);

        String expectedStatement = "INSERT INTO employee (name) VALUES ('Jane Smith');";
        String actualStatement = insert.generateInsertStatement();
        assertEquals(expectedStatement, actualStatement);
    }
}
