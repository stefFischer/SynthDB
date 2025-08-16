package at.sfischer.synth.db.generation.values;

import at.sfischer.synth.db.Utils;
import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.DBSchema;
import at.sfischer.synth.db.model.Table;
import kotlin.Pair;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InsertDataGenerationTest {

    @Test
    public void generateEmptyDependentTableValuesTest() {
        String valuesString = InsertDataGeneration.generateDependentTableValues(new HashMap<>());
        assertTrue(valuesString.isEmpty());
    }

    @Test
    public void generateNullDependentTableValuesTest() {
        String valuesString = InsertDataGeneration.generateDependentTableValues(null);
        assertTrue(valuesString.isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void generateDependentTableValuesTest() throws JSQLParserException {
        String ddl = """
            CREATE TABLE employee (
                id INT PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                department_id INT REFERENCES department(id)
            );

            CREATE TABLE department (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            );
        """;

        DBSchema schema = DBSchema.parseSchema(ddl);

        Table employee = schema.getTable("employee");
        Column employeeId = employee.getColumn("id");
        Column employeeFirstName = employee.getColumn("first_name");
        Column employeeSecondName = employee.getColumn("last_name");
        Column employeeDepartmentId = employee.getColumn("department_id");
        Table department = schema.getTable("department");
        Column departmentId = department.getColumn("id");
        Column departmentName = department.getColumn("name");

        Map<Table, List<Map<Column, Object>>> dependentTableValues = new LinkedHashMap<>();
        dependentTableValues.put(employee,
            List.of(
                Utils.linkedMap(
                        new Pair<>(employeeId, 1),
                        new Pair<>(employeeFirstName, "John"),
                        new Pair<>(employeeSecondName, "Doe"),
                        new Pair<>(employeeDepartmentId, 1)
                ),
                Utils.linkedMap(
                        new Pair<>(employeeId, 2),
                        new Pair<>(employeeFirstName, "Jane"),
                        new Pair<>(employeeSecondName, "Smith"),
                        new Pair<>(employeeDepartmentId, 3)
                )
            )
        );
        dependentTableValues.put(department,
            List.of(
                Utils.linkedMap(
                        new Pair<>(departmentId, 1),
                        new Pair<>(departmentName, "HR")
                ),
                Utils.linkedMap(
                        new Pair<>(departmentId, 2),
                        new Pair<>(departmentName, "Research")
                ),
                Utils.linkedMap(
                        new Pair<>(departmentId, 3),
                        new Pair<>(departmentName, "Finance")
                )
            )
        );

        String valuesString = InsertDataGeneration.generateDependentTableValues(dependentTableValues);

        String expected = """
                Table: employee
                | id | first_name | last_name | department_id |
                | --- | --- | --- | --- |
                | 1 | John | Doe | 1 |
                | 2 | Jane | Smith | 3 |
                
                Table: department
                | id | name |
                | --- | --- |
                | 1 | HR |
                | 2 | Research |
                | 3 | Finance |
                
                """;

        assertEquals(expected, valuesString);
    }

    @Test
    public void generateUserMessageTest() throws JSQLParserException {
        String ddl = """
            CREATE TABLE employee (
                          id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique identifier for the employee',
                          name VARCHAR(100) NOT NULL COMMENT 'Full name of the employee',
                          hire_date DATE COMMENT 'Date when the employee was hired',
                          salary DECIMAL(10,2) COMMENT 'Monthly salary in USD'
            ) COMMENT='Stores company employees and their employment details';
        """;

        DBSchema schema = DBSchema.parseSchema(ddl);

        Table employee = schema.getTable("employee");
        String values = """
                | id | name          | hire_date | salary  |
                | -- | ------------- | ---------- | ------- |
                | 1  | John Doe      | 2018-04-15 | 5500.00 |
                | 2  | Jane Smith    | 2019-11-03 | 6200.50 |
                | 3  | Michael Brown | 2020-06-20 | 4800.75 |
                """;

        String userMessage = InsertDataGeneration.generateUserMessage(employee, 3, values, "");
        String expected = """
        This is the table to generate data for:
        ```
        CREATE TABLE employee (id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Unique identifier for the employee', name VARCHAR (100) NOT NULL COMMENT 'Full name of the employee', hire_date DATE COMMENT 'Date when the employee was hired', salary DECIMAL (10, 2) COMMENT 'Monthly salary in USD') COMMENT = 'Stores company employees and their employment details'
        ```
        There are already 3 rows in the table.
        Here are some example values already in the table:
        | id | name          | hire_date | salary  |
        | -- | ------------- | ---------- | ------- |
        | 1  | John Doe      | 2018-04-15 | 5500.00 |
        | 2  | Jane Smith    | 2019-11-03 | 6200.50 |
        | 3  | Michael Brown | 2020-06-20 | 4800.75 |""";

        assertEquals(expected, userMessage);
    }
}
