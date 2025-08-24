package at.sfischer.synth.db.model;

import net.sf.jsqlparser.JSQLParserException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DBSchemaTest {

    @Test
    public void parseMySQLSchemaTest1() throws JSQLParserException {
        String ddl = """
            CREATE TABLE users (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
                name VARCHAR(100),
                PRIMARY KEY (id)
            );
        """;

        DBSchema schema = DBSchema.parseSchema(ddl);
        assertEquals(1, schema.getTables().size());

        Table users = schema.getTable("users");
        assertNotNull(users);

        Column id = users.getColumn("id");
        assertNotNull(id);
        assertEquals("BIGINT UNSIGNED", id.getType());
        assertTrue(id.isAutoIncrement());
        assertTrue(id.isPrimaryKey());
        assertTrue(id.isUnique());
    }

    @Test
    public void parseMySQLSchemaTest2() throws JSQLParserException {
        String ddl = """
            CREATE TABLE users (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100)
            );
        """;

        DBSchema schema = DBSchema.parseSchema(ddl);
        assertEquals(1, schema.getTables().size());

        Table users = schema.getTable("users");
        assertNotNull(users);

        Column id = users.getColumn("id");
        assertNotNull(id);
        assertEquals("BIGINT UNSIGNED", id.getType());
        assertTrue(id.isAutoIncrement());
        assertTrue(id.isPrimaryKey());
        assertTrue(id.isUnique());
    }

    @Test
    public void parsePostgreSQLSchemaTest1() throws JSQLParserException {
        String ddl = """
            CREATE TABLE users (
                id SERIAL,
                name VARCHAR(100),
                PRIMARY KEY (id)
            );
        """;

        DBSchema schema = DBSchema.parseSchema(ddl);
        assertEquals(1, schema.getTables().size());

        Table users = schema.getTable("users");
        assertNotNull(users);

        Column id = users.getColumn("id");
        assertNotNull(id);
        assertEquals("SERIAL", id.getType());
        assertTrue(id.isAutoIncrement());
        assertTrue(id.isPrimaryKey());
        assertTrue(id.isUnique());
    }

    @Test
    public void parsePostgreSQLSchemaTest2() throws JSQLParserException {
        String ddl = """
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100)
            );
        """;

        DBSchema schema = DBSchema.parseSchema(ddl);
        assertEquals(1, schema.getTables().size());

        Table users = schema.getTable("users");
        assertNotNull(users);

        Column id = users.getColumn("id");
        assertNotNull(id);
        assertEquals("SERIAL", id.getType());
        assertTrue(id.isAutoIncrement());
        assertTrue(id.isPrimaryKey());
        assertTrue(id.isUnique());
    }
}
