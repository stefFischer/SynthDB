package at.sfischer.synth.db;

public enum DatabaseType {
    MySQL,
    PostgreSQL,
//        Oracle, // Problem with SELECT * FROM ... ORDER BY RANDOM() LIMIT 5;
//        MSSQLServer // Problem with SELECT * FROM ... ORDER BY RANDOM() LIMIT 5;
}
