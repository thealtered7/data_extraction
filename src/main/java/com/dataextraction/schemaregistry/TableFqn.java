package com.dataextraction.schemaregistry;

public record TableFqn(String databaseName, String schemaName, String tableName) {

    private static final String UNKNOWN = "unknown";

    public static TableFqn parse(String tableFqn) {
        String[] parts = tableFqn.split("\\.", 3);
        if (parts.length >= 3) {
            return new TableFqn(parts[0], parts[1], parts[2]);
        }
        return new TableFqn(UNKNOWN, UNKNOWN, tableFqn);
    }

    public static TableFqn of(String databaseName, String schemaName, String tableName) {
        return new TableFqn(databaseName, schemaName, tableName);
    }

    @Override
    public String toString() {
        return databaseName + "." + schemaName + "." + tableName;
    }
}
