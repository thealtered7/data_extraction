package com.dataextraction.pg.meta;

public record TableFqn(String instanceName,String databaseName, String schemaName, String tableName) {

    private static final String UNKNOWN = "unknown";

    static TableFqn parse(String tableFqn) {
        String[] parts = tableFqn.split("\\.", 3);
        if (parts.length >= 4) {
            return new TableFqn(parts[0], parts[1], parts[2], parts[3]);
        }
        return new TableFqn(UNKNOWN, UNKNOWN, UNKNOWN, tableFqn);
    }

    static TableFqn of(String instanceName, String databaseName, String schemaName, String tableName) {
        return new TableFqn(instanceName, databaseName, schemaName, tableName);
    }

    @Override
    public String toString() {
        return instanceName + "." + databaseName + "." + schemaName + "." + tableName;
    }
}
