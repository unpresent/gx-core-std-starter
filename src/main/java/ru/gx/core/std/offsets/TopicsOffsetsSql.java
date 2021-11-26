package ru.gx.core.std.offsets;

class TopicsOffsetsSql {
    public static class Load {
        public final static String SQL =
                "SELECT\n" +
                        "    \"Topic\",\n" +
                        "    \"Partition\",\n" +
                        "    \"Offset\"\n" +
                        "FROM \"Kafka\".\"Offsets\"" +
                        "WHERE  \"Direction\" = ?" +
                        "   AND \"ServiceName\" = ?";
        public final static int COLUMN_INDEX_TOPIC = 1;
        public final static int COLUMN_INDEX_PARTITION = 2;
        public final static int COLUMN_INDEX_OFFSET = 3;

        public final static int PARAM_INDEX_DIRECTION = 1;
        public final static int PARAM_INDEX_SERVICE_NAME = 2;
    }

    public static class Save {
        public final static String SQL =
                "INSERT INTO \"Kafka\".\"Offsets\" (\"Direction\", \"ServiceName\", \"Topic\", \"Partition\", \"Offset\")\n" +
                        "VALUES (?, ?, ?, ?, ?)\n" +
                        "ON CONFLICT (\"Direction\", \"ServiceName\", \"Topic\", \"Partition\") DO UPDATE SET\n" +
                        "    \"Offset\" = EXCLUDED.\"Offset\"";
        public final static int PARAM_INDEX_DIRECTION = 1;
        public final static int PARAM_INDEX_READER = 2;
        public final static int PARAM_INDEX_TOPIC = 3;
        public final static int PARAM_INDEX_PARTITION = 4;
        public final static int PARAM_INDEX_OFFSET = 5;

    }
}
