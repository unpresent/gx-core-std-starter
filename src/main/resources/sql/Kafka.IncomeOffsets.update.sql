UPDATE "Kafka"."IncomeOffsets" SET
    "Partition" = ?,
    "Offset"    = ?
WHERE   "Reader" = ?
    AND "Topic" = ?