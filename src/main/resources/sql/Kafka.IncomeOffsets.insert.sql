INSERT INTO "Kafka"."IncomeOffsets" ("Reader", "Topic", "Partition", "Offset")
VALUES (?, ?, ?, ?)
ON CONFLICT ("Reader", "Topic", "Partition") DO UPDATE SET
    "Offset" = EXCLUDED."Offset";