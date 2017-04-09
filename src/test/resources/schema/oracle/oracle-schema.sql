CREATE TABLE "journal" (
  "ordering" NUMERIC NOT NULL,
  "deleted" char check ("deleted" in (0,1)),
  "persistence_id" VARCHAR(255) NOT NULL,
  "sequence_number" NUMERIC NOT NULL,
  "tags" VARCHAR(255) DEFAULT NULL,
  "message" BLOB NOT NULL,
  PRIMARY KEY("persistence_id", "sequence_number")
)
/

CREATE TABLE "snapshot" (
  "persistence_id" VARCHAR(255) NOT NULL,
  "sequence_number" NUMERIC NOT NULL,
  "created" NUMERIC NOT NULL,
  "snapshot" BLOB NOT NULL,
  PRIMARY KEY ("persistence_id", "sequence_number")
)
/