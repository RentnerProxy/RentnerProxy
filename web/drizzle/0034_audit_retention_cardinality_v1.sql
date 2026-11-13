-- Generated from CustomMigrations/audit_retention_cardinality_v1.sql; sha256:7cd7f8a866775820ff11798a24cfcd5e4afe55aa0c1d4c983b3263d9ccd5e71d
LOCK TABLE "rentnerproxy"."audit_events" IN SHARE ROW EXCLUSIVE MODE;
--> statement-breakpoint
SELECT pg_advisory_xact_lock(7421908);
--> statement-breakpoint
INSERT INTO "rentnerproxy"."audit_retention_state" ("singleton", "row_count")
SELECT true, count(*) FROM "rentnerproxy"."audit_events";
--> statement-breakpoint
CREATE FUNCTION "rentnerproxy"."audit_events_lock_statement"() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(7421908);
    RETURN NULL;
END;
$$;
--> statement-breakpoint
CREATE FUNCTION "rentnerproxy"."audit_events_count_statement"() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    delta bigint;
BEGIN
    SELECT count(*) INTO delta FROM affected_rows;
    IF delta = 0 THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'DELETE' THEN
        delta := -delta;
    END IF;
    UPDATE "rentnerproxy"."audit_retention_state"
    SET "row_count" = "row_count" + delta WHERE "singleton";
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Audit retention cardinality state is missing';
    END IF;
    RETURN NULL;
END;
$$;
--> statement-breakpoint
CREATE FUNCTION "rentnerproxy"."audit_events_reset_count"() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE "rentnerproxy"."audit_retention_state" SET "row_count" = 0 WHERE "singleton";
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Audit retention cardinality state is missing';
    END IF;
    RETURN NULL;
END;
$$;
--> statement-breakpoint
CREATE TRIGGER "audit_events_lock_statement"
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON "rentnerproxy"."audit_events"
FOR EACH STATEMENT EXECUTE FUNCTION "rentnerproxy"."audit_events_lock_statement"();
--> statement-breakpoint
CREATE TRIGGER "audit_events_count_insert"
AFTER INSERT ON "rentnerproxy"."audit_events"
REFERENCING NEW TABLE AS affected_rows
FOR EACH STATEMENT EXECUTE FUNCTION "rentnerproxy"."audit_events_count_statement"();
--> statement-breakpoint
CREATE TRIGGER "audit_events_count_delete"
AFTER DELETE ON "rentnerproxy"."audit_events"
REFERENCING OLD TABLE AS affected_rows
FOR EACH STATEMENT EXECUTE FUNCTION "rentnerproxy"."audit_events_count_statement"();
--> statement-breakpoint
CREATE TRIGGER "audit_events_count_truncate"
AFTER TRUNCATE ON "rentnerproxy"."audit_events"
FOR EACH STATEMENT EXECUTE FUNCTION "rentnerproxy"."audit_events_reset_count"();
