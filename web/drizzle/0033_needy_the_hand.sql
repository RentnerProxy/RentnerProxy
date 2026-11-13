CREATE TABLE "rentnerproxy"."audit_retention_state" (
	"singleton" boolean PRIMARY KEY DEFAULT true NOT NULL,
	"row_count" bigint NOT NULL,
	CONSTRAINT "audit_retention_state_singleton_check" CHECK ("rentnerproxy"."audit_retention_state"."singleton"),
	CONSTRAINT "audit_retention_state_row_count_check" CHECK ("rentnerproxy"."audit_retention_state"."row_count" >= 0)
);
