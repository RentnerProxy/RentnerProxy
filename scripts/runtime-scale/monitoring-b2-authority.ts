// oxlint-disable no-await-in-loop -- Fixture DDL and publication share explicit ordered ownership fences.
import { SQL } from 'bun'
import { randomBytes, randomUUID } from 'node:crypto'
import { monitoringB2DatabaseUrl } from './monitoring-b2-isolation.ts'
import type { MonitoringB2RoleLifecycle } from './Types/monitoring-b2.types.ts'

/** Installs disposable authority on the real isolated application tables; never a production migration. */
export async function createMonitoringB2Authority(
    sharedDatabase?: SQL,
    roles?: MonitoringB2RoleLifecycle,
) {
    const databaseUrl = await monitoringB2DatabaseUrl()
    const suffix = randomUUID().replaceAll('-', '')
    const namespace = 'monitor_b2_' + suffix
    const role = 'monitor_b2_role_' + suffix
    const password = randomBytes(32).toString('hex')
    const storeId = randomUUID()
    const controllerEpoch = randomUUID()
    const database = sharedDatabase ?? new SQL(databaseUrl, { max: 2, connectionTimeout: 3 })
    const workerUrl = new URL(databaseUrl)
    workerUrl.username = role
    workerUrl.password = password
    const triggers: { table: string; name: string }[] = []
    let schemaCreated = false
    let roleCreated = false
    /** Attempt every owned trigger/schema/role cleanup; independent failures cannot suppress later attempts. */
    const cleanup = async () => {
        const failures: unknown[] = []
        for (const trigger of triggers) {
            try {
                await database.unsafe(
                    `DROP TRIGGER IF EXISTS ${trigger.name} ON rentnerproxy.${trigger.table}`,
                )
            } catch (error) {
                failures.push(error)
            }
        }
        if (schemaCreated)
            try {
                await database.unsafe(`DROP SCHEMA ${namespace} CASCADE`)
            } catch (error) {
                failures.push(error)
            }
        if (roleCreated)
            try {
                if (roles) await roles.drop(role)
                else await database.unsafe(`DROP ROLE IF EXISTS ${role}`)
            } catch (error) {
                failures.push(error)
            }
        if (!sharedDatabase) await database.close()
        if (failures.length) throw new AggregateError(failures, 'B2 fixture cleanup failed')
    }
    try {
        await database.unsafe(
            `CREATE SCHEMA ${namespace}; REVOKE ALL ON SCHEMA ${namespace} FROM PUBLIC`,
        )
        schemaCreated = true
        await database.unsafe(`
            CREATE TABLE ${namespace}.identity(id integer PRIMARY KEY CHECK(id=1), store_id text NOT NULL, epoch text NOT NULL, generation bigint NOT NULL DEFAULT 1, published_generation bigint NOT NULL DEFAULT 0, paused boolean NOT NULL DEFAULT true, resync_required boolean NOT NULL DEFAULT true);
            CREATE TABLE ${namespace}.contexts(policy_id uuid PRIMARY KEY, policy_version uuid NOT NULL, before jsonb NOT NULL);
            CREATE TABLE ${namespace}.admissions(lease_token uuid PRIMARY KEY,original_claim jsonb NOT NULL,admitted_ms bigint,probe_started_ms bigint,created_at timestamptz NOT NULL DEFAULT clock_timestamp());
            CREATE TABLE ${namespace}.raw_completed(lease_token uuid PRIMARY KEY,policy_id uuid NOT NULL,policy_version uuid NOT NULL,store_id text NOT NULL,epoch text NOT NULL,generation bigint NOT NULL,completed_at timestamptz NOT NULL DEFAULT clock_timestamp());
            CREATE TABLE ${namespace}.receipts(attempt_id uuid PRIMARY KEY, store_id text NOT NULL, epoch text NOT NULL, generation bigint NOT NULL, policy_id uuid NOT NULL, policy_version uuid NOT NULL, digest text NOT NULL, transaction_id text NOT NULL, disposition text NOT NULL DEFAULT 'candidate' CHECK(disposition IN ('candidate','rejected')), reason text, observation_id uuid,due_ms bigint,started_ms bigint,admitted_ms bigint,durable_ms bigint,completed_ms bigint,CHECK((disposition='rejected')=(reason IS NOT NULL)));
            CREATE TABLE ${namespace}.replay_pending(attempt_id uuid PRIMARY KEY,store_id text NOT NULL,epoch text NOT NULL,generation bigint NOT NULL,policy_id uuid NOT NULL,policy_version uuid NOT NULL,digest text NOT NULL,due_ms bigint,started_ms bigint,admitted_ms bigint,durable_ms bigint,completed_ms bigint);
            CREATE FUNCTION ${namespace}.observation_receipt() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
            DECLARE pending ${namespace}.replay_pending%ROWTYPE;
            BEGIN
                SELECT r.* INTO pending FROM ${namespace}.replay_pending r JOIN rentnerproxy.host_check_policies p ON p.id=r.policy_id AND p.version=r.policy_version AND p.lease_token=r.attempt_id WHERE r.policy_id=NEW.policy_id AND r.policy_version=NEW.policy_version;
                IF NOT FOUND THEN RETURN NEW; END IF;
                IF NOT EXISTS(SELECT 1 FROM ${namespace}.admissions a WHERE a.lease_token=pending.attempt_id AND a.admitted_ms=pending.admitted_ms AND a.original_claim @> jsonb_build_object('storeId',pending.store_id,'controllerEpoch',pending.epoch,'generation',pending.generation,'policyId',pending.policy_id,'policyVersion',pending.policy_version)) THEN RAISE EXCEPTION 'B2 replay original admission mismatch'; END IF;
                PERFORM 1 FROM ${namespace}.identity i WHERE i.id=1 AND i.store_id=pending.store_id AND i.epoch=pending.epoch AND i.generation=pending.generation AND i.generation=i.published_generation AND NOT i.paused AND NOT i.resync_required FOR SHARE;
                IF NOT FOUND THEN RAISE EXCEPTION 'B2 stale replay identity'; END IF;
                INSERT INTO ${namespace}.receipts(attempt_id,store_id,epoch,generation,policy_id,policy_version,digest,transaction_id,observation_id,due_ms,started_ms,admitted_ms,durable_ms,completed_ms)
                VALUES(pending.attempt_id,pending.store_id,pending.epoch,pending.generation,pending.policy_id,pending.policy_version,pending.digest,mod(pg_current_xact_id()::text::numeric,4294967296)::text,NEW.id,pending.due_ms,pending.started_ms,pending.admitted_ms,pending.durable_ms,pending.completed_ms);
                RETURN NEW;
            END $$;
            REVOKE ALL ON FUNCTION ${namespace}.observation_receipt() FROM PUBLIC;
            CREATE FUNCTION ${namespace}.invalidate() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
            BEGIN
                IF TG_TABLE_NAME = 'system_settings' THEN
                    IF TG_OP='DELETE' THEN
                        IF OLD.key NOT LIKE 'proxy_runtime_%' AND OLD.key NOT IN ('default_site_v1','docker_discovery_management_v1') THEN RETURN NULL; END IF;
                    ELSIF TG_OP='INSERT' THEN
                        IF NEW.key NOT LIKE 'proxy_runtime_%' AND NEW.key NOT IN ('default_site_v1','docker_discovery_management_v1') THEN RETURN NULL; END IF;
                    ELSE
                        IF OLD.key NOT LIKE 'proxy_runtime_%' AND OLD.key NOT IN ('default_site_v1','docker_discovery_management_v1') AND NEW.key NOT LIKE 'proxy_runtime_%' AND NEW.key NOT IN ('default_site_v1','docker_discovery_management_v1') THEN RETURN NULL; END IF;
                    END IF;
                END IF;
                UPDATE ${namespace}.identity SET generation=generation+1,resync_required=true WHERE id=1;
                RETURN NULL;
            END $$;
            REVOKE ALL ON FUNCTION ${namespace}.invalidate() FROM PUBLIC;
        `)
        roleCreated = true
        if (roles) await roles.create(role, password)
        else
            await database.unsafe(
                `CREATE ROLE ${role} LOGIN PASSWORD '${password}' CONNECTION LIMIT 2 NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT`,
            )
        await database.unsafe(
            `INSERT INTO ${namespace}.identity(id,store_id,epoch) VALUES(1,$1,$2)`,
            [storeId, controllerEpoch],
        )
        // Full private host proof includes jobs/discovery/settings; snapshot revision includes access policies and CAs.
        // Conservative invalidation blocks *all* new claims until Bun republishes after any such committed edit.
        for (const table of [
            'proxy_hosts',
            'redirect_hosts',
            'host_domains',
            'access_policies',
            'access_policy_basic_auth_accounts',
            'trusted_cas',
            'certificates',
            'certificate_domains',
            'certificate_binding_jobs',
            'system_settings',
            'users',
            'roles',
            'permissions',
            'role_permissions',
            'user_roles',
        ]) {
            const name = 'b2_' + suffix.slice(0, 12) + '_' + table
            await database.unsafe(
                `CREATE TRIGGER ${name} AFTER INSERT OR UPDATE OR DELETE ON rentnerproxy.${table} FOR EACH ROW EXECUTE FUNCTION ${namespace}.invalidate()`,
            )
            triggers.push({ table, name })
        }
        const policyTrigger = 'b2_' + suffix + '_policy'
        await database.unsafe(
            `CREATE TRIGGER ${policyTrigger} AFTER INSERT OR DELETE OR UPDATE OF version,enabled,domain,proxy_host_id,interval_seconds,timeout_ms,expected_status,failure_threshold,recovery_threshold ON rentnerproxy.host_check_policies FOR EACH ROW EXECUTE FUNCTION ${namespace}.invalidate()`,
        )
        triggers.push({ table: 'host_check_policies', name: policyTrigger })
        const receiptTrigger = 'b2_' + suffix.slice(0, 12) + '_receipt'
        await database.unsafe(
            `CREATE TRIGGER ${receiptTrigger} AFTER INSERT ON rentnerproxy.host_check_observations FOR EACH ROW EXECUTE FUNCTION ${namespace}.observation_receipt()`,
        )
        triggers.push({ table: 'host_check_observations', name: receiptTrigger })
        const { HOST_MONITORING_LIMITS } =
            await import('../../web/src/config/host-monitoring.config.ts')
        await database.unsafe(`
            CREATE FUNCTION ${namespace}.claim(requested integer, expected_store text, expected_epoch text) RETURNS TABLE(claim jsonb)
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
            DECLARE available integer; current_identity ${namespace}.identity%ROWTYPE;
            BEGIN
                IF requested IS NULL OR requested < 1 OR requested > 16 THEN RAISE EXCEPTION 'B2 claim bound'; END IF;
                IF expected_store IS NULL OR expected_epoch IS NULL THEN RETURN; END IF;
                PERFORM set_config('statement_timeout','2000ms',true);
                PERFORM set_config('lock_timeout','2000ms',true);
                PERFORM set_config('transaction_timeout','2000ms',true);
                PERFORM 1 FROM rentnerproxy.system_settings WHERE key='proxy_runtime_editor_v1' FOR UPDATE;
                PERFORM pg_advisory_xact_lock(${HOST_MONITORING_LIMITS.advisoryLockId});
                SELECT * INTO current_identity FROM ${namespace}.identity WHERE id=1 FOR SHARE;
                IF NOT FOUND OR current_identity.store_id IS DISTINCT FROM expected_store OR current_identity.epoch IS DISTINCT FROM expected_epoch OR current_identity.paused OR current_identity.resync_required OR current_identity.generation <> current_identity.published_generation THEN RETURN; END IF;
                DELETE FROM ${namespace}.admissions WHERE created_at<=clock_timestamp()-interval '120 seconds';
                DELETE FROM ${namespace}.raw_completed c WHERE c.completed_at<clock_timestamp()-interval '120 seconds' OR NOT EXISTS(SELECT 1 FROM rentnerproxy.host_check_policies p WHERE p.id=c.policy_id AND p.lease_token=c.lease_token);
                SELECT greatest(64-count(*),0)::integer INTO available FROM rentnerproxy.host_check_policies p WHERE p.lease_expires_at > clock_timestamp() AND NOT EXISTS(SELECT 1 FROM ${namespace}.raw_completed c WHERE c.policy_id=p.id AND c.lease_token=p.lease_token AND c.store_id=current_identity.store_id AND c.epoch=current_identity.epoch);
                available:=least(available,greatest(4096-(SELECT count(*) FROM ${namespace}.admissions),0)::integer);
                RETURN QUERY WITH candidates AS (
                    SELECT p.id,p.next_check_at FROM rentnerproxy.host_check_policies p
                    JOIN rentnerproxy.proxy_hosts h ON h.id=p.proxy_host_id AND h.enabled
                    JOIN rentnerproxy.host_domains d ON d.proxy_host_id=h.id AND d.domain=p.domain
                    JOIN ${namespace}.contexts c ON c.policy_id=p.id AND c.policy_version=p.version
                    WHERE p.enabled AND p.next_check_at <= clock_timestamp() AND (p.lease_expires_at IS NULL OR p.lease_expires_at <= clock_timestamp())
                    ORDER BY p.next_check_at,p.id LIMIT least(requested,available) FOR UPDATE OF p SKIP LOCKED
                ), admitted AS (
                    UPDATE rentnerproxy.host_check_policies p SET lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '45 seconds'
                    FROM candidates x WHERE p.id=x.id
                    RETURNING p.id,p.version,p.lease_token,p.lease_expires_at,p.proxy_host_id,p.domain,p.timeout_ms,x.next_check_at
                ), encoded AS (SELECT a.lease_token,jsonb_build_object('storeId',current_identity.store_id,'controllerEpoch',current_identity.epoch,'generation',current_identity.generation,
                    'policyId',a.id,'policyVersion',a.version,'leaseToken',a.lease_token,'leaseExpiresAtMs',floor(extract(epoch FROM a.lease_expires_at)*1000)::bigint,
                    'dueAtMs',floor(extract(epoch FROM a.next_check_at)*1000)::bigint,'hostId',a.proxy_host_id,'domain',a.domain,'timeoutMs',a.timeout_ms,'before',c.before)
                    AS original_claim FROM admitted a JOIN ${namespace}.contexts c ON c.policy_id=a.id ORDER BY a.next_check_at,a.id)
                    INSERT INTO ${namespace}.admissions AS captured(lease_token,original_claim) SELECT e.lease_token,e.original_claim FROM encoded e RETURNING captured.original_claim;
            END $$;
            REVOKE ALL ON FUNCTION ${namespace}.claim(integer,text,text) FROM PUBLIC;
            CREATE FUNCTION ${namespace}.begin_check(expected_policy uuid,expected_version uuid,expected_token uuid,expected_store text,expected_epoch text,expected_generation bigint) RETURNS bigint
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
            DECLARE admitted bigint;
            BEGIN
                IF expected_policy IS NULL OR expected_version IS NULL OR expected_token IS NULL OR expected_store IS NULL OR expected_epoch IS NULL OR expected_generation IS NULL OR expected_generation<1 THEN RETURN NULL; END IF;
                PERFORM set_config('statement_timeout','2000ms',true);
                PERFORM set_config('lock_timeout','2000ms',true);
                PERFORM set_config('transaction_timeout','2000ms',true);
                PERFORM 1 FROM rentnerproxy.system_settings WHERE key='proxy_runtime_editor_v1' FOR SHARE;
                PERFORM pg_advisory_xact_lock(${HOST_MONITORING_LIMITS.advisoryLockId});
                SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint INTO admitted
                FROM ${namespace}.identity i
                JOIN rentnerproxy.host_check_policies p ON p.id=expected_policy AND p.version=expected_version AND p.lease_token=expected_token AND p.lease_expires_at>clock_timestamp() AND p.enabled
                JOIN rentnerproxy.proxy_hosts h ON h.id=p.proxy_host_id AND h.enabled
                JOIN rentnerproxy.host_domains d ON d.proxy_host_id=h.id AND d.domain=p.domain
                JOIN ${namespace}.contexts c ON c.policy_id=p.id AND c.policy_version=p.version
                JOIN ${namespace}.admissions a ON a.lease_token=p.lease_token
                WHERE i.id=1 AND i.store_id=expected_store AND i.epoch=expected_epoch AND i.generation=expected_generation AND i.generation=i.published_generation AND NOT i.paused AND NOT i.resync_required AND NOT EXISTS(SELECT 1 FROM ${namespace}.raw_completed completed WHERE completed.lease_token=p.lease_token)
                AND a.original_claim @> jsonb_build_object('storeId',expected_store,'controllerEpoch',expected_epoch,'generation',expected_generation,'policyId',p.id,'policyVersion',p.version,'leaseToken',p.lease_token,'hostId',p.proxy_host_id,'domain',p.domain,'timeoutMs',p.timeout_ms,'leaseExpiresAtMs',floor(extract(epoch FROM p.lease_expires_at)*1000)::bigint,'before',c.before)
                AND a.original_claim->'before'=c.before
                FOR SHARE OF i,p,h,d,c,a;
                IF admitted IS NULL THEN RETURN NULL; END IF;
                UPDATE ${namespace}.admissions SET admitted_ms=coalesce(admitted_ms,admitted) WHERE lease_token=expected_token RETURNING admitted_ms INTO admitted;
                RETURN admitted;
            END $$;
            REVOKE ALL ON FUNCTION ${namespace}.begin_check(uuid,uuid,uuid,text,text,bigint) FROM PUBLIC;
            CREATE FUNCTION ${namespace}.record_raw_complete(expected_policy uuid,expected_version uuid,expected_token uuid,expected_store text,expected_epoch text,expected_generation bigint) RETURNS boolean
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
            BEGIN
                IF expected_policy IS NULL OR expected_version IS NULL OR expected_token IS NULL OR expected_store IS NULL OR expected_epoch IS NULL OR expected_generation IS NULL OR expected_generation<1 THEN RETURN false; END IF;
                PERFORM set_config('statement_timeout','2000ms',true);
                PERFORM set_config('lock_timeout','2000ms',true);
                PERFORM set_config('transaction_timeout','2000ms',true);
                PERFORM 1 FROM rentnerproxy.system_settings WHERE key='proxy_runtime_editor_v1' FOR SHARE;
                PERFORM pg_advisory_xact_lock(${HOST_MONITORING_LIMITS.advisoryLockId});
                IF NOT EXISTS(SELECT 1 FROM ${namespace}.identity i JOIN rentnerproxy.host_check_policies p ON p.id=expected_policy AND p.version=expected_version AND p.lease_token=expected_token AND p.lease_expires_at>clock_timestamp() WHERE i.id=1 AND i.store_id=expected_store AND i.epoch=expected_epoch AND i.generation=expected_generation AND i.generation=i.published_generation AND NOT i.paused AND NOT i.resync_required) THEN RETURN false; END IF;
                IF NOT EXISTS(SELECT 1 FROM ${namespace}.admissions a WHERE a.lease_token=expected_token AND a.admitted_ms IS NOT NULL AND a.original_claim @> jsonb_build_object('storeId',expected_store,'controllerEpoch',expected_epoch,'generation',expected_generation,'policyId',expected_policy,'policyVersion',expected_version)) THEN RETURN false; END IF;
                IF EXISTS(SELECT 1 FROM ${namespace}.raw_completed WHERE lease_token=expected_token) THEN RETURN true; END IF;
                DELETE FROM ${namespace}.raw_completed c WHERE c.completed_at<clock_timestamp()-interval '120 seconds' OR NOT EXISTS(SELECT 1 FROM rentnerproxy.host_check_policies p WHERE p.id=c.policy_id AND p.lease_token=c.lease_token);
                IF (SELECT count(*) FROM ${namespace}.raw_completed)>=4096 THEN RETURN false; END IF;
                INSERT INTO ${namespace}.raw_completed(lease_token,policy_id,policy_version,store_id,epoch,generation) VALUES(expected_token,expected_policy,expected_version,expected_store,expected_epoch,expected_generation);
                RETURN true;
            END $$;
            REVOKE ALL ON FUNCTION ${namespace}.record_raw_complete(uuid,uuid,uuid,text,text,bigint) FROM PUBLIC;
            GRANT USAGE ON SCHEMA ${namespace} TO ${role};
            GRANT EXECUTE ON FUNCTION ${namespace}.claim(integer,text,text) TO ${role};
            GRANT EXECUTE ON FUNCTION ${namespace}.begin_check(uuid,uuid,uuid,text,text,bigint) TO ${role};
            GRANT EXECUTE ON FUNCTION ${namespace}.record_raw_complete(uuid,uuid,uuid,text,text,bigint) TO ${role};
        `)
    } catch (error) {
        try {
            await cleanup()
        } catch (cleanupError) {
            throw new AggregateError([error, cleanupError], 'B2 setup and cleanup failed', {
                cause: cleanupError,
            })
        }
        throw error
    }
    return {
        namespace,
        storeId,
        controllerEpoch,
        workerUrl: workerUrl.toString(),
        database,
        /** Publishes canonical Bun proofs under the same configuration and admission locks as execution. */
        async publish(reactivate = false) {
            const [
                { getAuthDatabase },
                { hostCheckPolicies },
                { sql },
                { prepareWorkerTransaction },
                { readHostMonitoringContextsInTransaction },
            ] = await Promise.all([
                import('../../web/src/server/Auth/Core/database.server.ts'),
                import('../../web/src/db/schema.ts'),
                import('drizzle-orm'),
                import('../../web/src/server/HostMonitoring/host-monitoring.storage.ts'),
                import('../../web/src/server/RuntimeDiagnostics/domain-diagnostics-data.ts'),
            ])
            return getAuthDatabase().transaction(async (transaction) => {
                await prepareWorkerTransaction(transaction)
                await transaction.execute(
                    sql.raw(`SELECT id FROM ${namespace}.identity WHERE id=1 FOR UPDATE`),
                )
                const policies = await transaction.select().from(hostCheckPolicies)
                const contexts = await readHostMonitoringContextsInTransaction(
                    transaction,
                    policies.map((policy) => ({
                        hostType: 'proxy' as const,
                        hostId: policy.proxyHostId,
                        domain: policy.domain,
                    })),
                )
                await transaction.execute(sql.raw(`DELETE FROM ${namespace}.contexts`))
                const publication = policies.flatMap((policy) => {
                    const before = contexts.get(policy.proxyHostId)
                    return before?.enabled && policy.enabled
                        ? [{ policy_id: policy.id, policy_version: policy.version, before }]
                        : []
                })
                if (publication.length)
                    await transaction.execute(
                        sql`INSERT INTO ${sql.identifier(namespace)}.contexts(policy_id,policy_version,before) SELECT entry.policy_id::uuid,entry.policy_version::uuid,entry.before FROM jsonb_to_recordset(${JSON.stringify(publication)}::text::jsonb) AS entry(policy_id text,policy_version text,before jsonb)`,
                    )
                await transaction.execute(
                    sql`UPDATE ${sql.identifier(namespace)}.identity SET published_generation=generation,resync_required=false,paused=CASE WHEN ${reactivate} THEN false ELSE paused END WHERE id=1`,
                )
                return policies.length
            })
        },
        /** Fences reset authority; the caller must separately stop/drain native and replay writers before replacing state or keys. */
        async resetIdentity() {
            const nextStore = randomUUID(),
                nextEpoch = randomUUID()
            const changed = await database.unsafe(
                `UPDATE ${namespace}.identity SET paused=true,resync_required=true,generation=generation+1,store_id=$1,epoch=$2 WHERE id=1 RETURNING id`,
                [nextStore, nextEpoch],
            )
            if (changed.length !== 1) throw new Error('B2 reset identity missing')
            return { storeId: nextStore, controllerEpoch: nextEpoch }
        },
        cleanup,
    }
}
