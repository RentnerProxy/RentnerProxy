import assert from 'node:assert/strict'
import type { ManagedBackupSmokeStage } from './Types/managed-backup-diagnostics.types.ts'

export const MANAGED_BACKUP_SMOKE_STAGES = [
    'setup',
    'online-restore',
    'online-restore-health',
    'interrupt-probe',
    'interrupt-stop',
    'interrupt-exit',
    'interrupt-repair',
    'interrupt-start',
    'interrupt-health',
    'interrupt-recover',
    'cleanup',
] as const
const stages: ReadonlySet<string> = new Set(MANAGED_BACKUP_SMOKE_STAGES)
const fatalReasons = {
    'timed out waiting for web readiness within 60 seconds': 'web-readiness-timeout',
    'timed out waiting for controller recovery progress within 60 seconds':
        'controller-progress-startup-timeout',
    'controller maintenance has no exclusive owned process': 'controller-ownership',
    'controller maintenance could not be prepared': 'controller-prepare',
    'controller could not quiesce safely': 'controller-quiesce',
    'controller maintenance handoff could not be confirmed': 'controller-handoff',
    'an application or CrowdSec process survived backup quiescing': 'consumer-survivor',
    'backup exporter could not prove safe completion': 'exporter-completion',
    'managed restore failed; restart or explicit recovery is required': 'restore-execution',
    'maintenance web could not stop safely': 'restore-web-quiesce',
    'retained Caddy could not stop safely': 'retained-caddy-stop',
    'web could not quiesce safely for backup': 'backup-web-quiesce',
    'CrowdSec could not quiesce safely for backup': 'backup-crowdsec-quiesce',
    'CrowdSec could not quiesce safely for restore': 'restore-crowdsec-quiesce',
    'controller recovery progress stopped': 'controller-progress',
    'PostgreSQL remained unresponsive': 'postgres-health',
    'Valkey remained unresponsive': 'valkey-health',
    'backups remained unresponsive': 'backups-health',
    'web stopped unexpectedly': 'web-exit',
    'controller stopped unexpectedly': 'controller-exit',
    'backups stopped unexpectedly': 'backups-exit',
    'postgres stopped unexpectedly': 'postgres-exit',
    'valkey stopped unexpectedly': 'valkey-exit',
    'crowdsec stopped unexpectedly': 'crowdsec-exit',
    'a web writer survived isolated recovery': 'web-writer-survivor',
    'web recovery cannot prove the persisted maintenance fence safe': 'web-maintenance-fence',
} as const
const fatalCategories: ReadonlySet<string> = new Set([
    ...Object.values(fatalReasons),
    'unknown',
    'unavailable',
])

/** Classify only complete known supervisor fatal lines; incidental messages and log payloads never escape. */
export function managedBackupSupervisorDiagnostic(logs: string): string {
    let category = 'unknown'
    for (const line of logs.split(/\r?\n/u)) {
        for (const [reason, known] of Object.entries(fatalReasons)) {
            if (line === '[rentnerproxy] fatal: ' + reason) category = known
        }
    }
    return 'Managed backup supervisor fatal=' + category
}

/** Accept only fixed checkpoints, error categories and bounded locations in the three owned smoke sources. */
export function isManagedBackupSmokeDiagnostic(line: string): boolean {
    if (line.includes('\n') || line.includes('\r')) return false
    const fatal = /^Managed backup supervisor fatal=([a-z-]+)$/u.exec(line)
    if (fatal) return fatalCategories.has(fatal[1]!)
    if (
        /^Managed backup appliance unavailable$/u.test(line) ||
        /^Managed backup appliance running=(?:true|false) exitCode=(?:0|[1-9]\d?|1\d{2}|2[0-4]\d|25[0-5]) oomKilled=(?:true|false)$/u.test(
            line,
        )
    )
        return true
    if (
        /^Managed backup process exitCode=(?:null|0|[1-9]\d?|1\d{2}|2[0-4]\d|25[0-5]) timedOut=(?:true|false) stopped=(?:true|false)$/u.test(
            line,
        )
    )
        return true
    const match =
        /^Managed backup (?:stage=([^\s]+)|failure stage=([^\s]+) category=(?:assertion|timeout|command|other)(?: at \.github\/scripts\/ci\/(?:managed-backups-smoke|fixtures\/(?:managed-backup-probe|managed-restore-probe))\.ts:[1-9]\d{0,6}:[1-9]\d{0,6})?)$/u.exec(
            line,
        )
    return match !== null && stages.has((match[1] ?? match[2])!)
}

/** Classify failures without copying messages, assertion operands, absolute paths or unapproved stack frames. */
export function managedBackupFailureDiagnostic(
    stage: ManagedBackupSmokeStage,
    error: unknown,
): string {
    const category =
        error instanceof assert.AssertionError
            ? 'assertion'
            : error instanceof Error &&
                (error.name === 'TimeoutError' ||
                    error.name === 'AbortError' ||
                    error.message.startsWith('Managed backup smoke deadline exceeded:'))
              ? 'timeout'
              : error instanceof Error &&
                  error.message.startsWith('Managed backup smoke operation failed:')
                ? 'command'
                : 'other'
    const source =
        error instanceof Error
            ? /\.github[/\\]scripts[/\\]ci[/\\](?:managed-backups-smoke|fixtures[/\\](?:managed-backup-probe|managed-restore-probe))\.ts:[1-9]\d{0,6}:[1-9]\d{0,6}(?!\d)/u
                  .exec(error.stack ?? '')?.[0]
                  ?.replaceAll('\\', '/')
            : undefined
    const line =
        'Managed backup failure stage=' +
        (stages.has(stage) ? stage : 'setup') +
        ' category=' +
        category +
        (source ? ' at ' + source : '')
    return isManagedBackupSmokeDiagnostic(line)
        ? line
        : 'Managed backup failure stage=setup category=other'
}
