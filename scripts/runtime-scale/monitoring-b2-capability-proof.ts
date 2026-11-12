import { lstat } from 'node:fs/promises'

export interface MonitoringB2CapabilityProof {
    passed: boolean
    evidence: Record<string, string | number | boolean | null>
    failures: string[]
}

/** Negative calls run as the actual native Unix role, with only its probe key, never a broad/root/web key. */
export async function runMonitoringB2CapabilityProof(
    probeKey: string,
): Promise<MonitoringB2CapabilityProof> {
    if (
        process.platform !== 'linux' ||
        process.getuid?.() !== 0 ||
        process.env.RENTNERPROXY_MONITORING_B2 !== '1' ||
        !/^[a-f0-9]{64}$/u.test(probeKey)
    )
        throw new Error('B2 capability proof requires the root Linux fixture')
    for (const path of [
        '/run/rentnerproxy/app-key/value',
        '/run/rentnerproxy/database-url/value',
        '/run/rentnerproxy/controller-token/value',
        '/tmp/monitoring-b2/control.cap',
        '/tmp/monitoring-b2/web.cap',
    ])
        if (!(await lstat(path)).isFile())
            throw new Error('B2 capability proof secret fixture missing')
    const source = `import {readFile} from 'node:fs/promises';
const input=JSON.parse(await Bun.stdin.text());
const evidence={nativeUID:process.getuid(),nativeGID:process.getgid(),nativeGroups:[...new Set([process.getgid(),...process.getgroups()])].toSorted((a,b)=>a-b).join(',')};
const files=await Promise.all(['/run/rentnerproxy/app-key/value','/run/rentnerproxy/database-url/value','/run/rentnerproxy/controller-token/value','/tmp/monitoring-b2/control.cap','/tmp/monitoring-b2/web.cap'].map(async path=>{try{await readFile(path);return 'READABLE'}catch(error){return ['EACCES','EPERM'].includes(error.code)?error.code:'UNPROVEN'}}));
const denied=code=>code==='EACCES'||code==='EPERM';
evidence.appSecretReadRejected=denied(files[0])&&denied(files[1]);evidence.controllerSecretReadRejected=denied(files[2]);
evidence.appSecretErrno=files[0];evidence.controllerSecretErrno=files[2];evidence.rootCapErrno=files[3];evidence.webCapErrno=files[4];
evidence.nativeGroupsRestricted=evidence.nativeUID===20004&&evidence.nativeGID===20004&&evidence.nativeGroups==='20004';
const call=async(port,path,method='GET',key=input.probeKey,body)=>{const response=await fetch('http://127.0.0.1:'+port+path,{method,headers:{authorization:'Bearer '+key,'content-type':'application/json','x-b2-attempt':'00000000-0000-0000-0000-000000000001'},body:method==='GET'?undefined:JSON.stringify(body??{}),redirect:'error',signal:AbortSignal.timeout(2000)});await response.body?.cancel();return response.status};
const paths=[['/internal/v1/proxy/config','PUT'],['/internal/v1/certificates','GET'],['/internal/v1/dns/connections','GET'],['/internal/v1/backups/status','GET'],['/internal/v1/proxy/status','GET']];
const [broker,direct,root,oldRoot,web,drain]=await Promise.all([Promise.all(paths.map(([path,method])=>call(9024,path,method))),Promise.all(paths.map(([path,method])=>call(8081,path,method))),call(9020,'/report'),call(9020,'/report','GET','monitoring-b2-fixture-only'),call(9022,'/pause','POST'),call(9020,'/quiesce','POST')]);
evidence.brokerManagementDenials=broker.filter(code=>code===403).length;evidence.directManagementDenials=direct.filter(code=>code===401||code===403).length;
evidence.managementCapabilityRejected=evidence.brokerManagementDenials===5&&evidence.directManagementDenials===5;
evidence.probeTokenRejectedByController=direct.every(code=>code===401||code===403);
evidence.rootControlStatus=root;evidence.webControlStatus=web;evidence.rootControlRejected=root===403&&oldRoot===403&&drain===403;evidence.webControlRejected=web===403;
const interactive=await call(9024,'/internal/v1/diagnostics/domain','POST',input.probeKey,{hostType:'proxy',hostId:'00000000-0000-0000-0000-000000000001',domain:'fixture.invalid',expectedRevision:'sha256:'+('0'.repeat(64)),upstreamOnly:false,timeoutMs:3000});
evidence.interactiveProbeRejected=interactive===400;
process.stdout.write(JSON.stringify(evidence));`
    const child = Bun.spawn(
        ['gosu', 'monitoring-b2', process.execPath, '--no-env-file', '-e', source],
        {
            env: { PATH: process.env.PATH ?? '/usr/local/bin:/usr/bin:/bin' },
            stdin: new TextEncoder().encode(JSON.stringify({ probeKey })),
            stdout: 'pipe',
            stderr: 'pipe',
        },
    )
    const timeout = Bun.sleep(10000).then(() => null)
    const result = await Promise.race([
        Promise.all([
            child.exited,
            new Response(child.stdout).text(),
            new Response(child.stderr).text(),
        ]),
        timeout,
    ])
    if (result === null) {
        child.kill('SIGKILL')
        await child.exited
        return { passed: false, evidence: {}, failures: ['native-capability-subject-timeout'] }
    }
    if (result[0] !== 0 || result[1].length > 8192)
        return { passed: false, evidence: {}, failures: ['native-capability-subject-failed'] }
    const evidence = JSON.parse(result[1]) as Record<string, string | number | boolean | null>
    const failures = Object.entries(evidence)
        .filter(([, value]) => value === false)
        .map(([key]) => key + '-not-proven')
    for (const key of [
        'appSecretReadRejected',
        'controllerSecretReadRejected',
        'rootControlRejected',
        'webControlRejected',
        'managementCapabilityRejected',
        'probeTokenRejectedByController',
        'nativeGroupsRestricted',
        'interactiveProbeRejected',
    ])
        if (evidence[key] !== true && !failures.includes(key + '-not-proven'))
            failures.push(key + '-not-proven')
    if (
        !['EACCES', 'EPERM'].includes(String(evidence.rootCapErrno)) ||
        !['EACCES', 'EPERM'].includes(String(evidence.webCapErrno))
    )
        failures.push('privileged-capability-file-readable')
    return { passed: failures.length === 0, evidence, failures }
}
