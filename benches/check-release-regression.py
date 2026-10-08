#!/usr/bin/env python3
"""Block a repeatable public-API regression against crates.io amalgam-cache 0.3.1."""
import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess

EXPECTED={(kind,count) for kind in ['hot_read','hot_get_or_set'] for count in [1,8]}
EXPECTED|={(kind,1) for kind in ['set','cold','l2_read','l2_get_or_set']}

def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def identity(root):
    paths=[root/'Cargo.toml',root/'Cargo.lock',root/'.github/fixtures/release-regression.rs',root/'benches/scaling/warmup.rs',root/'benches/check-release-regression.py']
    paths+=sorted((root/'src').rglob('*.rs'))
    return {str(path.relative_to(root)):digest(path) for path in paths}
def execute(command,root,env,log):
    run=subprocess.run(command,cwd=root,env=env,capture_output=True,text=True)
    log.write_text(run.stdout); log.with_suffix(log.suffix+'.stderr').write_text(run.stderr)
    if run.returncode: raise SystemExit(run.stderr)
    return run

def build(root,output,label,toolchain,env):
    consumer=output/label; (consumer/'src').mkdir(parents=True,exist_ok=True)
    shutil.copyfile(root/'.github/fixtures/release-regression.rs',consumer/'src/main.rs')
    shutil.copyfile(root/'benches/scaling/warmup.rs',consumer/'src/warmup.rs')
    dependency='version = "=0.3.1"' if label=='baseline' else 'path = '+json.dumps(str(root))
    manifest='[package]\nname = "amalgam-release-regression-'+label+'"\nversion = "0.0.0"\nedition = "2024"\nrust-version = "1.88"\n[features]\nbaseline = []\n[dependencies]\namalgam = { package = "amalgam-cache", '+dependency+' }\ntokio = { version = "=1.52.3", features = ["rt", "sync", "time"] }\nserde_json = "=1.0.150"\n'
    (consumer/'Cargo.toml').write_text(manifest)
    command=['cargo','+'+toolchain,'build','--release','--manifest-path',str(consumer/'Cargo.toml'),'--message-format=json']
    if label=='baseline':command+=['--features','baseline']
    result=execute(command,root,env,output/(label+'-build.jsonl'))
    binaries=[json.loads(line)['executable'] for line in result.stdout.splitlines() if line.startswith('{') and json.loads(line).get('reason')=='compiler-artifact' and json.loads(line).get('executable')]
    if len(binaries)!=1: raise SystemExit('Expected exactly one benchmark executable')
    frozen=output/(label+'-benchmark'); shutil.copy2(binaries[0],frozen)
    metadata=execute(['cargo','+'+toolchain,'metadata','--format-version=1','--manifest-path',str(consumer/'Cargo.toml')],root,env,output/(label+'-metadata.json'))
    package=next(p for p in json.loads(metadata.stdout)['packages'] if p['name']=='amalgam-cache')
    if label=='baseline' and (package['version']!='0.3.1' or not package['source'].startswith('registry+')):
        raise SystemExit('Baseline must be the actual published registry package')
    return frozen,{'version':package['version'],'source':package['source'],'lock_sha256':digest(consumer/'Cargo.lock'),'binary_sha256':digest(frozen)}

def samples(text):
    rows={}
    for row in csv.DictReader(io.StringIO(text)):
        key=(row['scenario'],int(row['threads'])); ns=float(row['ns_per_op']); operations=int(row['operations'])
        if key in rows or not math.isfinite(ns) or ns<=0 or operations<=0: raise SystemExit('Invalid benchmark row')
        rows[key]={'ns':ns,'operations':operations}
    if rows.keys()!=EXPECTED: raise SystemExit('Missing or unexpected benchmark scenario')
    return rows

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pairs',type=int,default=3)
    parser.add_argument('--toolchain',default='1.88.0')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.pairs<3:parser.error('At least three counterbalanced pairs are required')
    root=Path(__file__).resolve().parents[1]; output=args.output.resolve()
    if output==root or root in output.parents:parser.error('Keep reports outside the checkout')
    output.mkdir(parents=True,exist_ok=True); env=dict(os.environ); env['CARGO_INCREMENTAL']='0'
    frozen=identity(root); binaries={}; packages={}
    for label in ['baseline','candidate']:binaries[label],packages[label]=build(root,output,label,args.toolchain,env)
    if identity(root)!=frozen:raise SystemExit('Sources changed during the build')
    trials={label:[] for label in binaries}; failures=[]
    for pair in range(args.pairs):
        for label in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
            result=execute([str(binaries[label])],root,env,output/f'pair-{pair+1}-{label}.csv')
            trials[label].append(samples(result.stdout))
            warmups=[json.loads(line.removeprefix('warmup ')) for line in result.stderr.splitlines() if line.startswith('warmup ')]
            expected_labels={f'hot:{str(kind).lower()}:{count}:{worker}' for kind in [False,True] for count in [1,8] for worker in range(count)}|{'set','cold','l2_read','l2_get_or_set'}
            if len(warmups)!=len(expected_labels) or {w['label'] for w in warmups}!=expected_labels:
                failures.append(f'{label} pair {pair+1}: incomplete warmup evidence')
            for w in warmups:
                values=w['windows_ns']
                stable=len(values)>=5 and max(values[-5:])/min(values[-5:])<=1.10
                if not w['stable'] or not stable or w['seconds']<3:failures.append(f'{label} pair {pair+1} {w["label"]}: unsettled warmup')
        if identity(root)!=frozen:raise SystemExit('Sources changed during measurement')
    rows=[]
    for key in sorted(EXPECTED):
        before=[t[key]['ns'] for t in trials['baseline']]; after=[t[key]['ns'] for t in trials['candidate']]
        operations={t[key]['operations'] for label in trials for t in trials[label]}
        if len(operations)!=1:raise SystemExit('Different timed workload counts')
        ratio=statistics.median(after)/statistics.median(before)
        row={'scenario':key[0],'threads':key[1],'baseline_ns':statistics.median(before),'candidate_ns':statistics.median(after),'candidate_over_031':ratio,'baseline_range':[min(before),max(before)],'candidate_range':[min(after),max(after)]}
        rows.append(row)
        if ratio>1.05:failures.append(f'{key}: {ratio:.3f}x published 0.3.1 exceeds 1.05 noise allowance')
    report={'baseline':'crates.io amalgam-cache 0.3.1','pairs':args.pairs,'noise_allowance':1.05,'platform':platform.platform(),'packages':packages,'sources_sha256':frozen,'measurements':rows,'failures':failures}
    (output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    for row in rows:print(f'{row["scenario"]:16} {row["threads"]} {row["candidate_over_031"]:.3f}x 0.3.1')
    if failures:raise SystemExit('\n'.join(failures))
if __name__=='__main__':main()
