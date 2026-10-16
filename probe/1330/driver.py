"""Serial ten-run exit probe; only synthetic data, no host CLI or credentials."""
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import time

worktree = Path(__file__).resolve().parents[2]
output = Path(sys.argv[1]).resolve()
output.mkdir(parents=True, exist_ok=True)
(output / 'versions.json').write_text(json.dumps({
    'python': sys.version, 'platform': platform.platform(), 'machine': platform.machine(),
    'sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=worktree, text=True).strip(),
    'packages': {name: importlib.metadata.version(name) for name in ('fastembed', 'onnxruntime', 'numpy', 'py-spy')},
    'worker_capacity': 4, 'onnx_threads': 2, 'host_sha': 'd923a5eb487a17c0a68801bba9ef0f56c81040a8',
    'boundary': 'engine-only with snapshotted host lifecycle and empty external-resource fixtures',
}, indent=2))
rows = []
for variant in ('normal', 'off'):
    for repeat in range(1, 6):
        run = output / f'{variant}-{repeat}'
        run.mkdir()
        stdout = run / 'stdout.txt'
        stderr = run / 'stderr.txt'
        start = time.monotonic()
        with stdout.open('w') as out, stderr.open('w') as err:
            proc = subprocess.Popen(['nice', '-n', '19', sys.executable, '-B',
                str(worktree / 'probe/1330/repro.py'), '--worktree', str(worktree),
                '--run-dir', str(run), '--variant', variant], cwd=run, stdout=out, stderr=err,
                env={**os.environ, 'OMP_NUM_THREADS': '2', 'OPENBLAS_NUM_THREADS': '2',
                    'TOKENIZERS_PARALLELISM': 'false', 'PYTHONDONTWRITEBYTECODE': '1'})
            turn_end = None
            captured = False
            killed = False
            while proc.poll() is None:
                marker = run / 'turn-end.json'
                if turn_end is None and marker.exists():
                    try:
                        turn_end = json.loads(marker.read_text())['monotonic']
                    except json.JSONDecodeError:
                        pass
                now = time.monotonic()
                if turn_end is not None and now - turn_end >= 10 and not captured:
                    captured = True
                    with (run / 'py-spy-native.txt').open('w') as stack:
                        try:
                            sample = subprocess.run(['sudo', '-n', shutil.which('py-spy'),
                                'dump', '--native', '--pid', str(proc.pid)],
                                stdout=stack, stderr=subprocess.STDOUT, timeout=8)
                            stack.write(f'\nPY_SPY_RC={sample.returncode}\n')
                        except subprocess.TimeoutExpired:
                            stack.write('\nPY_SPY_TIMEOUT\n')
                    if proc.poll() is None:
                        os.kill(proc.pid, signal.SIGUSR1)
                if now - (turn_end or start) >= 150:
                    killed = True
                    proc.kill()
                    break
                time.sleep(0.02)
            rc = proc.wait()
            end = time.monotonic()
        if turn_end is None and (run / 'turn-end.json').exists():
            turn_end = json.loads((run / 'turn-end.json').read_text())['monotonic']
        # Cores stay raw in artifact; gdb can identify an early SIGABRT before t+10.
        for core in run.glob('core*'):
            if core.is_file():
                with (run / (core.name + '-native.txt')).open('w') as stack:
                    try:
                        subprocess.run(['gdb', '-q', '-batch', '-ex', 'set pagination off',
                            '-ex', 'thread apply all bt', sys.executable, str(core)],
                            stdout=stack, stderr=subprocess.STDOUT, timeout=30)
                    except subprocess.TimeoutExpired:
                        stack.write('\nGDB_TIMEOUT\n')
        lines = stderr.read_text(errors='replace').splitlines()
        row = {'keys': 'on' if variant == 'normal' else 'off', 'repeat': repeat,
            'exit_wall_s': None if turn_end is None else round(end - turn_end, 4),
            'rc': rc, 'stderr_last_line': lines[-1] if lines else '',
            'native_capture_at_10s': captured, 'driver_killed_at_150s': killed,
            'turn_marker_present': turn_end is not None, 'receipt': run.name}
        rows.append(row)
        (output / 'measurements.json').write_text(json.dumps(rows, indent=2))
        print(json.dumps(row), flush=True)
if any(not row['turn_marker_present'] for row in rows):
    sys.exit('PROBE_INVALID: a run never reached turn end')
