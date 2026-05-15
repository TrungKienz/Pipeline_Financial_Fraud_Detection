import psutil, os

targets = ['logical_sources', 'pipeline_test_set.csv']
for proc in psutil.process_iter(['pid', 'name']):
    try:
        for f in proc.open_files():
            for t in targets:
                if t in f.path:
                    name = proc.info['name']
                    pid = proc.info['pid']
                    print(f'{name} (PID {pid}): {f.path}')
    except (psutil.AccessDenied, psutil.NoSuchProcess):
        pass
