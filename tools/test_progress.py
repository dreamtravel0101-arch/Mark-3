import importlib
pb = importlib.import_module('core.progress_bar')
print('Loaded:', hasattr(pb, 'SimpleProgress'), hasattr(pb, 'show_progress_line'))
sp = pb.SimpleProgress(100, prefix='TEST')
sp.set_stage('Testing')
sp.update(10, 100)
sp.done()
print('OK')
