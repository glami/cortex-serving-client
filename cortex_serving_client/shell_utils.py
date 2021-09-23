import psutil


def kill_process_with_children(parent_pid):
    parent = psutil.Process(parent_pid)
    for child in parent.children(recursive=True):
        child.kill()
    parent.kill()