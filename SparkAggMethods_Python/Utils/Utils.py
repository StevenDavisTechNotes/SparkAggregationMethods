import multiprocessing


def always_true(_):
    return True



def detectCPUs():
    return multiprocessing.cpu_count()