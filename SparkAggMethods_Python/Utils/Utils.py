import multiprocessing


def always_true(_):
    return True


def detectCPUs():
    return multiprocessing.cpu_count()


def int_divide_round_up(x, y):
    return (x + y - 1) // y
