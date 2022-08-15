from psutil import virtual_memory
import time


def mem():
    print(f"used memory : {round(virtual_memory()[3]/(1024*1024*1024)*10)/10}Go")


def stats(start_time):
    print("--- %s seconds ---" % (time.time() - start_time))
    mem()


def global_stats(global_start_time):
    print("--- %s seconds ---" % (time.time() - global_start_time))
    mem()
