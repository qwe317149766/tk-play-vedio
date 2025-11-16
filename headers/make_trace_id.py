from random import choice
import time

def make_x_tt_trace_id(stime,device_id:str=""):
    return (
            str("%x" % (round(time.time() * 1000) & 0xffffffff))
            + "10"
            + "".join(choice('0123456789abcdef') for _ in range(16))
            if not device_id
            else hex(int(device_id))[2:]
            + "".join(choice('0123456789abcdef') for _ in range(2))
            + "0"
            + hex(int("1233"))[2:]
        )
