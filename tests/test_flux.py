import threading, time
import pytest
from wcn_flux import (Channel, ShedPolicy, Pipeline, FifoScheduler,
                      BroadcastRouter, Full)

# ---- Channel / backpressure / shedding ----
def test_channel_basic_fifo():
    c=Channel(10)
    c.put(1); c.put(2); c.put(3)
    assert [c.get(),c.get(),c.get()]==[1,2,3]

def test_drop_new_when_full():
    c=Channel(2, shed=ShedPolicy.DROP_NEW)
    assert c.put("a") and c.put("b")
    assert c.put("c") is False        # dropped
    assert c.dropped==1
    assert len(c)==2
    assert c.get()=="a"               # oldest kept

def test_drop_old_when_full():
    c=Channel(2, shed=ShedPolicy.DROP_OLD)
    c.put("a"); c.put("b"); c.put("c")  # evicts "a"
    assert c.dropped==1
    assert c.get()=="b"               # newest kept

def test_raise_when_full():
    c=Channel(1, shed=ShedPolicy.RAISE)
    c.put("a")
    with pytest.raises(Full):
        c.put("b")

def test_block_applies_backpressure():
    c=Channel(1, shed=ShedPolicy.BLOCK)
    c.put("a")
    t0=time.monotonic()
    # blocked put with timeout returns False (couldn't get space)
    assert c.put("b", timeout=0.05) is False
    assert time.monotonic()-t0 >= 0.05

def test_block_unblocks_when_drained():
    c=Channel(1, shed=ShedPolicy.BLOCK)
    c.put("a")
    results=[]
    def producer(): results.append(c.put("b", timeout=1.0))
    th=threading.Thread(target=producer); th.start()
    time.sleep(0.02)
    assert c.get()=="a"               # make space
    th.join(timeout=1.0)
    assert results==[True]            # producer succeeded once space freed

def test_invalid_capacity():
    with pytest.raises(ValueError): Channel(0)

def test_get_timeout_returns_none():
    c=Channel(2)
    assert c.get(timeout=0.02) is None

# ---- Pipeline operators ----
def test_pipeline_map_filter():
    p=Pipeline().map(lambda x:x*2).filter(lambda x:x>4)
    assert p.run([1,2,3,4])==[6,8]    # 2,4,6,8 -> keep >4

def test_pipeline_flatmap():
    p=Pipeline().flatmap(lambda x:[x,x])
    assert p.run([1,2])==[1,1,2,2]

def test_pipeline_chain_order():
    p=Pipeline().filter(lambda x:x%2==0).map(lambda x:x+100)
    assert p.run([1,2,3,4,5,6])==[102,104,106]

# ---- Seams (the open-core extension points) ----
def test_custom_scheduler_seam():
    class ReverseSched(FifoScheduler):
        def order(self, items): return list(reversed(items))
    p=Pipeline(scheduler=ReverseSched()).flatmap(lambda x:[x,x+1]).map(lambda x:x)
    # reverse ordering inside stage application is exercised without error
    out=p.run([10])
    assert sorted(out)==[10,11]

def test_default_seams_present():
    p=Pipeline()
    assert isinstance(p.scheduler, FifoScheduler)
    assert isinstance(p.router, BroadcastRouter)
