```
[ec2-user@ip-Xbigobj]$ # ==============================================================
# PERF TEST 1: 4KB (10K keys to exceed controller cache)
# ==============================================================
~/valkey/src/valkey-cli -p 6399 SHUTDOWN NOSAVE 2>/dev/null; sleep 1
taskset -c 0-31 ~/valkey/src/valkey-server --port 6399 \
  --loadmodule ~/bigobj/target/release/libvalkey_bigobj.so \
  data-dir /mnt/bigobj-data pool-buf-size 4096 pool-buf-count 5000 \
  --daemonize yes --logfile /tmp/bigobj.log --save ""
sleep 2

python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 6399))
for i in range(10000):
    key = f'key:{i:012d}'
    val = 'B' * 4096
    cmd = f'*3\r\n\$6\r\nBO.SET\r\n\${len(key)}\r\n{key}\r\n\${len(val)}\r\n{val}\r\n'
    s.sendall(cmd.encode())
import time; time.sleep(2)
s.recv(65536); s.close()
print('Loaded 10000 x 4KB')
"
sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

taskset -c 32-63 ~/valkey/src/valkey-benchmark -p 6399 -c 750 --duration 60 -r 10000 BO.GET key:__rand_int__ &
sleep 5
top -H -p $(pgrep valkey-server) -bn1 | head -10
PID=$(pgrep valkey-server)
sudo perf record -p $PID -g --call-graph dwarf -F 99 -o /tmp/perf-4k.data -- sleep 30
wait
echo "=== PERF 4KB ==="
sudo perf report -i /tmp/perf-4k.data --stdio --no-children -g none --percent-limit 1.0

# ==============================================================
# PERF TEST 2: 1MB
# ==============================================================
~/valkey/src/valkey-cli -p 6399 SHUTDOWN NOSAVE; sleep 1
taskset -c 0-31 ~/valkey/src/valkey-server --port 6399 \
  --loadmodule ~/bigobj/target/release/libvalkey_bigobj.so \
  data-dir /mnt/bigobj-data pool-buf-size 1048576 pool-buf-count 5000 \
  --daemonize yes --logfile /tmp/bigobj.log --save ""
sleep 2

python3 -c "
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 6399))
val = 'B' * (1024*1024)
for i in range(100):
    key = f'key:{i:012d}'
sudo perf report -i /tmp/perf-50m.data --stdio --no-children -g none --percent-limit 1.0key:__rand_int__ & &
Loaded 10000 x 4KB
[1] 233578
top - 22:36:22 up 2 days, 23:28,  1 user,  load average: 0.91, 0.46, 0.19(overall: 0.431) 4.5 seconds 
Threads:   9 total,   2 running,   7 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.2 us,  0.5 sy,  0.0 ni, 98.7 id,  0.2 wa,  0.0 hi,  0.3 si,  0.0 st
MiB Mem : 1520795.+total, 1517454.+free,   2990.0 used,    351.0 buff/cache
MiB Swap:      0.0 total,      0.0 free,      0.0 used. 1513467.+avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
 233562 ec2-user  20   0  533308  75748   5344 R  86.7   0.0   0:04.37 valkey-server
 233564 ec2-user  20   0  533308  75748   5344 R  53.3   0.0   0:02.45 bigobj-uring-po
 233565 ec2-user  20   0  533308  75748   5344 S   0.0   0.0   0:00.00 bio_close_file
[ perf record: Woken up 159 times to write data ]68282.0) avg_msec=0.450 (overall: 0.445) 35.0 seconds 
[ perf record: Captured and wrote 40.931 MB /tmp/perf-4k.data (4926 samples) ]all: 0.445) 35.3 seconds 
====== BO.GET key:__rand_int__ ======                                                                  
  9974992 requests completed in 60.00 seconds
  750 parallel clients
  39 bytes payload
  keep alive: 1
  host configuration "save": 
  host configuration "appendonly": no
  multi-thread: no

Latency by percentile distribution:
0.000% <= 0.151 milliseconds (cumulative count 2)
50.000% <= 0.439 milliseconds (cumulative count 5596950)
75.000% <= 0.463 milliseconds (cumulative count 7546597)
87.500% <= 0.487 milliseconds (cumulative count 8741453)
93.750% <= 0.511 milliseconds (cumulative count 9388562)
96.875% <= 0.535 milliseconds (cumulative count 9698244)
98.438% <= 0.559 milliseconds (cumulative count 9841299)
99.219% <= 0.583 milliseconds (cumulative count 9905314)
99.609% <= 0.607 milliseconds (cumulative count 9936017)
99.805% <= 0.639 milliseconds (cumulative count 9957088)
99.902% <= 0.663 milliseconds (cumulative count 9965321)
99.951% <= 0.695 milliseconds (cumulative count 9970310)
99.976% <= 0.743 milliseconds (cumulative count 9972713)
99.988% <= 0.823 milliseconds (cumulative count 9973787)
99.994% <= 0.983 milliseconds (cumulative count 9974367)
99.997% <= 2.255 milliseconds (cumulative count 9974665)
99.998% <= 3.423 milliseconds (cumulative count 9974818)
99.999% <= 11.767 milliseconds (cumulative count 9974891)
100.000% <= 11.999 milliseconds (cumulative count 9974932)
100.000% <= 12.191 milliseconds (cumulative count 9974949)
100.000% <= 12.287 milliseconds (cumulative count 9974958)
100.000% <= 12.367 milliseconds (cumulative count 9974965)
100.000% <= 12.375 milliseconds (cumulative count 9974967)
100.000% <= 12.375 milliseconds (cumulative count 9974967)

Cumulative distribution of latencies:
0.000% <= 0.103 milliseconds (cumulative count 0)
0.000% <= 0.207 milliseconds (cumulative count 24)
0.070% <= 0.303 milliseconds (cumulative count 7013)
22.915% <= 0.407 milliseconds (cumulative count 2285727)
92.433% <= 0.503 milliseconds (cumulative count 9220204)
99.610% <= 0.607 milliseconds (cumulative count 9936017)
99.960% <= 0.703 milliseconds (cumulative count 9970966)
99.987% <= 0.807 milliseconds (cumulative count 9973646)
99.992% <= 0.903 milliseconds (cumulative count 9974213)
99.994% <= 1.007 milliseconds (cumulative count 9974391)
99.995% <= 1.103 milliseconds (cumulative count 9974446)
99.995% <= 1.207 milliseconds (cumulative count 9974466)
99.995% <= 1.303 milliseconds (cumulative count 9974484)
99.995% <= 1.407 milliseconds (cumulative count 9974491)
99.995% <= 1.503 milliseconds (cumulative count 9974505)
99.996% <= 1.607 milliseconds (cumulative count 9974523)
99.996% <= 1.703 milliseconds (cumulative count 9974548)
99.996% <= 1.807 milliseconds (cumulative count 9974573)
99.996% <= 1.903 milliseconds (cumulative count 9974600)
99.996% <= 2.007 milliseconds (cumulative count 9974616)
99.997% <= 2.103 milliseconds (cumulative count 9974624)
99.998% <= 3.103 milliseconds (cumulative count 9974733)
99.999% <= 4.103 milliseconds (cumulative count 9974850)
100.000% <= 12.103 milliseconds (cumulative count 9974942)
100.000% <= 13.103 milliseconds (cumulative count 9974967)

Summary:
  throughput summary: 166249.86 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.439     0.144     0.439     0.519     0.575    12.375
[1]+  Done                    taskset -c 32-63 ~/valkey/src/valkey-benchmark -p 6399 -c 750 --duration 60 -r 10000 BO.GET key:__rand_int__
=== PERF 4KB ===
# To display the perf.data header info, please use --header/--header-only options.
#
#
# Total Lost Samples: 0
#
# Samples: 4K of event 'cycles'
# Event count (approx.): 99121246227
#
# Overhead  Command          Shared Object        Symbol                                                                                                                                                         >
# ........  ...............  ...................  ...............................................................................................................................................................>
#
     2.95%  bigobj-uring-po  [kernel.kallsyms]    [k] finish_task_switch.isra.0
     2.54%  valkey-server    [kernel.kallsyms]    [k] __wake_up_sync_key
     2.41%  valkey-server    [kernel.kallsyms]    [k] el0_svc
     1.91%  valkey-server    [kernel.kallsyms]    [k] skb_release_data
     1.81%  valkey-server    [kernel.kallsyms]    [k] __inet_lookup_established
     1.48%  valkey-server    [kernel.kallsyms]    [k] skb_defer_free_flush
     1.40%  valkey-server    [kernel.kallsyms]    [k] __slab_free
     1.34%  valkey-server    [kernel.kallsyms]    [k] skb_attempt_defer_free
     1.27%  bigobj-uring-po  [kernel.kallsyms]    [k] fget
     1.16%  valkey-server    [kernel.kallsyms]    [k] kfree_skbmem
     1.13%  valkey-server    [kernel.kallsyms]    [k] arch_counter_get_cntvct
     1.11%  bigobj-uring-po  [kernel.kallsyms]    [k] bio_associate_blkg_from_css
     1.11%  valkey-server    [kernel.kallsyms]    [k] tcp_v4_rcv
     1.08%  valkey-server    [kernel.kallsyms]    [k] tcp_check_space
     1.06%  valkey-server    libvalkey_bigobj.so  [.] <valkey_bigobj::storage::engine::StorageEngine>::get_status
     1.02%  bigobj-uring-po  libc.so.6            [.] __GI___pthread_mutex_unlock_usercnt


#
# (Tip: To record callchains for each sample: perf record -g)
#

:q
Loaded 100 x 1MB

:q
[1] 233677
top - 22:37:34 up 2 days, 23:29,  1 user,  load average: 1.73, 0.83, 0.33overall: 12.987) 4.5 seconds 
Threads:   9 total,   1 running,   8 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.1 us,  0.5 sy,  0.0 ni, 99.1 id,  0.2 wa,  0.0 hi,  0.1 si,  0.0 st
MiB Mem : 1520795.+total, 1512362.+free,   8052.9 used,    380.3 buff/cache
MiB Swap:      0.0 total,      0.0 free,      0.0 used. 1508369.+avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
 233663 ec2-user  20   0 6391612   5.0g   5296 R  53.3   0.3   0:03.87 bigobj-uring-po
 233661 ec2-user  20   0 6391612   5.0g   5296 S  33.3   0.3   0:01.68 valkey-server
 233664 ec2-user  20   0 6391612   5.0g   5296 S   0.0   0.3   0:00.00 bio_close_file
[ perf record: Woken up 100 times to write data ]842.3) avg_msec=16.064 (overall: 15.552) 34.8 seconds 
[ perf record: Captured and wrote 25.378 MB /tmp/perf-1m.data (3029 samples) ]ll: 15.555) 35.1 seconds 
====== BO.GET key:__rand_int__ ======                                                                  
  2832522 requests completed in 60.00 seconds
  750 parallel clients
  39 bytes payload
  keep alive: 1
  host configuration "save": 
  host configuration "appendonly": no
  multi-thread: no

Latency by percentile distribution:
0.000% <= 3.471 milliseconds (cumulative count 1)
50.000% <= 15.183 milliseconds (cumulative count 1416262)
75.000% <= 17.151 milliseconds (cumulative count 2126466)
87.500% <= 19.327 milliseconds (cumulative count 2478758)
93.750% <= 20.175 milliseconds (cumulative count 2656290)
96.875% <= 20.767 milliseconds (cumulative count 2745441)
98.438% <= 21.231 milliseconds (cumulative count 2788647)
99.219% <= 21.631 milliseconds (cumulative count 2810625)
99.609% <= 21.999 milliseconds (cumulative count 2821792)
99.805% <= 22.319 milliseconds (cumulative count 2827137)
99.902% <= 22.623 milliseconds (cumulative count 2829869)
99.951% <= 22.895 milliseconds (cumulative count 2831170)
99.976% <= 23.167 milliseconds (cumulative count 2831841)
99.988% <= 23.423 milliseconds (cumulative count 2832176)
99.994% <= 23.663 milliseconds (cumulative count 2832350)
99.997% <= 23.855 milliseconds (cumulative count 2832436)
99.998% <= 24.111 milliseconds (cumulative count 2832477)
99.999% <= 24.367 milliseconds (cumulative count 2832500)
100.000% <= 24.559 milliseconds (cumulative count 2832510)
100.000% <= 24.671 milliseconds (cumulative count 2832515)
100.000% <= 25.199 milliseconds (cumulative count 2832519)
100.000% <= 25.231 milliseconds (cumulative count 2832520)
100.000% <= 25.231 milliseconds (cumulative count 2832520)

Cumulative distribution of latencies:
0.000% <= 0.103 milliseconds (cumulative count 0)
0.005% <= 4.103 milliseconds (cumulative count 154)
0.010% <= 5.103 milliseconds (cumulative count 284)
0.013% <= 6.103 milliseconds (cumulative count 359)
0.016% <= 7.103 milliseconds (cumulative count 451)
0.022% <= 8.103 milliseconds (cumulative count 633)
0.031% <= 9.103 milliseconds (cumulative count 890)
0.100% <= 10.103 milliseconds (cumulative count 2820)
0.873% <= 11.103 milliseconds (cumulative count 24742)
3.354% <= 12.103 milliseconds (cumulative count 95013)
9.214% <= 13.103 milliseconds (cumulative count 260998)
24.597% <= 14.103 milliseconds (cumulative count 696724)
48.154% <= 15.103 milliseconds (cumulative count 1363979)
66.818% <= 16.103 milliseconds (cumulative count 1892623)
74.850% <= 17.103 milliseconds (cumulative count 2120128)
79.325% <= 18.111 milliseconds (cumulative count 2246909)
85.774% <= 19.103 milliseconds (cumulative count 2429579)
93.362% <= 20.111 milliseconds (cumulative count 2644510)
98.107% <= 21.103 milliseconds (cumulative count 2778913)
99.700% <= 22.111 milliseconds (cumulative count 2824020)
99.972% <= 23.103 milliseconds (cumulative count 2831731)
99.998% <= 24.111 milliseconds (cumulative count 2832477)
100.000% <= 25.103 milliseconds (cumulative count 2832516)
100.000% <= 26.111 milliseconds (cumulative count 2832520)

Summary:
  throughput summary: 47208.70 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
       15.762     3.464    15.183    20.383    21.503    25.231
[1]+  Done                    taskset -c 32-63 ~/valkey/src/valkey-benchmark -p 6399 -c 750 --duration 60 -r 100 BO.GET key:__rand_int__
=== PERF 1MB ===
# To display the perf.data header info, please use --header/--header-only options.
#
#
# Total Lost Samples: 0
#
# Samples: 3K of event 'cycles'
# Event count (approx.): 60251392762
#
# Overhead  Command          Shared Object        Symbol                                                                                                                                                         >
# ........  ...............  ...................  ...............................................................................................................................................................>
#
     7.05%  bigobj-uring-po  [kernel.kallsyms]    [k] blk_map_iter_next
     4.81%  bigobj-uring-po  [kernel.kallsyms]    [k] __srcu_read_lock
     4.65%  bigobj-uring-po  [kernel.kallsyms]    [k] bio_split_io_at
     4.60%  bigobj-uring-po  [kernel.kallsyms]    [k] nvme_pci_setup_data_prp
     2.94%  bigobj-uring-po  [kernel.kallsyms]    [k] dma_pool_alloc
     2.63%  bigobj-uring-po  [kernel.kallsyms]    [k] bio_associate_blkg_from_css
     1.61%  valkey-server    [kernel.kallsyms]    [k] __wake_up_sync_key
     1.59%  bigobj-uring-po  [kernel.kallsyms]    [k] __submit_bio
     1.40%  bigobj-uring-po  [kernel.kallsyms]    [k] nvme_submit_cmds.part.0
     1.23%  bigobj-uring-po  [kernel.kallsyms]    [k] __iomap_dio_rw
     1.21%  bigobj-uring-po  [kernel.kallsyms]    [k] wbt_track
     1.09%  bigobj-uring-po  [kernel.kallsyms]    [k] __bio_advance
     1.06%  bigobj-uring-po  [kernel.kallsyms]    [k] blk_mq_rq_ctx_init.isra.0


#
# (Tip: To see list of saved events and attributes: perf evlist -v)
#

  sent 0
  sent 1
  sent 2
  sent 3
  sent 4
  sent 5
  sent 6
  sent 7
  sent 8
  sent 9
  sent 10
  sent 11
  sent 12
  sent 13
  sent 14
  sent 15
  sent 16
  sent 17
  sent 18
  sent 19
Loaded 20 x 50MB

[1] 233785
top - 22:39:25 up 2 days, 23:31,  1 user,  load average: 0.83, 0.85, 0.40rall: 17.889) 5.0 seconds  
Threads:   9 total,   1 running,   8 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.0 us,  0.2 sy,  0.0 ni, 99.5 id,  0.2 wa,  0.0 hi,  0.0 si,  0.0 st
MiB Mem : 1520795.+total, 1511528.+free,   8861.7 used,    405.6 buff/cache
MiB Swap:      0.0 total,      0.0 free,      0.0 used. 1507535.+avail Mem 

    PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
 233771 ec2-user  20   0 8298812   5.9g   5268 R  40.0   0.4   0:03.74 bigobj-uring-po
 233769 ec2-user  20   0 8298812   5.9g   5268 S   0.0   0.4   0:01.11 valkey-server
 233772 ec2-user  20   0 8298812   5.9g   5268 S   0.0   0.4   0:00.00 bio_close_file
[ perf record: Woken up 61 times to write data ].0) avg_msec=21.504 (overall: 20.897) 35.6 seconds  
[ perf record: Captured and wrote 15.946 MB /tmp/perf-50m.data (1890 samples) ]
====== BO.GET key:__rand_int__ ======                                                              
  56728 requests completed in 60.01 seconds
  20 parallel clients
  39 bytes payload
  keep alive: 1
  host configuration "save": 
  host configuration "appendonly": no
  multi-thread: no

Latency by percentile distribution:
0.000% <= 10.231 milliseconds (cumulative count 1)
50.000% <= 19.903 milliseconds (cumulative count 28364)
75.000% <= 25.743 milliseconds (cumulative count 42590)
87.500% <= 30.159 milliseconds (cumulative count 49657)
93.750% <= 31.791 milliseconds (cumulative count 53198)
96.875% <= 32.223 milliseconds (cumulative count 54984)
98.438% <= 32.415 milliseconds (cumulative count 55887)
99.219% <= 32.527 milliseconds (cumulative count 56298)
99.609% <= 32.639 milliseconds (cumulative count 56520)
99.805% <= 32.767 milliseconds (cumulative count 56625)
99.902% <= 32.895 milliseconds (cumulative count 56673)
99.951% <= 33.567 milliseconds (cumulative count 56700)
99.976% <= 35.103 milliseconds (cumulative count 56714)
99.988% <= 36.543 milliseconds (cumulative count 56722)
99.994% <= 36.831 milliseconds (cumulative count 56726)
99.998% <= 36.863 milliseconds (cumulative count 56727)
100.000% <= 36.863 milliseconds (cumulative count 56727)

Cumulative distribution of latencies:
0.000% <= 0.103 milliseconds (cumulative count 0)
0.659% <= 11.103 milliseconds (cumulative count 374)
2.791% <= 12.103 milliseconds (cumulative count 1583)
3.145% <= 13.103 milliseconds (cumulative count 1784)
3.591% <= 14.103 milliseconds (cumulative count 2037)
8.114% <= 15.103 milliseconds (cumulative count 4603)
28.436% <= 16.103 milliseconds (cumulative count 16131)
43.027% <= 17.103 milliseconds (cumulative count 24408)
44.723% <= 18.111 milliseconds (cumulative count 25370)
47.096% <= 19.103 milliseconds (cumulative count 26716)
50.433% <= 20.111 milliseconds (cumulative count 28609)
54.325% <= 21.103 milliseconds (cumulative count 30817)
58.055% <= 22.111 milliseconds (cumulative count 32933)
62.854% <= 23.103 milliseconds (cumulative count 35655)
68.682% <= 24.111 milliseconds (cumulative count 38961)
73.182% <= 25.103 milliseconds (cumulative count 41514)
76.665% <= 26.111 milliseconds (cumulative count 43490)
79.754% <= 27.103 milliseconds (cumulative count 45242)
82.733% <= 28.111 milliseconds (cumulative count 46932)
85.131% <= 29.103 milliseconds (cumulative count 48292)
87.420% <= 30.111 milliseconds (cumulative count 49591)
90.736% <= 31.103 milliseconds (cumulative count 51472)
95.847% <= 32.111 milliseconds (cumulative count 54371)
99.938% <= 33.119 milliseconds (cumulative count 56692)
99.959% <= 34.111 milliseconds (cumulative count 56704)
99.977% <= 35.103 milliseconds (cumulative count 56714)
99.988% <= 36.127 milliseconds (cumulative count 56720)
100.000% <= 37.119 milliseconds (cumulative count 56727)

Summary:
  throughput summary: 945.32 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
       21.133    10.224    19.903    32.015    32.495    36.863
[1]+  Done                    taskset -c 32-63 ~/valkey/src/valkey-benchmark -p 6399 -c 20 --duration 60 -r 20 BO.GET key:__rand_int__
=== PERF 50MB ===
# To display the perf.data header info, please use --header/--header-only options.
#
#
# Total Lost Samples: 0
#
# Samples: 1K of event 'cycles'
# Event count (approx.): 36212697179
#
# Overhead  Command          Shared Object      Symbol                                    
# ........  ...............  .................  ..........................................
#
    12.29%  bigobj-uring-po  [kernel.kallsyms]  [k] blk_map_iter_next
     8.50%  bigobj-uring-po  [kernel.kallsyms]  [k] __srcu_read_lock
     7.69%  bigobj-uring-po  [kernel.kallsyms]  [k] bio_split_io_at
     7.03%  bigobj-uring-po  [kernel.kallsyms]  [k] nvme_pci_setup_data_prp
     5.67%  bigobj-uring-po  [kernel.kallsyms]  [k] dma_pool_alloc
     4.00%  bigobj-uring-po  [kernel.kallsyms]  [k] bio_associate_blkg_from_css
     2.52%  bigobj-uring-po  [kernel.kallsyms]  [k] iov_iter_advance
     2.43%  bigobj-uring-po  [kernel.kallsyms]  [k] blk_cgroup_bio_start
     2.30%  bigobj-uring-po  [kernel.kallsyms]  [k] bio_will_gap
     2.23%  bigobj-uring-po  [kernel.kallsyms]  [k] __submit_bio
     2.15%  bigobj-uring-po  [kernel.kallsyms]  [k] dm_split_and_process_bio
     2.09%  bigobj-uring-po  [kernel.kallsyms]  [k] nvme_submit_cmds.part.0
     1.98%  bigobj-uring-po  [kernel.kallsyms]  [k] __radix_tree_lookup
     1.66%  bigobj-uring-po  [kernel.kallsyms]  [k] __bio_advance
     1.29%  bigobj-uring-po  [kernel.kallsyms]  [k] __blk_mq_alloc_requests
     1.29%  bigobj-uring-po  [kernel.kallsyms]  [k] blk_mq_submit_bio
     1.22%  bigobj-uring-po  [kernel.kallsyms]  [k] blk_dma_map_iter_start
     1.19%  bigobj-uring-po  [kernel.kallsyms]  [k] blk_mq_rq_ctx_init.isra.0
     1.08%  bigobj-uring-po  [kernel.kallsyms]  [k] sbitmap_find_bit
     1.03%  bigobj-uring-po  [kernel.kallsyms]  [k] __update_cpu_freelist_fast
     1.02%  bigobj-uring-po  [kernel.kallsyms]  [k] wbt_track


#
# (Tip: Customize output of perf script with: perf script -F event,ip,sym)
#
[ec2-user@ip-Xbigobj]$ 

```