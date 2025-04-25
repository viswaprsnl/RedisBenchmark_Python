import string
import random
import argparse
import time
import threading
from redis.cluster import RedisCluster, ClusterNode
from redis.connection import ClusterConnectionPool

# === CLI Args ===
parser = argparse.ArgumentParser(description="Redis Cluster Load Benchmark with TTL and key size distribution")
parser.add_argument('--host', default='127.0.0.1')
parser.add_argument('--port', type=int, default=0000)
parser.add_argument('--password', default=None)
parser.add_argument('--ops', type=int, default=100000)
parser.add_argument('--threads', type=int, default=4)
parser.add_argument('--batch-size', type=int, default=5)
parser.add_argument('--key-maximum', type=int, default=100000)
parser.add_argument('--ratio', default='1:1:1')  # mset:mget:hset
parser.add_argument('--pipeline', type=int, default=1)
args = parser.parse_args()

# === TTL and Key Size Distribution ===
TTL_SECONDS = 900  # 15 minutes
size_distribution = {
    1: 730 * 1024,   # 1% of keys = 730 KB
    5: 4 * 1024,     # 5% of keys = 4 KB
    94: 128          # 94% of keys = 128 B
}

def pick_size():
    rand = random.randint(1, 100)
    cumulative = 0
    for percent, size in size_distribution.items():
        cumulative += percent
        if rand <= cumulative:
            return size
    return 128

# === Ratios Setup ===
mset_r, mget_r, hset_r = map(int, args.ratio.split(':'))
op_choices = ['mset'] * mset_r + ['mget'] * mget_r + ['hset'] * hset_r

def same_slot_keys(base_index, count):
    return [f"user:{{{base_index}}}:{i}" for i in range(count)]

def generate_value(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

# === Worker Thread ===
def worker(thread_id, rc, start_index, ops_per_thread):
    pipe = rc.pipeline(transaction=False)
    pipe_depth = 0

    for i in range(start_index, start_index + ops_per_thread):
        op_type = random.choice(op_choices)
        key_id = i % args.key_maximum
        keys = same_slot_keys(key_id, args.batch_size)
        values = [generate_value(pick_size()) for _ in keys]

        try:
            if op_type == 'mset':
                args_list = []
                for k, v in zip(keys, values):
                    args_list.extend([k, v])
                pipe.execute_command("MSET", *args_list)
                for k in keys:
                    pipe.expire(k, TTL_SECONDS)

            elif op_type == 'mget':
                pipe.execute_command("MGET", *keys)

            elif op_type == 'hset':
                hash_key = f"user:{{{key_id}}}:profile"
                field_data = {f"field_{j}": values[j] for j in range(args.batch_size)}
                pipe.hset(hash_key, mapping=field_data)
                pipe.expire(hash_key, TTL_SECONDS)

            pipe_depth += 1
            if pipe_depth >= args.pipeline:
                pipe.execute()
                pipe = rc.pipeline(transaction=False)
                pipe_depth = 0

        except Exception as e:
            print(f"[Thread {thread_id}] Error: {e}")

    if pipe_depth > 0:
        pipe.execute()

# === Redis Cluster Connection Pool ===
pool = ClusterConnectionPool(
    startup_nodes=[ClusterNode(host=args.host, port=args.port)],
    max_connections=1000,
    decode_responses=True,
    password=args.password
)
rc = RedisCluster(connection_pool=pool)

# === Thread Launch ===
threads = []
ops_per_thread = args.ops // args.threads
print(f"🚀 Starting benchmark: {args.ops} ops across {args.threads} threads")

start_time = time.time()
for t in range(args.threads):
    thread = threading.Thread(target=worker, args=(t, rc, t * ops_per_thread, ops_per_thread))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

end_time = time.time()
total_time = end_time - start_time
throughput = args.ops / total_time

# === Final Output ===
print("\n====== Cranked Benchmark Results ======")
print(f"Total ops: {args.ops}")
print(f"Threads: {args.threads}")
print(f"Pipeline: {args.pipeline}")
print(f"TTL: {TTL_SECONDS} seconds (applied to SET and HSET keys)")
print(f"Total time: {total_time:.2f} sec")
print(f"Throughput: {throughput:.2f} ops/sec")
