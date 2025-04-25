How to run the script , sample

python benchmark_cranked.py \
  --host 127.0.0.1 \
  --port 0000 \
  --password pwd \
  --ops 1000000 \
  --threads 32 \
  --batch-size 10 \
  --key-maximum 1000000 \
  --pipeline 20 \
  --ratio 2:1:1


  Parameter	Value	Meaning
  
--host	127.0.0.1	Redis Cluster entry point IP
--port	0000	Cluster node port
--password	yourpassword	Auth password (replace with your actual one)
--ops	500000	Total number of operations (MSET + MGET + HSET combined)
--threads	16	16 concurrent worker threads
--batch-size	10	Each op touches 10 keys
--key-maximum	1000000	Total keyspace size (ensures even cluster distribution)
--pipeline	10	Groups 10 operations per Redis request
--ratio	2:1:1	2 MSETs : 1 MGET : 1 HSET
