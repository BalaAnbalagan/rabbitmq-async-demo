# Stress Test: 1,000,000 messages with lazy queue
python -m pip install -r requirements.txt
python src/producer.py --queue work.queue.1m --audit-queue audit.queue.1m `
  --count 1000000 --durable --lazy --purge --progress-every 50000
python src/monitor.py
python src/consumer.py --queue work.queue.1m --name Worker-1M `
  --prefetch 5000 --expect 1000000 --progress-every 50000 --lazy
python src/monitor.py
