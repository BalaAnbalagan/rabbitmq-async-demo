# Reliability Test: 10,000 messages end-to-end
python -m pip install -r requirements.txt
python src/producer.py --count 10000 --durable --purge
python src/monitor.py
python src/consumer.py --name Worker-1 --expect 10000
python src/monitor.py
