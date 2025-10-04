#!/usr/bin/env python3
"""
RabbitMQ Producer
- Publishes N messages to a direct exchange
- Adds metadata (message_id, timestamp, headers with producer_id, host)
- Supports durable queues and lazy (disk-backed) mode for large runs
"""
import argparse, time, uuid, json, getpass, socket
import pika

def open_channel(host, vhost, username, password, conn_name):
    creds = pika.PlainCredentials(username, password) if username else None
    params = pika.ConnectionParameters(
        host=host, virtual_host=vhost, credentials=creds,
        client_properties={"connection_name": conn_name}
    )
    conn = pika.BlockingConnection(params)
    return conn, conn.channel()

def declare_topology(ch, exchange, work_queue, audit_queue, routing_key, durable, lazy):
    ch.exchange_declare(exchange=exchange, exchange_type="direct", durable=True)
    qargs = {"x-queue-type": "classic"}
    if lazy:
        qargs["x-queue-mode"] = "lazy"  # disk-backed
    # work queue
    ch.queue_declare(queue=work_queue, durable=durable, arguments=qargs)
    ch.queue_bind(queue=work_queue, exchange=exchange, routing_key=routing_key)
    # audit queue (optional second copy for monitoring)
    if audit_queue:
        ch.queue_declare(queue=audit_queue, durable=durable, arguments=qargs)
        ch.queue_bind(queue=audit_queue, exchange=exchange, routing_key=routing_key)

def main():
    ap = argparse.ArgumentParser("RabbitMQ Producer")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--vhost", default="/")
    ap.add_argument("--username", default="guest")
    ap.add_argument("--password", default="guest")
    ap.add_argument("--exchange", default="direct.exchange")
    ap.add_argument("--queue", default="work.queue")
    ap.add_argument("--audit-queue", default="audit.queue")
    ap.add_argument("--routing-key", default="work")
    ap.add_argument("--count", type=int, default=10_000)
    ap.add_argument("--text", default="Hello World")
    ap.add_argument("--durable", action="store_true")
    ap.add_argument("--lazy", action="store_true")
    ap.add_argument("--purge", action="store_true")
    ap.add_argument("--progress-every", type=int, default=10_000)
    ap.add_argument("--producer-id", default=getpass.getuser())
    args = ap.parse_args()

    conn, ch = open_channel(args.host, args.vhost, args.username, args.password,
                            conn_name=f"RabbitMQ Producer ({socket.gethostname()})")
    declare_topology(ch, args.exchange, args.queue, args.audit_queue,
                     args.routing_key, args.durable, args.lazy)

    if args.purge:
        # Purge both work and audit queues
        ch.queue_purge(queue=args.queue)
        print(f"[producer] purged queue: {args.queue}")
        if args.audit_queue:
            ch.queue_purge(queue=args.audit_queue)
            print(f"[producer] purged audit queue: {args.audit_queue}")

    props = pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2 if args.durable else 1,  # 2 = persistent
        app_id="rabbitmq.producer",
        headers={"producer_id": args.producer_id, "host": socket.gethostname()}
    )

    start = time.perf_counter()
    for i in range(1, args.count + 1):
        body = json.dumps({"n": i, "text": args.text, "sent_by": args.producer_id}).encode("utf-8")
        props.message_id = str(uuid.uuid4())
        props.timestamp   = int(time.time())
        ch.basic_publish(exchange=args.exchange, routing_key=args.routing_key,
                         body=body, properties=props)
        if i % args.progress_every == 0:
            print(f"[producer] published {i}/{args.count}")

    ready = ch.queue_declare(queue=args.queue, passive=True).method.message_count
    elapsed = time.perf_counter() - start
    print(f"[producer] done. published={args.count}, in_queue_ready={ready}, time={elapsed:.2f}s")
    conn.close()

if __name__ == "__main__":
    main()
