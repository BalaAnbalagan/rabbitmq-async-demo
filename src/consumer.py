#!/usr/bin/env python3
"""
RabbitMQ Consumer (terminal)
- Manual acks (reliable)
- Prefetch for backpressure
- Shows "who sent it" and message_id
- Clean Ctrl+C handling
"""
import argparse, time, json, socket, signal
import pika

def open_channel(host, vhost, username, password, conn_name):
    creds = pika.PlainCredentials(username, password) if username else None
    params = pika.ConnectionParameters(
        host=host, virtual_host=vhost, credentials=creds,
        client_properties={"connection_name": conn_name},
        heartbeat=600,  # 10 minutes heartbeat for long-running consumers
        blocked_connection_timeout=300  # 5 minutes timeout
    )
    conn = pika.BlockingConnection(params)
    return conn, conn.channel()

def ensure_queue(ch, conn, queue, lazy=False):
    """Try passive declare; if missing, declare durable classic (optional lazy)."""
    try:
        ch.queue_declare(queue=queue, passive=True)
        return ch
    except pika.exceptions.ChannelClosedByBroker:
        # reopen channel and declare with standard args
        ch = conn.channel()
        qargs = {"x-queue-type": "classic"}
        if lazy:
            qargs["x-queue-mode"] = "lazy"
        ch.queue_declare(queue=queue, durable=True, arguments=qargs)
        return ch

def main():
    ap = argparse.ArgumentParser("RabbitMQ Consumer")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--vhost", default="/")
    ap.add_argument("--username", default="guest")
    ap.add_argument("--password", default="guest")
    ap.add_argument("--queue", default="work.queue")
    ap.add_argument("--name", default="Worker")
    ap.add_argument("--prefetch", type=int, default=1000)
    ap.add_argument("--expect", type=int, default=None)
    ap.add_argument("--progress-every", type=int, default=10_000)
    ap.add_argument("--lazy", action="store_true", help="Match a lazy queue if used by producer")
    args = ap.parse_args()

    conn, ch = open_channel(args.host, args.vhost, args.username, args.password,
                            conn_name=f"RabbitMQ Consumer {args.name} ({socket.gethostname()})")

    ch = ensure_queue(ch, conn, args.queue, lazy=args.lazy)
    ch.basic_qos(prefetch_count=args.prefetch)

    count = 0
    start = time.perf_counter()

    def on_msg(ch_cb, method, props, body):
        nonlocal count
        count += 1
        producer = (props.headers or {}).get("producer_id") if props else None
        msg_id = getattr(props, "message_id", None)
        try:
            data = json.loads(body)
            text = data.get("text")
            n = data.get("n")
        except Exception:
            text = body.decode("utf-8", "ignore")
            n = "?"
        print(f"[{args.name}] got #{count} (msg_id={msg_id}, from={producer}) n={n} text={text}")
        ch_cb.basic_ack(delivery_tag=method.delivery_tag)

        if args.expect and count >= args.expect:
            ch_cb.stop_consuming()

    ch.basic_consume(queue=args.queue, on_message_callback=on_msg, auto_ack=False)

    def handle_sigint(sig, frame):
        print(f"\n[{args.name}] Ctrl+C — stopping…")
        try: ch.stop_consuming()
        except Exception: pass
    signal.signal(signal.SIGINT, handle_sigint)

    print(f"[{args.name}] waiting for messages… Press Ctrl+C to stop")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.perf_counter() - start
        print(f"[{args.name}] done. consumed={count}, time={elapsed:.2f}s")
        try:
            if conn.is_open: conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
