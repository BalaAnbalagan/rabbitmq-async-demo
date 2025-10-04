#!/usr/bin/env python3
"""
RabbitMQ GUI Consumer (Tkinter)
- Opens a window for this consumer and displays each message line-by-line
"""
import argparse, json, socket, threading, queue, tkinter as tk
from tkinter.scrolledtext import ScrolledText
import pika

def run_consumer(qname, host, vhost, username, password, conn_name, prefetch, stop_evt, ui_queue):
    creds = pika.PlainCredentials(username, password) if username else None
    params = pika.ConnectionParameters(
        host=host, virtual_host=vhost, credentials=creds,
        client_properties={"connection_name": conn_name}
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    # Passive declare to avoid mismatched-argument errors
    ch.queue_declare(queue=qname, passive=True)
    ch.basic_qos(prefetch_count=prefetch)

'''     def on_msg(ch_cb, method, props, body):
        producer = (props.headers or {}).get("producer_id") if props else None
        msg_id = getattr(props, "message_id", None)
        try:
            data = json.loads(body)
            text = data.get("text")
            n = data.get("n")
        except Exception:
            text = body.decode("utf-8", "ignore")
            n = "?"
        ui_queue.put(f"msg_id={msg_id} from={producer} n={n} text={text}")
        method.channel.basic_ack(delivery_tag=method.delivery_tag)
        if stop_evt.is_set():
            ch_cb.stop_consuming()

    ch.basic_consume(queue=qname, on_message_callback=on_msg, auto_ack=False)

    try:
        ch.start_consuming()
    finally:
        try: conn.close()
        except Exception: pass '''
    def on_msg(ch_cb, method, props, body):
        nonlocal count
        try:
            count += 1
            producer = (props.headers or {}).get("producer_id") if props else None
            msg_id = getattr(props, "message_id", None)

            try:
                data = json.loads(body)
                text = data.get("text")
                n = data.get("n")
            except Exception:
                # Non-JSON body is fine; just show as text
                text = body.decode("utf-8", "ignore")
                n = "?"

            print(f"[{args.name}] got #{count} (msg_id={msg_id}, from={producer}) n={n} text={text}")

            # ACK exactly once on the *same* channel that delivered the message
            method.channel.basic_ack(delivery_tag=method.delivery_tag)

            if args.expect and count >= args.expect:
                ch_cb.stop_consuming()

        except Exception as e:
            # Log and requeue the message so it isn't lost
            import traceback
            print(f"[{args.name}] ERROR in callback: {e}")
            traceback.print_exc()
            try:
                method.channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception:
                pass  # If nack fails, channel may already be closing; next run will pick it up


def main():
    ap = argparse.ArgumentParser("RabbitMQ GUI Consumer")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--vhost", default="/")
    ap.add_argument("--username", default="guest")
    ap.add_argument("--password", default="guest")
    ap.add_argument("--queue", default="work.queue")
    ap.add_argument("--name", default="Window-1")
    ap.add_argument("--prefetch", type=int, default=500)
    args = ap.parse_args()

    root = tk.Tk()
    root.title(f"RabbitMQ Consumer — {args.name}")

    text = ScrolledText(root, height=24, width=100)
    text.pack(fill="both", expand=True)
    status = tk.StringVar(value="waiting…")
    tk.Label(root, textvariable=status, anchor="w").pack(fill="x")

    ui_queue = queue.Queue()
    stop_evt = threading.Event()
    thread = threading.Thread(
        target=run_consumer,
        args=(args.queue, args.host, args.vhost, args.username, args.password,
              f"RabbitMQ GUI Consumer {args.name} ({socket.gethostname()})", args.prefetch, stop_evt, ui_queue),
        daemon=True
    )
    thread.start()

    count = 0
    def pump():
        nonlocal count
        try:
            while True:
                line = ui_queue.get_nowait()
                count += 1
                text.insert("end", f"{count:>6}: {line}\n")
                text.see("end")
        except queue.Empty:
            pass
        status.set(f"received={count}")
        root.after(100, pump)
    pump()

    def on_close():
        stop_evt.set()
        root.destroy()
    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()

if __name__ == "__main__":
    main()
