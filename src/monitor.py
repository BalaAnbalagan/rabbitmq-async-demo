#!/usr/bin/env python3
"""
RabbitMQ Monitor via Management API
- Lists queues (ready/unacked/total), consumers, connections
- Optional --watch N refreshes every N seconds
- Card-style display with color coding
- Activity log showing recent events
"""
import argparse, time, requests, sys
from collections import deque

def snapshot(base_url, auth):
    try:
        q  = requests.get(f"{base_url}/api/queues", auth=auth, timeout=10).json()
        cs = requests.get(f"{base_url}/api/consumers", auth=auth, timeout=10).json()
        cn = requests.get(f"{base_url}/api/connections", auth=auth, timeout=10).json()
        ov = requests.get(f"{base_url}/api/overview", auth=auth, timeout=10).json()
        nd = requests.get(f"{base_url}/api/nodes", auth=auth, timeout=10).json()
    except Exception as e:
        print(f"[monitor] error contacting management API: {e}", file=sys.stderr)
        return [], [], [], {}, {}

    # Enhanced queue info with rates
    queues = []
    queue_details = {}
    for x in q:
        name = x["name"]
        ready = x.get("messages_ready", 0)
        unacked = x.get("messages_unacknowledged", 0)
        total = x.get("messages", 0)

        # Message rates
        msg_stats = x.get("message_stats", {})
        publish_rate = msg_stats.get("publish_details", {}).get("rate", 0.0)
        deliver_rate = msg_stats.get("deliver_get_details", {}).get("rate", 0.0)
        ack_rate = msg_stats.get("ack_details", {}).get("rate", 0.0)

        queues.append((name, ready, unacked, total))
        queue_details[name] = {
            "publish_rate": publish_rate,
            "deliver_rate": deliver_rate,
            "ack_rate": ack_rate,
            "consumers": x.get("consumers", 0)
        }

    queues = sorted(queues, key=lambda r: r[0])

    consumers = [(c["queue"]["name"], c["consumer_tag"], c["channel_details"]["connection_name"])
                 for c in cs]
    conns = [(c.get("name",""), c.get("user",""), c.get("client_properties",{}).get("connection_name",""))
             for c in cn]

    # Extract RabbitMQ resource metrics from nodes API (first node)
    queue_totals = ov.get("queue_totals", {})
    object_totals = ov.get("object_totals", {})
    node_stats = nd[0] if nd else {}

    resources = {
        "memory_used": node_stats.get("mem_used", 0),
        "memory_limit": node_stats.get("mem_limit", 0),
        "disk_free": node_stats.get("disk_free", 0),
        "disk_limit": node_stats.get("disk_free_limit", 0),
        "fd_used": node_stats.get("fd_used", 0),
        "fd_total": node_stats.get("fd_total", 0),
        "sockets_used": node_stats.get("sockets_used", 0),
        "sockets_total": node_stats.get("sockets_total", 0),
        "proc_used": node_stats.get("proc_used", 0),
        "proc_total": node_stats.get("proc_total", 0),
        "uptime": node_stats.get("uptime", 0),
        "total_messages": queue_totals.get("messages", 0),
        "total_ready": queue_totals.get("messages_ready", 0),
        "total_unacked": queue_totals.get("messages_unacknowledged", 0),
        "total_queues": object_totals.get("queues", 0),
        "total_connections": object_totals.get("connections", 0),
        "total_channels": object_totals.get("channels", 0),
        "total_consumers": object_totals.get("consumers", 0),
    }

    return queues, consumers, conns, queue_details, resources

def format_number(n):
    """Format large numbers with K/M suffixes"""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)

def format_bytes(n):
    """Format bytes to MB/GB"""
    if n >= 1_073_741_824:  # 1GB
        return f"{n/1_073_741_824:.1f}GB"
    elif n >= 1_048_576:  # 1MB
        return f"{n/1_048_576:.0f}MB"
    elif n >= 1024:  # 1KB
        return f"{n/1024:.0f}KB"
    return f"{n}B"

def format_uptime(ms):
    """Format uptime from milliseconds to human readable"""
    seconds = ms // 1000
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    if days > 0:
        return f"{days}d {hours}h"
    elif hours > 0:
        return f"{hours}h {minutes}m"
    else:
        return f"{minutes}m"

def get_status_color(ready, unacked):
    """Return color code based on message count"""
    total = ready + unacked
    if total == 0:
        return "\033[32m"  # green
    elif total < 1000:
        return "\033[33m"  # yellow
    else:
        return "\033[31m"  # red

def print_card_style(queues, consumers, conns, queue_details, resources, event_log=None):
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    CLEAR_LINE = "\033[K"  # Clear from cursor to end of line

    # Summary
    total_ready = sum(q[1] for q in queues)
    total_unacked = sum(q[2] for q in queues)
    total_consumers = len(consumers)

    # Activity indicators
    active_queues = sum(1 for name, _, _, _ in queues if queue_details.get(name, {}).get("publish_rate", 0) > 0 or queue_details.get(name, {}).get("deliver_rate", 0) > 0)

    # Calculate resource percentages (only show if data available)
    show_resources = resources["memory_limit"] > 0 or resources["uptime"] > 0

    if show_resources:
        mem_pct = (resources["memory_used"] / resources["memory_limit"] * 100) if resources["memory_limit"] > 0 else 0
        fd_pct = (resources["fd_used"] / resources["fd_total"] * 100) if resources["fd_total"] > 0 else 0

        print(f"\n{BOLD}╔{'═' * 65}╗{RESET}{CLEAR_LINE}")
        print(f"{BOLD}║  RabbitMQ Monitor Summary{' ' * 38}║{RESET}{CLEAR_LINE}")
        print(f"{BOLD}╠{'═' * 65}╣{RESET}{CLEAR_LINE}")
        print(f"{BOLD}║{RESET}  Queues: {CYAN}{len(queues):<3}{RESET}  Consumers: {GREEN}{total_consumers:<3}{RESET}  Connections: {YELLOW}{len(conns):<3}{RESET}           {BOLD}║{RESET}{CLEAR_LINE}")
        print(f"{BOLD}║{RESET}  Ready: {CYAN}{format_number(total_ready):>6}{RESET}  Unacked: {YELLOW}{format_number(total_unacked):>6}{RESET}  Active: {GREEN}{active_queues}/{len(queues)}{RESET}        {BOLD}║{RESET}{CLEAR_LINE}")
        print(f"{BOLD}╠{'═' * 65}╣{RESET}{CLEAR_LINE}")
        print(f"{BOLD}║{RESET}  Memory: {format_bytes(resources['memory_used']):>6} / {format_bytes(resources['memory_limit']):<6} ({mem_pct:>4.1f}%)       {BOLD}║{RESET}{CLEAR_LINE}")
        print(f"{BOLD}║{RESET}  Disk Free: {format_bytes(resources['disk_free']):>8}  File Desc: {resources['fd_used']:>4}/{resources['fd_total']:<4} ({fd_pct:>4.1f}%)  {BOLD}║{RESET}{CLEAR_LINE}")
        print(f"{BOLD}║{RESET}  Uptime: {format_uptime(resources['uptime']):<8}  Sockets: {resources['sockets_used']:>4}/{resources['sockets_total']:<4}           {BOLD}║{RESET}{CLEAR_LINE}")
        print(f"{BOLD}╚{'═' * 65}╝{RESET}\n{CLEAR_LINE}")
    else:
        # Simplified summary without resources
        print(f"\n{BOLD}╔{'═' * 65}╗{RESET}{CLEAR_LINE}")
        print(f"{BOLD}║  RabbitMQ Monitor Summary{' ' * 38}║{RESET}{CLEAR_LINE}")
        print(f"{BOLD}╠{'═' * 65}╣{RESET}{CLEAR_LINE}")
        print(f"{BOLD}║{RESET}  Queues: {CYAN}{len(queues):<3}{RESET}  Consumers: {GREEN}{total_consumers:<3}{RESET}  Connections: {YELLOW}{len(conns):<3}{RESET}           {BOLD}║{RESET}{CLEAR_LINE}")
        print(f"{BOLD}║{RESET}  Ready: {CYAN}{format_number(total_ready):>6}{RESET}  Unacked: {YELLOW}{format_number(total_unacked):>6}{RESET}  Active: {GREEN}{active_queues}/{len(queues)}{RESET}        {BOLD}║{RESET}{CLEAR_LINE}")
        print(f"{BOLD}╚{'═' * 65}╝{RESET}\n{CLEAR_LINE}")

    # Queues with activity info
    if queues:
        print(f"\n{BOLD}{CYAN}━━━ QUEUES ━━━{RESET}{CLEAR_LINE}")
        print(CLEAR_LINE)
        for name, ready, unacked, total in queues:
            color = get_status_color(ready, unacked)
            details = queue_details.get(name, {})
            pub_rate = details.get("publish_rate", 0.0)
            del_rate = details.get("deliver_rate", 0.0)
            ack_rate = details.get("ack_rate", 0.0)
            num_consumers = details.get("consumers", 0)

            # Activity indicator
            activity = ""
            if pub_rate > 0:
                activity += f"{GREEN}⬆ Publishing{RESET} "
            if del_rate > 0 or ack_rate > 0:
                activity += f"{YELLOW}⬇ Consuming{RESET} "
            if not activity:
                activity = f"{DIM}Idle{RESET}"

            print(f"{color}●{RESET} {BOLD}{name}{RESET}  [{activity}]{CLEAR_LINE}")
            print(f"  Messages: Ready={color}{format_number(ready):>6}{RESET}  Unacked={format_number(unacked):>6}  Total={format_number(total):>6}{CLEAR_LINE}")

            # Show rates if active
            if pub_rate > 0 or del_rate > 0 or ack_rate > 0:
                rate_info = f"  Activity: "
                if pub_rate > 0:
                    rate_info += f"Pub={pub_rate:.1f}/s  "
                if del_rate > 0:
                    rate_info += f"Del={del_rate:.1f}/s  "
                if ack_rate > 0:
                    rate_info += f"Ack={ack_rate:.1f}/s  "
                print(f"{DIM}{rate_info.rstrip()}{RESET}{CLEAR_LINE}")

            if num_consumers > 0:
                print(f"  {GREEN}✓{RESET} {num_consumers} active consumer(s){CLEAR_LINE}")
            print(CLEAR_LINE)
    else:
        print(f"{DIM}No queues found{RESET}\n{CLEAR_LINE}")

    # Consumers
    if consumers:
        print(f"\n{BOLD}{GREEN}━━━ CONSUMERS ({len(consumers)}) ━━━{RESET}{CLEAR_LINE}")
        print(CLEAR_LINE)
        for queue, tag, conn_name in consumers:
            print(f"  Queue: {BOLD}{queue}{RESET}{CLEAR_LINE}")
            print(f"  Tag: {DIM}{tag}{RESET}{CLEAR_LINE}")
            print(f"  Connection: {conn_name}{CLEAR_LINE}")
            print(CLEAR_LINE)
    else:
        print(f"{DIM}No active consumers{RESET}\n{CLEAR_LINE}")

    # Connections
    if conns:
        print(f"\n{BOLD}{YELLOW}━━━ CONNECTIONS ({len(conns)}) ━━━{RESET}{CLEAR_LINE}")
        print(CLEAR_LINE)
        for name, user, conn_name in conns:
            print(f"  {BOLD}{conn_name or name}{RESET}{CLEAR_LINE}")
            print(f"  User: {user}{CLEAR_LINE}")
            print(CLEAR_LINE)
    else:
        print(f"{DIM}No active connections{RESET}\n{CLEAR_LINE}")

    # Activity Log
    if event_log and len(event_log) > 0:
        print(f"\n{BOLD}{CYAN}━━━ RECENT ACTIVITY (last {len(event_log)} events) ━━━{RESET}{CLEAR_LINE}")
        print(CLEAR_LINE)
        for event in event_log:
            print(f"  {event}{CLEAR_LINE}")
        print(CLEAR_LINE)

    # Clear any remaining lines below
    for _ in range(5):
        print(CLEAR_LINE)

def main():
    ap = argparse.ArgumentParser("RabbitMQ Monitor (Management API)")
    ap.add_argument("--host", default="http://localhost:15672")
    ap.add_argument("--username", default="guest")
    ap.add_argument("--password", default="guest")
    ap.add_argument("--watch", type=int, default=3, help="Auto-refresh every N seconds (0=once, default=3)")
    ap.add_argument("--once", action="store_true", help="Run once and exit (same as --watch 0)")
    args = ap.parse_args()

    # If --once is specified, override --watch
    if args.once:
        args.watch = 0

    base = args.host.rstrip("/")
    auth = (args.username, args.password)

    if args.watch > 0:
        import os

        # Event log to track changes
        event_log = deque(maxlen=10)  # Keep last 10 events
        prev_state = {}  # Track previous state for change detection

        try:
            # Clear screen once at start
            if os.name == 'nt':  # Windows
                os.system('cls')
            else:  # Unix/Linux/Mac
                os.system('clear')

            # Get initial snapshot to set up screen
            first_run = True

            while True:
                # Only clear screen on first run, then just reposition cursor
                if first_run:
                    if os.name == 'nt':  # Windows
                        os.system('cls')
                    else:  # Unix/Linux/Mac
                        print("\033[2J\033[H", end="")
                    first_run = False
                else:
                    # Move cursor to home without clearing
                    print("\033[H", end="")

                current_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"\033[1m⏰ Last updated: {current_time}\033[0m\033[K")  # Clear to end of line

                queues, consumers, conns, queue_details, resources = snapshot(base, auth)

                # Detect changes and log events
                current_state = {}
                for name, ready, unacked, total in queues:
                    current_state[name] = (ready, unacked, total)

                    # Check for changes
                    if name in prev_state:
                        prev_ready, prev_unacked, prev_total = prev_state[name]

                        # Detect significant changes
                        if ready > prev_ready + 100:
                            delta = ready - prev_ready
                            event_log.append(f"[{current_time}] 📥 {name}: +{format_number(delta)} messages published")
                        elif ready < prev_ready - 100:
                            delta = prev_ready - ready
                            event_log.append(f"[{current_time}] 📤 {name}: -{format_number(delta)} messages consumed")

                        if unacked > prev_unacked + 100:
                            delta = unacked - prev_unacked
                            event_log.append(f"[{current_time}] ⏳ {name}: +{format_number(delta)} unacked (consumer active)")
                        elif unacked < prev_unacked - 100:
                            delta = prev_unacked - unacked
                            event_log.append(f"[{current_time}] ✓ {name}: {format_number(delta)} messages acknowledged")
                    else:
                        # New queue detected
                        if total > 0:
                            event_log.append(f"[{current_time}] 🆕 {name}: New queue created with {format_number(total)} messages")

                # Detect consumer changes
                current_consumer_count = len(consumers)
                prev_consumer_count = prev_state.get('_consumer_count', 0)
                if current_consumer_count > prev_consumer_count:
                    event_log.append(f"[{current_time}] 👤 Consumer connected (total: {current_consumer_count})")
                elif current_consumer_count < prev_consumer_count:
                    event_log.append(f"[{current_time}] 👋 Consumer disconnected (total: {current_consumer_count})")

                prev_state = current_state
                prev_state['_consumer_count'] = current_consumer_count

                print_card_style(queues, consumers, conns, queue_details, resources, event_log)
                sys.stdout.flush()  # Force output
                time.sleep(args.watch)
        except KeyboardInterrupt:
            # Clear screen and show exit message
            if os.name == 'nt':  # Windows
                os.system('cls')
            else:  # Unix/Linux/Mac
                print("\033[2J\033[H", end="")  # Clear screen and move cursor home
            print("\n\033[32m✅ Monitor stopped\033[0m\n")
        finally:
            # Reset terminal
            print("\033[0m", end="")  # Reset colors
            sys.stdout.flush()
    else:
        queues, consumers, conns, queue_details, resources = snapshot(base, auth)
        print_card_style(queues, consumers, conns, queue_details, resources, event_log=None)

if __name__ == "__main__":
    main()
