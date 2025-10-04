#!/bin/bash
# RabbitMQ Queue and Exchange Cleanup Script
# Deletes queues and exchanges to reset the environment

show_help() {
    echo -e "\033[36mRabbitMQ Queue and Exchange Cleanup Script\033[0m"
    echo ""
    echo -e "\033[33mUsage:\033[0m"
    echo "  ./cleanup_queues.sh all              # Delete ALL queues and exchanges"
    echo "  ./cleanup_queues.sh exchanges        # Delete only unused exchanges"
    echo "  ./cleanup_queues.sh work.queue       # Delete specific queue"
    echo "  ./cleanup_queues.sh q1 q2 q3         # Delete multiple queues"
    echo ""
    echo -e "\033[33mExamples:\033[0m"
    echo "  ./cleanup_queues.sh all              # Complete reset (queues + exchanges)"
    echo "  ./cleanup_queues.sh exchanges        # Clean up old exchanges only"
    echo "  ./cleanup_queues.sh work.queue       # Delete work.queue only"
    echo "  ./cleanup_queues.sh work.queue audit.queue  # Delete both"
    echo ""
}

if [ $# -eq 0 ] || [ "$1" == "help" ] || [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    show_help
    exit 0
fi

echo -e "\n\033[36mRabbitMQ Queue Cleanup\033[0m"
echo -e "\033[36m================================\033[0m\n"

# Get list of existing queues
echo -e "\033[33m[*] Listing current queues...\033[0m"
rabbitmqctl list_queues name messages
echo ""

# Parse existing queue names
existing_queues=$(rabbitmqctl list_queues -q 2>/dev/null | tail -n +2 | awk '{print $1}')

if [ "$1" == "all" ]; then
    if [ -z "$existing_queues" ]; then
        echo -e "\033[33m[INFO] No queues found to delete.\033[0m"
    else
        queue_count=$(echo "$existing_queues" | wc -l)
        echo -e "\033[33m[*] Deleting ALL queues ($queue_count found)...\033[0m"
        for queue in $existing_queues; do
            if rabbitmqctl delete_queue "$queue" 2>/dev/null; then
                echo -e "  \033[32m[OK] Deleted: $queue\033[0m"
            else
                echo -e "  \033[31m[FAIL] Could not delete: $queue\033[0m"
            fi
        done
    fi
elif [ "$1" == "exchanges" ]; then
    # Skip queue deletion when only cleaning exchanges
    :
else
    echo -e "\033[33m[*] Deleting specified queues...\033[0m"
    for queue in "$@"; do
        if rabbitmqctl delete_queue "$queue" 2>/dev/null; then
            echo -e "  \033[32m[OK] Deleted: $queue\033[0m"
        else
            echo -e "  \033[31m[FAIL] Not found or error: $queue\033[0m"
        fi
    done
fi

# Clean up exchanges if requested
if [ "$1" == "all" ] || [ "$1" == "exchanges" ]; then
    echo ""
    echo -e "\033[33m[*] Cleaning up custom exchanges...\033[0m"
    echo -e "  \033[36m[INFO] Waiting for bindings to clear...\033[0m"
    sleep 1

    # List all exchanges
    exchange_list=$(rabbitmqctl list_exchanges name type 2>&1 | tail -n +2)

    # Built-in exchanges to preserve (never delete these)
    builtin_exchanges=("" "amq.direct" "amq.fanout" "amq.headers" "amq.match" "amq.rabbitmq.trace" "amq.topic")

    deleted_count=0
    failed_count=0
    while IFS= read -r line; do
        if [[ $line =~ ^([^[:space:]]+)[[:space:]]+ ]]; then
            exchange_name="${BASH_REMATCH[1]}"

            # Skip built-in exchanges
            is_builtin=false
            for builtin in "${builtin_exchanges[@]}"; do
                if [ "$exchange_name" == "$builtin" ]; then
                    is_builtin=true
                    break
                fi
            done

            if [ "$is_builtin" == "false" ]; then
                # Try with --if-unused flag first (safer - only deletes if no bindings)
                if rabbitmqctl delete_exchange "$exchange_name" --if-unused 2>/dev/null; then
                    echo -e "  \033[32m[OK] Deleted exchange: $exchange_name\033[0m"
                    ((deleted_count++))
                else
                    # If --if-unused fails, try force delete
                    error_output=$(rabbitmqctl delete_exchange "$exchange_name" 2>&1)
                    if [ $? -eq 0 ]; then
                        echo -e "  \033[32m[OK] Deleted exchange: $exchange_name (forced)\033[0m"
                        ((deleted_count++))
                    else
                        echo -e "  \033[31m[FAIL] Could not delete exchange: $exchange_name\033[0m"
                        echo -e "        \033[90mReason: $(echo "$error_output" | head -1)\033[0m"
                        ((failed_count++))
                    fi
                fi
            fi
        fi
    done <<< "$exchange_list"

    if [ $deleted_count -eq 0 ] && [ $failed_count -eq 0 ]; then
        echo -e "  \033[33m[INFO] No custom exchanges found to delete\033[0m"
    fi

    echo ""
    echo -e "\033[33m[*] Remaining exchanges:\033[0m"
    rabbitmqctl list_exchanges name type
fi

echo ""
echo -e "\033[33m[*] Final queue list:\033[0m"
rabbitmqctl list_queues name messages

echo -e "\n\033[32m[SUCCESS] Cleanup complete!\033[0m\n"
