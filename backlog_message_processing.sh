#!/bin/bash

#Title           :Automation_Script - Backlog in SV Message Queue
#Description     :Take neccessary action when Backlog in SV Message Queue is found.
#author          :Jibin Varghese
#date            :22052023
#======================================================

exec >> /u/netwdm2/backlog_monitor/backlog_message_process.log 2>&1

export ORACLE_HOME=/opt/dbms/oracle/product/11.2.0/client64/
export PATH=/opt/dbms/oracle/product/11.2.0/client64//bin:/usr/local/bin:/bin:/usr/bin:/usr/bin:/bin:/opt/dbms/oracle/product/11.2.0/client64//bin:/u/netwdm2/bin
export TNS_ADMIN=/u/netwdm2
export MAIN_HOME=/u/netwdm2
export JAVA_HOME=/opt/java/jdk1.6.0_39/
export RV_HOME=/opt/tib/NETWDM2/rv8.3.2

# Set the email recipients and subject
EMAIL_RECIPIENTS="OSS_CTL_PASE_CR@lumen.com"
EMAIL_SUBJECT="Automation_Script - Backlog in SV Message Queue"

# Define the path to the log file
LOGFILE="/u/netwdm2/backlog_monitor/backlog_message_validation.log"

# Read the last line of the log file
LASTLINE=$(tail -n 1 $LOGFILE)

#############################################################Define functions
function start_listener() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Starting listener"
    /u/netwdm2/bin/lnplistener start
}

function bounce_listener() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Bouncing listener"
    /u/netwdm2/bin/lnplistener stop

    # Check if the listener is still running
    while netstat -an | grep 9523 >/dev/null; do
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Listener still running, waiting 5 more seconds..."
        sleep 5
    done

    # Start the listener
    echo "Starting listener"
    /u/netwdm2/bin/lnplistener start
}


function process_batch_seq() {
    # Run the SQL query and extract the first 4 rows
    BSC=$( sqlplus -s 'NM00/2022!NMp$wd'@cpacnmqp <<EOF
    set heading off
    set feedback off
    select batch_seq, count(*) from sv_message_queue WHERE BATCH_SEQ IS NOT NULL group by batch_seq order by 1 fetch first 4 rows only;
    exit;
EOF
)

    # Check if any values were returned
    if [ -z "$BSC" ]; then
        echo -e "$(date +'%Y-%m-%d %H:%M:%S') CDT - No stuck messages found while executing below  query \n\n'select batch_seq, count(*) from sv_message_queue WHERE BATCH_SEQ IS NOT NULL group by batch_seq order by 1 fetch first 4 rows only;'.\n\nIf the issue persists check whether there are any null batches and clear them post Validation." | mail -s "${EMAIL_SUBJECT}" "${EMAIL_RECIPIENTS}"
        exit 0
    fi

    # Declare an array to store the batch sequences and counts
    declare -a batch_seqs
    declare -a counts

    # Extract the first 4 (or less) batch sequences and their counts into the arrays
    i=0
    while read -r line; do
    if [[ "$line" =~ ^[[:space:]]*$ ]]; then
    # Skip blank lines
    continue
    fi

    # Extract the batch_seq and count from the line
    i=$((i+1))
    batch_seq="$(echo "$line" | awk '{print $1}')"
    count="$(echo "$line" | awk '{print $2}')"

    # Store the values in the arrays
    batch_seqs[$i]=$batch_seq
    counts[$i]=$count

    if [ "$i" -eq 4 ]; then
    # We have extracted the first 4 batch sequences and their counts, so exit the loop
    break
    fi
    done <<< "$BSC"

    # Print the batch sequences and counts
    for ((i=1; i<=4; i++)); do
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Batch Seq $i: ${batch_seqs[$i]}"
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Count $i: ${counts[$i]}"
    done

    # Pass the variables with values through the URL
    for ((i=1; i<=4; i++)); do
    if [ -n "${batch_seqs[$i]}" ]; then
        url="http://lxomavmap454.qintra.com:49999/npac?id=${batch_seqs[$i]}"
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Passing ${batch_seqs[$i]} through URL: $url"
        curl "$url"
    fi
    done
}

function validation() {
    # Get the total number of messages to be processed
    MESSAGE_COUNT=$( sqlplus -s 'NM00/2022!NMp$wd'@cpacnmqp <<EOF
    set heading off
    set feedback off
    select count(*) from sv_message_queue_hist where delete_ts is null and insert_ts < sysdate - 15/1440;
    exit;
EOF
)

    # Print the message count
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - There are ${MESSAGE_COUNT} messages to be processed."
    # Check if there are messages to be processed
    if [[ ${MESSAGE_COUNT} -eq 0 ]]; then
        # No messages to process, exit script
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Current Count is Zero. No messages to process. Exiting the script"
        exit 0
    else
        # Start loop for checking count changes
        for i in {1..4}; do
            sleep 60
            NEW_MESSAGE_COUNT=$( sqlplus -s 'NM00/2022!NMp$wd'@cpacnmqp <<EOF
            set heading off
            set feedback off
            select count(*) from sv_message_queue_hist where delete_ts is null and insert_ts < sysdate - 15/1440;
            exit;
EOF
)
        done
        if [[ ${NEW_MESSAGE_COUNT} -eq 0 ]]; then
            if [[ ${i} -eq 4 ]]; then
                # Send an email indicating that all messages have been processed
                echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Current Count is Zero. No messages to process. Exiting the script"
                exit 0
            fi
        elif [[ ${NEW_MESSAGE_COUNT} -lt ${MESSAGE_COUNT} ]]; then
            if [[ ${i} -eq 4 ]]; then
                # Send an email indicating that messages are being processed automatically
                echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Messages are being processed by listener. Exiting the script"
                exit 0
            fi
        else
            echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Messages are Stuck or increasing."
            echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Processing Batches through URL for 2nd time"
            process_batch_seq
            validation2
        fi
    fi
}

function validation2() {
    # Get the total number of messages to be processed
    MESSAGE_COUNT2=$( sqlplus -s 'NM00/2022!NMp$wd'@cpacnmqp <<EOF
    set heading off
    set feedback off
    select count(*) from sv_message_queue_hist where delete_ts is null and insert_ts < sysdate - 15/1440;
    exit;
EOF
)

    # Print the message count
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - There are ${MESSAGE_COUNT2} messages to be processed."
    # Check if there are messages to be processed
    if [[ ${MESSAGE_COUNT2} -eq 0 ]]; then
        # No messages to process, exit script
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Current Count is Zero. No messages to process. Exiting the script."
        exit 0
    else
        # Start loop for checking count changes
        for i in {1..4}; do
            sleep 60
            NEW_MESSAGE_COUNT2=$( sqlplus -s 'NM00/2022!NMp$wd'@cpacnmqp <<EOF
            set heading off
            set feedback off
            select count(*) from sv_message_queue_hist where delete_ts is null and insert_ts < sysdate - 15/1440;
            exit;
EOF
)
        done
        if [[ ${NEW_MESSAGE_COUNT2} -eq 0 ]]; then
            if [[ ${i} -eq 4 ]]; then
                # Send an email indicating that all messages have been processed
                echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Current Count is Zero. No messages to process. Exiting the script."
                exit 0
            fi
        elif [[ ${NEW_MESSAGE_COUNT2} -lt ${MESSAGE_COUNT2} ]]; then
            if [[ ${i} -eq 4 ]]; then
                # Send an email indicating that messages are being processed automatically
                echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Messages are being processed by listener. Exiting the script."
                exit 0
            fi
        else
            echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Messages are Stuck or increasing even after passing them through url, bouncing the listener and trying again."
            bounce_listener
            echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - initiating batch processing through URl for 3rd time."
            process_batch_seq
            final_validation
        fi
    fi
}

function final_validation() {
    # Get the total number of messages to be processed
    MESSAGE_COUNT3=$( sqlplus -s 'NM00/2022!NMp$wd'@cpacnmqp <<EOF
    set heading off
    set feedback off
    select count(*) from sv_message_queue_hist where delete_ts is null and insert_ts < sysdate - 15/1440;
    exit;
EOF
)

    # Print the message count
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - There are ${MESSAGE_COUNT3} messages to be processed."
    # Check if there are messages to be processed
    if [[ ${MESSAGE_COUNT3} -eq 0 ]]; then
        # No messages to process, exit script
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Current Count is Zero. No messages to process. Exiting the script"
        exit 0
    else
        # Start loop for checking count changes
        for i in {1..4}; do
            sleep 60
            NEW_MESSAGE_COUNT3=$( sqlplus -s 'NM00/2022!NMp$wd'@cpacnmqp <<EOF
            set heading off
            set feedback off
            select count(*) from sv_message_queue_hist where delete_ts is null and insert_ts < sysdate - 15/1440;
            exit;
EOF
)
        done
        if [[ ${NEW_MESSAGE_COUNT3} -eq 0 ]]; then
            if [[ ${i} -eq 4 ]]; then
                # Send an email indicating that all messages have been processed
                echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Current Count is Zero. No messages to process. Exiting the script"
                exit 0
            fi
        elif [[ ${NEW_MESSAGE_COUNT3} -lt ${MESSAGE_COUNT3} ]]; then
            if [[ ${i} -eq 4 ]]; then
                # Send an email indicating that messages are being processed automatically
                echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Messages are being processed by listener. Exiting the script"
                exit 0
            fi
        else
            echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Messages are Stuck or increasing. sending mail to stop the cron."
            echo -e "$(date +'%Y-%m-%d %H:%M:%S') CDT - Backlog Was found in SV_Message queue. Messages are stuck or increasing even after passing the batches through URl and bouncing the listener. Manual Intervention is needed.\nKindly disable the backlog cron jobs on lxomavmap454 server(netwdm2). Investigate the issue and take neccessary actions. WHen the count is Zero enable the Cron jobs.\nCurrent count is ${NEW_MESSAGE_COUNT3}." | mail -s "${EMAIL_SUBJECT}" "${EMAIL_RECIPIENTS}"
            exit 0
        fi
    fi
}

############################################# Check the contents of the last line and take action based on it
if [[ $LASTLINE == *"Messages are Stuck."* ]] || [[ $LASTLINE == *"Message Count is increasing."* ]]; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - SV messages are found to be stuck. Taking action..."
    # Get the total number of messages to be processed
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Getting count of messages to be processed."
    MESSAGE_COUNT=$( sqlplus -s 'NM00/2022!NMp$wd'@cpacnmqp <<EOF
    set heading off
    set feedback off
    select count(*) from sv_message_queue_hist where delete_ts is null and insert_ts < sysdate - 15/1440;
    exit;
EOF
)
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - There are ${MESSAGE_COUNT} messages to be processed."
    if [[ ${MESSAGE_COUNT} -eq 0 ]]; then
        # No messages to process, exit script
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Current Count is Zero. Exiting from script."
        exit 0
    fi

    #set the variables
    LOGFILE="/u/netwdm2/log/lnplistener_console.log"
    ERROR_PATTERN="lang"
    LISTENER_PID="9523"

    # Check if the listener is up
    echo "Checking listener status"
    if netstat -an | grep "$LISTENER_PID" >/dev/null; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Listener is already up"
    else
        start_listener

        if netstat -an | grep "$LISTENER_PID" >/dev/null; then
            echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Listener has been brought back up since it was already down"
            echo -e "$(date +'%Y-%m-%d %H:%M:%S') CDT - Listener has been brought back up since it was already down. The Automation script is still running and continuing its checks." | mail -s "${EMAIL_SUBJECT}" "${EMAIL_RECIPIENTS}"
        else
            echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Listener is still down after attempting to bring it back up. Please investigate. Exiting the Automation Script."
            echo -e "$(date +'%Y-%m-%d %H:%M:%S') CDT - Listener is still down after attempting to bring it back up. Please investigate." | mail -s "${EMAIL_SUBJECT}" "${EMAIL_RECIPIENTS}"
            exit 1
        fi
    fi

    # Check if the logfile contains any errors
    echo "Checking for error patterns in the console_log."
    if grep "$ERROR_PATTERN" "$LOGFILE"; then
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Error found in ${LOGFILE}. Bouncing the listener."
        bounce_listener
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - listener has been Bounced. Passing batches through URL."
    else
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - No error pattern found in the lnplistener_console.log."
    fi

    #processing batches through URL
    process_batch_seq
    validation

else
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - No stuck messages. Stopping the script"
    exit 0
fi
