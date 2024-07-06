#!/bin/bash

#Title           :Validation_Script - Backlog in SV Message Queue
#Description     :This is a validation script which monitors the sv_message queue backlog.
#author          :Jibin Varghese
#date            :22052023
#======================================================

# Define log file location
LOG_FILE="/u/netwdm2/backlog_monitor/backlog_message_validation.log"

# Define Oracle environment variables
export ORACLE_HOME=/opt/dbms/oracle/product/11.2.0/client64/
export PATH=/opt/dbms/oracle/product/11.2.0/client64//bin:/usr/local/bin:/bin:/usr/bin:/usr/bin:/bin:/opt/dbms/oracle/product/11.2.0/client64//bin:/u/netwdm2/bin
export TNS_ADMIN=/u/netwdm2

# Set the email recipients and subject
EMAIL_RECIPIENTS="OSS_CTL_PASE_CR@lumen.com"
EMAIL_SUBJECT="Validation_Script - Backlog in SV Message Queue"

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
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Current Count is Zero. No messages to proccess." >> ${LOG_FILE}
    exit 0
else
    # Start loop for checking count changes
    for i in {1..4}; do
        sleep 90
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
            echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Current Count is Zero. No messages to process." >> ${LOG_FILE}
            echo -e "$(date +'%Y-%m-%d %H:%M:%S') CDT - Backlog Was found in SV_Message queue. Messages are Fully processed by the listener. \nCurrent count is ${NEW_MESSAGE_COUNT}." | mail -s "${EMAIL_SUBJECT}" "${EMAIL_RECIPIENTS}"
            exit 0
        fi
    elif [[ ${NEW_MESSAGE_COUNT} -lt ${MESSAGE_COUNT} ]]; then
        if [[ ${i} -eq 4 ]]; then
            # Send an email indicating that messages are being processed automatically
            echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Messages are being processed by listener." >> ${LOG_FILE}
            echo -e "$(date +'%Y-%m-%d %H:%M:%S') CDT - Backlog found in SV_Message queue. Messages are being processed by the listener as of now.\nCurrent count is ${NEW_MESSAGE_COUNT}." | mail -s "${EMAIL_SUBJECT}" "${EMAIL_RECIPIENTS}"
        fi
    elif [[ ${NEW_MESSAGE_COUNT} -eq ${MESSAGE_COUNT} ]]; then
        if [[ ${i} -eq 4 ]]; then
            # Send an email indicating that message count is not reducing
            echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Messages are Stuck." >> ${LOG_FILE}
            echo -e "$(date +'%Y-%m-%d %H:%M:%S') CDT - Backlog found in SV_Message queue. Messages Seems to be in stuck state. The count hasnt reduced for the past few mins. \nCurrent count is ${NEW_MESSAGE_COUNT}." | mail -s "${EMAIL_SUBJECT}" "${EMAIL_RECIPIENTS}"
        fi
    else
        # Send an email indicating that message count is increasing
        echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Message Count is increasing." >> ${LOG_FILE}
        echo -e "$(date +'%Y-%m-%d %H:%M:%S') CDT - Backlog found in SV_Message queue. Count is increasing as of now. \nCurrent count is ${NEW_MESSAGE_COUNT}." | mail -s "${EMAIL_SUBJECT}" "${EMAIL_RECIPIENTS}"
    fi
fi