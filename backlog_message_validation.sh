#!/bin/bash

# Title           : backlog_message_validation.sh
# Description     : Monitors a database message queue for backlogs and reports the status.
# Author          : Jibin Varghese (Generic version by AI Assistant)
# Version         : 2.0
#===================================================================================

#======================================================
#               CONFIGURATION VARIABLES
#      (Modify these values to match your environment)
#======================================================

# --- System & Application Paths ---
# Base directory for your application home
APP_HOME="/u/netwdm2"
# Path to the Oracle client installation
ORACLE_HOME="/opt/dbms/oracle/product/11.2.0/client64"
# Path to your tnsnames.ora file
TNS_ADMIN="${APP_HOME}"

# --- Logging ---
# Directory where logs should be stored
LOG_DIR="${APP_HOME}/backlog_monitor"
# The log file this script writes to, and the processing script reads from.
VALIDATION_LOG_FILE="${LOG_DIR}/backlog_message_validation.log"

# --- Database & Application ---
# Database credentials.
# IMPORTANT: For production, use an Oracle Wallet instead of plain-text passwords.
DB_USER="user"
DB_PASSWORD="pass"
DB_TNS_NAME="db_tns"

# The name of the table to check for a backlog
# IMPORTANT: Change this to match your application's schema.
BACKLOG_TABLE="sv_message_queue_hist"

# --- Alerting ---
EMAIL_RECIPIENTS="mail_id@lumen.com"
EMAIL_SUBJECT_PREFIX="[Validation Script] Backlog in SV Message Queue"

# --- Behavior Tuning ---
# The age of a message (in minutes) to be considered part of the backlog
STUCK_THRESHOLD_MINUTES=15
# Seconds to wait between checking the message count
VALIDATION_SLEEP_SECONDS=90
# The number of times to loop while checking
VALIDATION_LOOP_COUNT=4

#======================================================
#               END OF CONFIGURATION
#======================================================

# Set environment variables for Oracle
export ORACLE_HOME
export PATH=${ORACLE_HOME}/bin:${PATH}
export TNS_ADMIN

# Form the database connection string
# For Oracle Wallet, you would use: DB_CONNECTION="/@${DB_TNS_NAME}"
DB_CONNECTION="${DB_USER}/${DB_PASSWORD}@${DB_TNS_NAME}"

# Function to get the current backlog count from the database
get_message_count() {
    sqlplus -s "${DB_CONNECTION}" <<EOF
set heading off
set feedback off
set pagesize 0
select count(*) from ${BACKLOG_TABLE} where delete_ts is null and insert_ts < sysdate - ${STUCK_THRESHOLD_MINUTES}/1440;
exit;
EOF
}

# --- Main Script Logic ---

echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Starting validation check."

MESSAGE_COUNT=$(get_message_count)
# Clean up any whitespace from sqlplus output
MESSAGE_COUNT=$(echo "${MESSAGE_COUNT}" | xargs)

echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Initial message count is ${MESSAGE_COUNT}."

# If there are no messages, log it and exit.
if [[ ${MESSAGE_COUNT} -eq 0 ]]; then
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Current Count is Zero. No messages to process." >> "${VALIDATION_LOG_FILE}"
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Exiting."
    exit 0
fi

# Loop for a few cycles to observe the trend
echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Monitoring queue for $((${VALIDATION_SLEEP_SECONDS} * ${VALIDATION_LOOP_COUNT})) seconds..."
for i in $(seq 1 ${VALIDATION_LOOP_COUNT}); do
    sleep ${VALIDATION_SLEEP_SECONDS}
    # We only need the final count after the total wait time
    if [[ ${i} -eq ${VALIDATION_LOOP_COUNT} ]]; then
        NEW_MESSAGE_COUNT=$(get_message_count)
        NEW_MESSAGE_COUNT=$(echo "${NEW_MESSAGE_COUNT}" | xargs)
    fi
done

echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Final message count is ${NEW_MESSAGE_COUNT}."

# Compare the initial and final counts to determine the status
if [[ ${NEW_MESSAGE_COUNT} -eq 0 ]]; then
    STATUS_MESSAGE="Messages Cleared."
    EMAIL_BODY="Backlog Was found in SV_Message queue. All messages have been processed by the listener.
Current count is ${NEW_MESSAGE_COUNT}."
elif [[ ${NEW_MESSAGE_COUNT} -lt ${MESSAGE_COUNT} ]]; then
    STATUS_MESSAGE="Messages are being processed."
    EMAIL_BODY="Backlog found in SV_Message queue. Messages are being processed by the listener.
Current count is ${NEW_MESSAGE_COUNT}."
elif [[ ${NEW_MESSAGE_COUNT} -eq ${MESSAGE_COUNT} ]]; then
    STATUS_MESSAGE="Messages are Stuck." # This is a trigger for the processing script
    EMAIL_BODY="Backlog found in SV_Message queue. The message count has not changed, indicating a stuck process.
Current count is ${NEW_MESSAGE_COUNT}."
else # NEW_MESSAGE_COUNT > MESSAGE_COUNT
    STATUS_MESSAGE="Message Count is increasing." # This is also a trigger
    EMAIL_BODY="Backlog found in SV_Message queue. The message count is increasing.
Current count is ${NEW_MESSAGE_COUNT}."
fi

# Write the final status to the log file for the processing script
echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - ${STATUS_MESSAGE}" >> "${VALIDATION_LOG_FILE}"

# Send an informational email
echo -e "$(date +'%Y-%m-%d %H:%M:%S') CDT - ${EMAIL_BODY}" | mail -s "${EMAIL_SUBJECT_PREFIX} - Status: ${STATUS_MESSAGE}" "${EMAIL_RECIPIENTS}"

echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - Validation finished."
exit 0
