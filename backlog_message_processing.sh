#!/bin/bash

# Title           : backlog_message_processing.sh
# Description     : Takes automated recovery actions when a backlog in a DB message queue is detected.
# Author          : Jibin Varghese (Generic version by AI Assistant)
# Version         : 2.0
#=======================================================================================

#======================================================
#               CONFIGURATION VARIABLES
#      (Modify these values to match your environment)
#======================================================

# --- System & Application Paths ---
APP_HOME="/u/netwdm2"
ORACLE_HOME="/opt/dbms/oracle/product/11.2.0/client64"
JAVA_HOME="/opt/java/jdk1.6.0_39"
TNS_ADMIN="${APP_HOME}"
# The full path to your listener control script/executable
LISTENER_EXECUTABLE="${APP_HOME}/bin/lnplistener"

# --- Logging ---
LOG_DIR="${APP_HOME}/backlog_monitor"
# The log file this script writes its own actions to.
AUTOMATION_LOG_FILE="${LOG_DIR}/backlog_message_process.log"
# The status log file written by the validation script (READ-ONLY for this script)
VALIDATION_LOG_FILE="${LOG_DIR}/backlog_message_validation.log"
# The console log for the application listener to check for errors
LISTENER_CONSOLE_LOG="${APP_HOME}/log/lnplistener_console.log"

# --- Database & Application ---
DB_USER="user"
DB_PASSWORD="pass" # IMPORTANT: Use Oracle Wallet in production
DB_TNS_NAME="db_tns"

# Table and column names for fetching stuck batches
# IMPORTANT: Change these to match your application's schema.
QUEUE_TABLE="sv_message_queue"
BACKLOG_TABLE="sv_message_queue_hist"
BATCH_COLUMN="batch_seq"

# The URL endpoint to force reprocessing of a batch. The batch ID will be appended.
PROCESSING_URL="http://host:port/npac?id="
# The network port your application listener runs on
LISTENER_PORT="9523"
# A pipe-separated list of error patterns to search for in the listener log
LOG_ERROR_PATTERN="lang|Exception|OutOfMemory"

# --- Alerting ---
EMAIL_RECIPIENTS="your_mail@lumen.com"
EMAIL_SUBJECT_PREFIX="[Automation Script] Backlog in SV Message Queue"

# --- Behavior Tuning ---
STUCK_THRESHOLD_MINUTES=15
VALIDATION_SLEEP_SECONDS=60
VALIDATION_LOOP_COUNT=4

#======================================================
#               END OF CONFIGURATION
#======================================================

# Redirect all script output to the automation log file
exec >> ${AUTOMATION_LOG_FILE} 2>&1

# Set up the environment
export ORACLE_HOME JAVA_HOME TNS_ADMIN
export PATH=${ORACLE_HOME}/bin:${JAVA_HOME}/bin:${PATH}:${APP_HOME}/bin

# Form the database connection string
DB_CONNECTION="${DB_USER}/${DB_PASSWORD}@${DB_TNS_NAME}"

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') CDT - $1"
}

# --- Reusable Functions ---

# Starts the application listener
function start_listener() {
    log "Attempting to start listener..."
    ${LISTENER_EXECUTABLE} start
}

# Bounces (stops and starts) the application listener
function bounce_listener() {
    log "Bouncing listener..."
    ${LISTENER_EXECUTABLE} stop

    # Wait until the port is free
    retries=6
    while [[ ${retries} -gt 0 ]] && netstat -an | grep -w "${LISTENER_PORT}" >/dev/null; do
        log "Listener port ${LISTENER_PORT} is still in use. Waiting 5 seconds..."
        sleep 5
        retries=$((retries - 1))
    done

    if [[ ${retries} -eq 0 ]]; then
        log "ERROR: Listener did not stop correctly. Manual intervention required."
        # Optionally send an email here
        exit 1
    fi

    log "Listener stopped successfully. Starting it again."
    start_listener
}

# Queries the DB for stuck batches and triggers reprocessing via URL
function process_stuck_batches() {
    log "Querying for top stuck batches..."
    local BATCH_DATA
    BATCH_DATA=$(sqlplus -s "${DB_CONNECTION}" <<EOF
    set heading off feedback off pagesize 0
    select ${BATCH_COLUMN} from ${QUEUE_TABLE} WHERE ${BATCH_COLUMN} IS NOT NULL group by ${BATCH_COLUMN} order by 1 fetch first 4 rows only;
    exit;
EOF
)
    if [[ -z "${BATCH_DATA}" ]]; then
        log "No non-null batch sequences found to process. Checking for null batches."
        local null_count
        null_count=$(sqlplus -s "${DB_CONNECTION}" <<< "set heading off feedback off pagesize 0; select count(*) from ${QUEUE_TABLE} where ${BATCH_COLUMN} is null;")
        
        local email_body="No stuck messages with a BATCH_SEQ found.
If the issue persists, there might be ${null_count} messages with a NULL batch ID.
These may require manual clearing."
        log "${email_body}"
        echo -e "${email_body}" | mail -s "${EMAIL_SUBJECT_PREFIX} - No Batches to Process" "${EMAIL_RECIPIENTS}"
        exit 0
    fi

    log "Found stuck batches. Triggering reprocessing via URL..."
    echo "${BATCH_DATA}" | while read -r batch_id; do
        # Trim whitespace
        batch_id=$(echo "${batch_id}" | xargs)
        if [[ -n "${batch_id}" ]]; then
            local url="${PROCESSING_URL}${batch_id}"
            log "Calling URL for batch ID ${batch_id}: ${url}"
            curl -s -o /dev/null "$url"
            sleep 1 # Small delay between calls
        fi
    done
}

# A reusable function to check if the message count is decreasing
# Takes the initial count as an argument
# Returns a status string: "CLEAR", "IMPROVING", or "STUCK"
function run_validation_check() {
    local initial_count=$1
    log "Starting validation. Initial count: ${initial_count}. Monitoring for $((${VALIDATION_SLEEP_SECONDS} * ${VALIDATION_LOOP_COUNT})) seconds."

    local latest_count=${initial_count}
    for i in $(seq 1 ${VALIDATION_LOOP_COUNT}); do
        sleep ${VALIDATION_SLEEP_SECONDS}
        latest_count=$(sqlplus -s "${DB_CONNECTION}" <<< "set heading off feedback off pagesize 0; select count(*) from ${BACKLOG_TABLE} where delete_ts is null and insert_ts < sysdate - ${STUCK_THRESHOLD_MINUTES}/1440;")
        latest_count=$(echo "${latest_count}" | xargs)
        log "Validation check ${i}/${VALIDATION_LOOP_COUNT}: Current count is ${latest_count}."
    done

    if [[ ${latest_count} -eq 0 ]]; then
        echo "CLEAR"
    elif [[ ${latest_count} -lt ${initial_count} ]]; then
        echo "IMPROVING"
    else
        echo "STUCK"
    fi
}

# --- Main Script Logic ---

# Read the last line of the validation log to see if action is needed
LASTLINE=$(tail -n 1 "${VALIDATION_LOG_FILE}")

if [[ $LASTLINE != *"Messages are Stuck."* ]] && [[ $LASTLINE != *"Message Count is increasing."* ]]; then
    log "No 'Stuck' or 'Increasing' status found. No action needed. Exiting."
    exit 0
fi

log "Problem detected: '${LASTLINE}'. Starting automated recovery process."

# --- Stage 1: Initial Check and Action ---

# Check if the listener process is running
log "Checking listener status on port ${LISTENER_PORT}."
if netstat -an | grep -w "${LISTENER_PORT}" >/dev/null; then
    log "Listener is running."
    # Check listener log for errors
    if grep -E -q "${LOG_ERROR_PATTERN}" "${LISTENER_CONSOLE_LOG}"; then
        log "ERROR pattern found in listener log. Bouncing the listener."
        bounce_listener
    else
        log "No error pattern found in listener log."
    fi
else
    log "Listener is DOWN. Attempting to start it."
    start_listener
    sleep 5 # Give it time to start
    if netstat -an | grep -w "${LISTENER_PORT}" >/dev/null; then
        log "Listener started successfully."
        echo "The application listener was found to be down and has been restarted by the automation script." | mail -s "${EMAIL_SUBJECT_PREFIX} - Listener Restarted" "${EMAIL_RECIPIENTS}"
    else
        log "CRITICAL: Failed to start the listener. Manual intervention required."
        echo "The application listener was found to be down and COULD NOT be restarted automatically. Please investigate immediately." | mail -s "${EMAIL_SUBJECT_PREFIX} - CRITICAL: Listener Start Failed" "${EMAIL_RECIPIENTS}"
        exit 1
    fi
fi

# Get current count before taking action
INITIAL_COUNT=$(sqlplus -s "${DB_CONNECTION}" <<< "set heading off feedback off pagesize 0; select count(*) from ${BACKLOG_TABLE} where delete_ts is null and insert_ts < sysdate - ${STUCK_THRESHOLD_MINUTES}/1440;")
INITIAL_COUNT=$(echo "${INITIAL_COUNT}" | xargs)
if [[ ${INITIAL_COUNT} -eq 0 ]]; then
    log "Count is already zero. Problem may have self-resolved. Exiting."
    exit 0
fi

log "Attempting to reprocess stuck batches (Attempt 1)..."
process_stuck_batches

# --- Stage 2: First Validation ---

STATUS=$(run_validation_check "${INITIAL_COUNT}")
if [[ "$STATUS" == "CLEAR" || "$STATUS" == "IMPROVING" ]]; then
    log "SUCCESS: Message count is decreasing after initial action. Exiting."
    exit 0
fi

# --- Stage 3: Escalation (Bounce and Reprocess) ---

log "WARNING: Queue is still stuck after first attempt. Bouncing listener and trying again."
bounce_listener

# Get new count before second attempt
CURRENT_COUNT=$(sqlplus -s "${DB_CONNECTION}" <<< "set heading off feedback off pagesize 0; select count(*) from ${BACKLOG_TABLE} where delete_ts is null and insert_ts < sysdate - ${STUCK_THRESHOLD_MINUTES}/1440;")
CURRENT_COUNT=$(echo "${CURRENT_COUNT}" | xargs)

log "Attempting to reprocess stuck batches (Attempt 2)..."
process_stuck_batches

# --- Stage 4: Final Validation ---

STATUS=$(run_validation_check "${CURRENT_COUNT}")
if [[ "$STATUS" == "CLEAR" || "$STATUS" == "IMPROVING" ]]; then
    log "SUCCESS: Message count is decreasing after bouncing listener. Exiting."
    exit 0
fi

# --- Stage 5: Final Alert ---

FINAL_COUNT=$(sqlplus -s "${DB_CONNECTION}" <<< "set heading off feedback off pagesize 0; select count(*) from ${BACKLOG_TABLE} where delete_ts is null and insert_ts < sysdate - ${STUCK_THRESHOLD_MINUTES}/1440;")
FINAL_COUNT=$(echo "${FINAL_COUNT}" | xargs)

log "FAILURE: Automated recovery failed. Sending alert for manual intervention."
EMAIL_BODY="Automated recovery for the SV Message Queue has failed.
The message count remains stuck or is increasing after all automated steps were taken.

Actions Performed:
1. Checked/Restarted Listener
2. Reprocessed top batches via URL
3. Bounced Listener
4. Reprocessed top batches again

The current backlog count is: ${FINAL_COUNT}.

RECOMMENDATION:
1. Manually investigate the application and database logs for the root cause.
2. Disable the automation cron jobs to prevent repeated alerts.
3. Once the issue is resolved and the queue is clear, re-enable the cron jobs."

echo -e "${EMAIL_BODY}" | mail -s "${EMAIL_SUBJECT_PREFIX} - ACTION REQUIRED: Automated Recovery Failed" "${EMAIL_RECIPIENTS}"

exit 1
