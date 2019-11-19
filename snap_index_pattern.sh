#!/bin/bash
# ./snap_index_pattern.sh 
# This script can snapshot to the container "$2":
# 	a. a single index passed as argument in "$1"
# 	b. all indexes matching the given pattern "$1"
# 	c. comma separated list of indexes given in "$1"
# The snapshot is named snap-<index_name>. 
# If you pass a non-existing index name, the script will log an error and exit.
# Note that ALL indices will be snapshotted to the SAME container.
# Blame sandeepkanabar@gmail.com for any bugs

if [ "$#" -ne 2 ]; then
        echo "#############################################################"
        echo ""
        echo "? Usage: $0 <index_pattern_or_index_name_comma_separated_index_names> <container_name>"
        echo "eg $0 indexA-* prod-repo"
        echo "eg $0 indexA-2019.10.01 prod-repo"
        echo "eg $0 indexA-2019.10.01,indexA-2019.10.02 prod-repo"
        echo ""
        echo "#############################################################"
        exit 1
fi

LOG_FILE=snap_pattern.log

ES_HOST=$(hostname -s)
ESHP="$ES_HOST:9200"

repo=$2
index_pattern=$1
username=elastic
password=changeme

MAIL_TO="receiver@foo.com"
MAIL_FROM="sender@foo.com"

es_cluster_name=$(/usr/bin/curl -s -u "$username:$password" "$ESHP?pretty" | python -c "import sys, json; print json.load(sys.stdin)['cluster_name']")

# Get all the indices matching the pattern. 
# Exclude current day's index using sed \$d 
function get_indices_list() {
    curl -s -u "$username:$password" "$ESHP/_cat/indices/$index_pattern?h=index&s=index" | awk "{ print $1 }" | sed \$d | tee -a "$LOG_FILE" 2>&1
}

function send_mail() {
	subject="$1 Snapshot Success"
	tail -n 26 daily_backup.log | mailx -s "$subject" -r "$MAIL_FROM" "$MAIL_TO"
}

function create_snapshots() {
	echo "==================================================================================" | tee -a "$LOG_FILE"

	echo "indices_list is :" | tee -a "$LOG_FILE"
	indices_list=$(get_indices_list)

	if echo "$indices_list" | grep -i 'error'
    then
		echo | tee -a "$LOG_FILE"
		status_msg="Either the given index / pattern $index_pattern is wrong or else the given index doesn't exist on cluster $es_cluster_name."
		echo "$status_msg" | tee -a "$LOG_FILE"
		echo | tee -a "$LOG_FILE"
		echo "$status_msg" | mailx -s "Snapshot Failed for $index_pattern on $es_cluster_name" -r "$MAIL_FROM" "$MAIL_TO"
		exit 2
	fi

    success_count=0
    failure_count=0
    count=0
    for index in $indices_list; do
    	((count++))
    	begin_date=$(date)
    	echo "$count: Index $index snapshot started at $begin_date" | tee -a "$LOG_FILE"
    	snapshot=snap-$index
		continue
    	ret_value=$(/usr/bin/curl -s -u "$username:$password" -XPUT "${ESHP}/_snapshot/$repo/$snapshot?pretty&wait_for_completion=true" \
	      -d "{ \"indices\": \"$index\", \"include_global_state\": false }" | tee -a "$LOG_FILE" 2>&1)

		if echo "$ret_value" | grep -i 'error'; then
			((failure_count++))
			echo | tee -a "$LOG_FILE"
			echo "$ret_value" | tee -a "$LOG_FILE"
			echo | tee -a "$LOG_FILE"
			echo "$ret_value" | mailx -s "Snapshot Failed for $index on $es_cluster_name" -r "$MAIL_FROM" "$MAIL_TO"
		else
			((success_count++))
			snap_status="$index snapshot finished at $(date) on cluster $es_cluster_name"
	        echo "$snap_status" | tee -a "$LOG_FILE"
	        echo "==================================================================================" |
	        send_mail "$index"
		fi
	done
	echo "====================================================" | tee -a "$LOG_FILE"
	end_date=$(date)
	final_status=" Total indices processed: $count \n Successful: $success_count \n Failed: $failure_count \n Index Pattern: $index_pattern \n Cluster: $es_cluster_name \n Begin Date: $begin_date \n End Date: $end_date"
	sleep 10
	echo -e "$final_status" | tee -a "$LOG_FILE"
	##echo -e "$final_status" | mailx -s "$index_pattern Snapshot Process Summary" -r "$MAIL_FROM" "$MAIL_TO"
	echo "====================================================" | tee -a "$LOG_FILE"		
}

# MAIN
create_snapshots
