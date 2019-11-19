#!/bin/bash
# ./purge_snapshots.sh 
# This script can delete/purge Snapshots from the container "$2":
# 	a. a single snapshot passed as argument in "$1"
# 	b. comma separated list of snapshots given in "$1"
# If you pass a non-existing snapshot, the script will log an error and exit.
# Blame sandeepkanabar@gmail.com for any bugs

if [ "$#" -ne 2 ]; then
        echo "#############################################################"
        echo ""
        echo "? Usage: $0 <snapshot_name_or_comma_separated_snapshot_names> <container_name>"
        echo "eg $0 snap-indexA-2017-12-31 prod-repo"
        echo "eg $0 snap-indexA-2017-12-31,snap-indexA-2018-01-01 colp-log"		
        echo ""
        echo "#############################################################"
        exit 1
fi

LOG_FILE=purge_snapshots.log

ES_HOST=$(hostname -s)
ESHP="$ES_HOST:9200"

repo=$2
snapshot_names=$1
username=elastic
password=changeme

MAIL_FROM="sender@foo.com"
MAIL_TO="receiver1@foo.com,receiver2@foo.com"

es_cluster_name=$(/usr/bin/curl -s -u "$username:$password" "$ESHP?pretty" | python -c "import sys, json; print json.load(sys.stdin)['cluster_name']")

function send_mail() {
	echo "$1" | mailx -s "$2" -r "$MAIL_FROM" "$MAIL_TO"	
}

function purge_snapshots() {
	echo "==================================================================================" | tee -a "$LOG_FILE"

	echo "snapshot_names is : $snapshot_names" | tee -a "$LOG_FILE"
	
	#Convert the comma-separated snapshot names into space separated snapshot names
	# value1,value2 becomes value1 value2
	# snap-indexA-2017-12-31,snap-indexA-2018-01-01 becomes 
	# snap-indexA-2017-12-31 snap-indexA-2018-01-01
	# If only a single value is passed, it remains the same
	snapshot_list=${snapshot_names//,/ }
	echo "snapshot_list is : $snapshot_list" | tee -a "$LOG_FILE"

    success_count=0
    count=0
    for snapshot in $snapshot_list; do
    	((count++))
    	begin_date=$(date)
    	echo "$count: Snapshot $snapshot purging started at $begin_date" | tee -a "$LOG_FILE"
    	purge_status=$(/usr/bin/curl -s -u "$username:$password" -XDELETE "${ESHP}/_snapshot/$repo/$snapshot?pretty" \
	    | python -c "import sys, json; print json.load(sys.stdin)['acknowledged']" | tee -a "$LOG_FILE" 2>&1)

		if [ "$purge_status" == "True" ]
		then 
			((success_count++))
			purged_msg="$success_count: snapshot $snapshot purged at $(date) on cluster $es_cluster_name"
	        echo "$purged_msg" | tee -a "$LOG_FILE"
			send_mail "$purged_msg" "$snapshot purged on $es_cluster_name"
		fi
	done
	echo "====================================================" | tee -a "$LOG_FILE"
	end_date=$(date)
	sleep 5	
	final_status=" Total Snapshots Processed: $count \n Successfully Purged: $success_count \n Snapshot Names: $snapshot_names \n Cluster: $es_cluster_name \n Begin Date: $begin_date \n End Date: $end_date"
	echo -e "$final_status" | tee -a "$LOG_FILE"
	echo -e "$final_status" | mailx -s "Snapshot Purge Summary $end_date" -r "$MAIL_FROM" "$MAIL_TO"
	echo "====================================================" | tee -a "$LOG_FILE"		
}

# MAIN
purge_snapshots