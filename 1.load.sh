#!/bin/bash

DATASTORE_SSH="/bin/bash -c"
PSQL_COPY_OPTIONS=""
DATA_ARCHIVE_PATH="$DATA_PATH/loaded"
PSQL_COPY_OPTIONS=""
SCRIPT_PATH=$(dirname "$(readlink -f "$0")")     # Local path on database front server (it is assumed that all scripts are in the same location as $0)
DATA_NULL_CHAR=""                                # A charater that is used if field is empty
DATA_COLUMN_DELIMITER="?"                       # A character that is used as a field separator in the raw data files
UNCOMP_SOFT="gunzip"                             # A software that is used to uncompress the raw data files, for example gunzip (.gz) or bunzip2 (.bunzip2)
DATA_FILES=(
  "Node_A_Alarm_Log*.txt.gz"   # 0
  "Node_A_Event_Log*.txt.gz"   # 1
  "Node_A_Restart_Log*.txt.gz" # 2
  "Node_B_Alarm_Log*.txt.gz"   # 3
  "Node_B_Event_Log*.txt.gz"   # 4
  "Node_B_to_Node_A_Mapping*.txt.gz" # 5
  "Node_A_Alarm_Severity*.txt.gz"    # 6 
  "Weather_data*.csv.gz"    # 7 
  ) 
DB_TABLES=(
  "tmp.node_a_alarm"   # 0
  "tmp.node_a_event"   # 1
  "tmp.node_a_restart" # 2
  "tmp.node_b_alarm"   # 3
  "tmp.node_b_event"   # 4
  "tmp.node_b_to_node_a" # 5
  "tmp.node_a_alarm_severity" # 6
  "tmp.weather" #7
  ) 


# Define beginning of the psql command
PSQL_CMD="psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -a -c"
echo "The beginning of the psql commnad : $PSQL_CMD"

# The error table
TABLE_ERROR="${TABLE}_err"

# Fetch raw data files that are uploaded to the database
DATASTORE_FILES=$($DATASTORE_SSH "find $DATASTORE_PATH -maxdepth 1 -name -type f -print | sort -f")


typeset -i PROCESSED_FILES=0
typeset -i LOADED_FILES=0
typeset -i ERROR_FILES=0
EXIT_CODE=0


for FILE in ${DATASTORE_FILES[@]}; do
  let PROCESSED_FILES=PROCESSED_FILES+1
  BASE_FILE=$(echo $FILE | xargs -n1 basename)
  if $DATASTORE_SSH "cat $FILE"  | $UNCOMP_SOFT  | sed "s,^,${BASE_FILE}${DELIMITER_SED},g" | tr -d "$REMCHARS" | $PSQL_CMD "SET CLIENT_ENCODING TO 'latin1'; COPY $TABLE FROM stdin WITH DELIMITER AS E'$DELIMITER' NULL AS '$NULLCHAR' $PSQL_COPY_OPTIONS LOG ERRORS INTO $TABLE_ERROR KEEP SEGMENT REJECT LIMIT 5000;"; then
     if [ "$DATASTORE_ARCHIVE_PATH" != "$DATASTORE_PATH" ]; then
        $DATASTORE_SSH "mv $FILE $DATASTORE_ARCHIVE_PATH"
        printFunc "File $FILE loaded to database and moved to $DATASTORE_ARCHIVE_PATH"
     fi
     let LOADED_FILES=LOADED_FILES+1
  else
      printFunc " ERROR: File $FILE was not loaded. Pipestatus = ( ${PIPESTATUS[@]} )"
      ## POSSIBLE DEV : Collect the names of unsuccessfully loaded files into one file
      #$DATASTORE_SSH "echo $FILE >> $DATASTORE_ERROR_FILE"
      let ERROR_FILES=ERROR_FILES+1
      EXIT_CODE=66
  fi
done