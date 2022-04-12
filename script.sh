if [ -f "value.conf" ]; then
  . value.conf
else
  echo "value.conf file not found"
  exit 1
fi

typeset date_time=`date "+%Y%m%d.%H%M%S"`
export p_id=$$
export script="script.sh"

checking_values()
{
  echo "Running script.sh <dir_name> <collection_name>"
  echo "dir_name is required"
  echo "collection_name is required"
}

export dir_name=$1
export collection_name=$2

if [ $# != 2 ]; then
  checking_values
fi
export logs="pragma_${dir_name}_${date_time}.log"
LogEntry "directory name : ${dir_name} collection name : ${collection_name}"
echo "$p_id|$dir_name|$collection_name" >>payload/payload.csv
# virtual_env=path
# source "${virtual_env}/activate"
command="python3 az_file.py" >>logs/result.logs
echo "executing : ${command}"
exit "$?"
