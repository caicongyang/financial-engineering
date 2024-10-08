

conda list --export | awk -F "=" '{print $1 "==" $2}' > requirements.txt
