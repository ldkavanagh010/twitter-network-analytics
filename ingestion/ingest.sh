cd verified_september
unzstd $1.zst
cd ..
python ingestion_script.py $1 $2
rm verified_september/$1
