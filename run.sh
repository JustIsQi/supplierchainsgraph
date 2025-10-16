for i in {0..20}
do
    nohup python pipeline.py > /dev/null 2>&1 &
done