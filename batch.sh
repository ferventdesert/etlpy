for((i=0; i<$1; ++i))  
do
 nohup  python src/distributed.py client $2 &
done  
