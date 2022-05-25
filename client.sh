spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 3 \
    A2.py \
    --output $1 
