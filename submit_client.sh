spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 6 \
    new.py \
    --output $1
