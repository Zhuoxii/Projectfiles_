spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors  10 \
    a.py \
    --output $1
