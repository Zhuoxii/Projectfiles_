spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors  4 \
    a.py \
    --output $1
