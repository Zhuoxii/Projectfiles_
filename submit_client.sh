spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors  10 \
    workk.py \
    --output $1
