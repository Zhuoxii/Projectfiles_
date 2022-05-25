spark-submit \
    --master yarn \
    --deploy-mode client \
    word.py \
    --output $1
