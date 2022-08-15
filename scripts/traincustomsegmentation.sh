#!/bin/bash
# Script intended to be run to train ersatz models
# The setup should be iso 696-3 language codes as directories containing
# training and validation data.

LANG_DIR=$1
ERSATZ_DIR=$2
LANG=$(basename $LANG_DIR)

SPM_PATH=$LANG_DIR/$LANG.ersatz.model
LEFT_SIZE=15
RIGHT_SIZE=5
TRAIN_OUTPUT_PATH=$LANG_DIR/ersatz-train.txt
INPUT_TRAIN_FILE_PATH=$LANG_DIR/train.txt
SHUFFLED_TRAIN_OUTPUT_PATH=$LANG_DIR/train-shuffled.txt

VALIDATION_OUTPUT_PATH=$LANG_DIR/ersatz-dev.txt
INPUT_DEV_FILE_PATHS=$LANG_DIR/dev.txt
MODELS_PATH=$LANG_DIR/models
mkdir -p $MODELS_PATH
LOGDIR=$LANG_DIR/logs
mkdir -p $LOGDIR

python $ERSATZ_DIR/dataset.py \
    --sentencepiece_path $SPM_PATH \
    --left-size $LEFT_SIZE \
    --right-size $RIGHT_SIZE \
    --output_path $TRAIN_OUTPUT_PATH \
    --input_paths $INPUT_TRAIN_FILE_PATH

shuf $TRAIN_OUTPUT_PATH > $SHUFFLED_TRAIN_OUTPUT_PATH

python $ERSATZ_DIR/dataset.py \
    --sentencepiece_path $SPM_PATH \
    --left-size $LEFT_SIZE \
    --right-size $RIGHT_SIZE \
    --output_path $VALIDATION_OUTPUT_PATH \
    --input_paths $INPUT_DEV_FILE_PATHS

python $ERSATZ_DIR/trainer.py \
  --sentencepiece_path=$SPM_PATH \
  --train_path $SHUFFLED_TRAIN_OUTPUT_PATH \
  --valid_path $VALIDATION_OUTPUT_PATH \
  --left_size=$LEFT_SIZE \
  --right_size=$RIGHT_SIZE \
  --output_path=$MODELS_PATH \
  --max-epochs=50 \
  --tb_dir=$LOGDIR \
  --eos_weight=200 \

