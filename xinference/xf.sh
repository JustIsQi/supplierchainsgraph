docker rm -f xf
rm -rf /data/share2/yy/workspace/models/xprovence-reranker-bgem3-v1/cache/*
docker run -ti -d --privileged --gpus all -e "CUDA_VISIBLE_DEVICES=0" --name xf \
    -v /data/share2/yy/workspace/models/xprovence-reranker-bgem3-v1:/models -e XINFERENCE_HOME=/models \
    -p 9997:9997  \
    rt.zhixuncloud.cn:8443/xprobe/xinference:nightly-main-cu128 \
    xinference-local -H 0.0.0.0 -p 9997

# 等待容器启动
sleep 10

# 检查容器状态
echo "检查容器状态..."
docker ps | grep xf

# 在容器内注册模型（使用 trust_remote_code 参数）
echo "注册模型..."
docker exec xf xinference register \
  --model-type rerank \
  --file /models/XProvence.json \
  --persist \
 

# 检查注册是否成功
echo "检查已注册的模型..."
docker exec xf xinference list

# 在容器内启动模型（添加 trust_remote_code）
echo "启动模型..."
docker exec xf xinference launch \
  --model-name XProvence \
  --model-type rerank \

