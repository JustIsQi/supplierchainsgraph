import os

mongo_url = "mongodb://omniFullAccessUser:fT9YDYAQfgZRHt@10.100.0.54:27019,10.100.0.55:27019,10.100.0.56:27019/OmniDataCrafter?retryWrites=true&loadBalanced=false&readPreference=primary&serverSelectionTimeoutMS=50000&socketTimeoutMS=300000&connectTimeoutMS=100000&authSource=OmniDataCrafter&authMechanism=SCRAM-SHA-256"

nebula_config = {
    'host': '10.100.0.205',
    'port': 9669,
    'user': 'root',
    'password': 'nebula'
}

elasticsearch = {
    "ip":os.getenv("ELASTICSEARCH_SEARCH_URL", "10.100.0.100"),
    "port":int(os.getenv("ELASTICSEARCH_SEARCH_PORT", 9200)),
    "user":os.getenv("ELASTICSEARCH_SEARCH_USER", "elastic"),
    "password":os.getenv("ELASTICSEARCH_SEARCH_PASSWORD", "6YCuCbNUf2ap"),
}

redis_config = {
    "host":"10.100.0.1",
    "port":6379,
    "password":"redis",
    "db":0,
}

REDIS_PENDING_KEY = "anno:pending"
# 临时正在处理队列（List）
REDIS_PROCESSING_KEY = "anno:processing"
# 每个字典在 Redis 里的 key 前缀
REDIS_DATA_KEY_PREFIX = "anno:data:"
# 异常解析文献列表
REDIS_MISS_DATA_KEY = "anno:error_data"

QWEN_INFERENCE_JSON_PATH = "/data/share2/yy/workspace/data/wind_anno_qwen_json"
QWEN_INFERENCE_MD_PATH = "/data/share2/yy/workspace/data/wind_anno_md"
QWEN_FILE_SUFFIX = "_qwen_thinking.json"