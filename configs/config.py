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