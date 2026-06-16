import redis

redis_pool = redis.ConnectionPool(
    host="redis",
    port=6379,
    db=0,
    decode_responses=True,
    max_connections=50,
    socket_timeout=0.5,       
    socket_connect_timeout=1.0, 
)

class ConexionRedis():
    def __init__(self):
        self.redis_pool = redis_pool
        self.r = redis.Redis(connection_pool=self.redis_pool)