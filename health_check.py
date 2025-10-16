import subprocess,time,os,logging
import requests  # 添加这个导入
from concurrent.futures import ThreadPoolExecutor, as_completed  # 添加多线程支持
import socket  # 添加socket模块用于获取IP地址

def setup_logger():
    """设置日志记录"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_file_path = f'/data/home/yy/logs/restart_container.log'
    
    # 创建日志目录
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

def container_restart(ip,port,container_name):
    
    docker_command = [
        'docker', 'restart', container_name
    ]#  定义要执行的docker run命令及其参数
    
    try:
        result = subprocess.run(docker_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f"Container restarted successfully: {ip},{port},{result.stdout.decode().strip()} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except subprocess.CalledProcessError as e:
        logger.error(f"An error occurred while trying to start the container: {ip},{port}, {e.stderr.decode()} at {time.strftime("%Y-%m-%d %H:%M:%S")}")



def health_check(url,timeout):
    response = requests.get(url,timeout=timeout)
    return response.status_code

def check_container_status(ip,port):
    try:
        health_url = f"http://{ip}:{port}/health/"
        liveness_code = health_check(health_url,10)
    except Exception as e:
        liveness_code = 503

    try:
        health_generate_url = f"http://{ip}:{port}/health_generate/"
        readiness_code = health_check(health_generate_url,60)
    except Exception as e:
        readiness_code = 503

    return liveness_code,readiness_code


def single_server_check(ip,port,container_name):
    liveness_code,readiness_code = check_container_status(ip,port)
    if liveness_code == 200 and readiness_code == 200:
        logger.info(f"Server {ip}:{port} {container_name} is running")
    else:
        container_restart(ip,port,container_name)
        time.sleep(60)
        liveness_code,readiness_code = check_container_status(ip,port)
        if liveness_code == 200 and readiness_code == 200:
            logger.info(f"Server {ip}:{port} {container_name} is running")
        else:
            logger.error(f"Server {ip}:{port} {container_name} restart failed")

def get_local_ip():
    """获取本机IP地址"""
    try:
        # 创建一个UDP socket连接到外部地址，获取本机IP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            # 连接到一个外部地址（不会实际发送数据）
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
        return local_ip
    except Exception as e:
        # 如果上述方法失败，尝试获取主机名对应的IP
        try:
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            return local_ip
        except Exception as e2:
            # 最后的备选方案，返回localhost
            print(f"Warning: Could not determine local IP, using localhost. Error: {e2}")
            return "127.0.0.1"

logger = setup_logger()

# 获取本机IP地址（只调用一次）
LOCAL_IP = get_local_ip()

server_list = [
    {"ip":LOCAL_IP,"port":"7001","container_name":"qwen1"},
    {"ip":LOCAL_IP,"port":"7002","container_name":"qwen2"},
   
]

maps = {"172.16.0.11":"1号机",
    "172.16.0.12":"2号机",
    "172.16.0.13":"3号机",
    "172.16.0.14":"4号机",
    "172.16.0.15":"5号机",
    "172.16.0.16":"6号机",
    "172.16.0.17":"7号机",
    "172.16.0.19":"8号机",
}


def main():
    # 使用线程池进行并发执行
    max_workers = min(len(server_list), 12)  # 最多使用10个线程，避免资源消耗过大
    logger.info(f"Starting health check for {len(server_list)} servers with {max_workers} concurrent threads")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务到线程池
        future_to_server = {
            executor.submit(single_server_check, server['ip'], server['port'], server['container_name']): server
            for server in server_list
        }
        
        # 等待所有任务完成
        for future in as_completed(future_to_server):
            server = future_to_server[future]
            try:
                future.result()  # 获取结果，如果有异常会抛出
                # logger.info(f"Completed check for server {server['ip']}:{server['port']}")
            except Exception as exc:
                logger.error(f"Server {server['ip']}:{server['port']} generated an exception: {exc}")
    
    logger.info(f"********************************************************{maps[LOCAL_IP]} server health checks completed***********************************************\n")

if __name__ == "__main__":
    main()