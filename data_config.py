"""
数据获取和存储的配置文件
"""
import os

# 数据库配置
DATABASE_PATH = os.environ.get('DATABASE_PATH', 'stock_data.db')

# 数据获取配置
DEFAULT_FETCH_DAYS = 365  # 默认获取近1年的数据
REQUEST_DELAY = 0.1  # 每次请求之间的延迟（秒），避免请求过快

# 数据存储配置
BATCH_SIZE = 1000  # 批量插入的批次大小

# 备份标志文件
BACKUP_FLAG_FILE = '.data_backup_flag'

# 日志配置
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# akshare配置（如果需要）
AKSHARE_TIMEOUT = 30  # 请求超时时间（秒）

