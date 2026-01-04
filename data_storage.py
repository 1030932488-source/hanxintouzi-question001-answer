"""
数据存储模块
处理数据的SQL存储、增量更新和数据恢复
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import os
import logging
from typing import Optional

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 数据库文件路径
DB_PATH = 'stock_data.db'
BACKUP_FLAG_FILE = '.data_backup_flag'


def get_db_connection():
    """
    获取数据库连接
    
    Returns:
        sqlite3.Connection: 数据库连接对象
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # 使查询结果可以通过列名访问
    return conn


def init_database():
    """
    初始化数据库，创建表结构
    """
    logger.info("初始化数据库...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 创建股票日频数据表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(ts_code, trade_date)
        )
    ''')
    
    # 创建索引以提高查询性能
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ts_code ON stock_daily(ts_code)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_trade_date ON stock_daily(trade_date)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ts_code_date ON stock_daily(ts_code, trade_date)
    ''')
    
    conn.commit()
    conn.close()
    logger.info("数据库初始化完成")


def check_database_exists():
    """
    检查数据库文件是否存在
    
    Returns:
        bool: 数据库文件是否存在
    """
    return os.path.exists(DB_PATH)


def check_data_exists():
    """
    检查数据库中是否有数据
    
    Returns:
        bool: 是否有数据
    """
    if not check_database_exists():
        return False
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM stock_daily')
    count = cursor.fetchone()[0]
    conn.close()
    
    return count > 0


def save_data(df: pd.DataFrame, batch_size: int = 1000):
    """
    保存数据到数据库（批量插入）
    
    Args:
        df (pd.DataFrame): 要保存的数据
        batch_size (int): 每批插入的记录数
    """
    if df is None or df.empty:
        logger.warning("没有数据需要保存")
        return
    
    logger.info(f"开始保存 {len(df)} 条记录到数据库...")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 准备数据
    records = []
    for _, row in df.iterrows():
        record = (
            row.get('ts_code', ''),
            row.get('trade_date', ''),
            row.get('open'),
            row.get('high'),
            row.get('low'),
            row.get('close'),
            row.get('volume'),
            row.get('amount')
        )
        records.append(record)
    
    # 批量插入
    insert_sql = '''
        INSERT OR REPLACE INTO stock_daily 
        (ts_code, trade_date, open, high, low, close, volume, amount, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    '''
    
    try:
        # 分批插入
        total = len(records)
        for i in range(0, total, batch_size):
            batch = records[i:i+batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            logger.info(f"已保存 {min(i+batch_size, total)}/{total} 条记录")
        
        logger.info("数据保存完成")
        
        # 设置备份标志
        with open(BACKUP_FLAG_FILE, 'w') as f:
            f.write(f"数据已备份，时间：{datetime.now().isoformat()}\n")
            f.write(f"记录数：{total}\n")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"保存数据失败：{str(e)}")
        raise
    finally:
        conn.close()


def get_latest_date_for_stock(ts_code: str) -> Optional[str]:
    """
    获取指定股票的最新交易日期
    
    Args:
        ts_code (str): 股票代码
    
    Returns:
        str: 最新交易日期（YYYYMMDD格式），如果没有数据返回None
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT MAX(trade_date) as latest_date 
        FROM stock_daily 
        WHERE ts_code = ?
    ''', (ts_code,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result and result[0]:
        return result[0]
    return None


def get_all_stock_codes_from_db():
    """
    从数据库获取所有股票代码列表
    
    Returns:
        list: 股票代码列表
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT DISTINCT ts_code FROM stock_daily ORDER BY ts_code')
    codes = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return codes


def update_incremental_data(fetch_function, days: int = 1):
    """
    增量更新数据：只获取并保存最新的数据
    
    Args:
        fetch_function: 数据获取函数，应该接受stock_code和日期参数
        days (int): 获取最近几天的数据，默认1天
    """
    logger.info("开始增量更新数据...")
    
    # 检查数据库是否存在
    if not check_database_exists():
        logger.warning("数据库不存在，将进行全量数据获取")
        return False
    
    # 获取数据库中的所有股票代码
    stock_codes = get_all_stock_codes_from_db()
    
    if not stock_codes:
        logger.warning("数据库中没有股票代码，将进行全量数据获取")
        return False
    
    # 计算日期范围
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    
    all_new_data = []
    updated_count = 0
    
    for ts_code in stock_codes:
        try:
            # 获取该股票的最新日期
            latest_date = get_latest_date_for_stock(ts_code)
            
            if latest_date:
                # 如果最新日期早于开始日期，获取从最新日期到今天的增量数据
                if latest_date < start_date:
                    fetch_start = latest_date
                else:
                    # 最新日期已经很新了，跳过
                    continue
            else:
                # 没有数据，获取全部
                fetch_start = start_date
            
            # 获取增量数据
            from data_fetcher import fetch_stock_daily_data, format_data_for_storage
            
            df = fetch_stock_daily_data(ts_code, fetch_start, end_date)
            if df is not None and not df.empty:
                formatted_df = format_data_for_storage(df)
                if not formatted_df.empty:
                    all_new_data.append(formatted_df)
                    updated_count += 1
            
        except Exception as e:
            logger.error(f"更新股票 {ts_code} 数据失败：{str(e)}")
            continue
    
    if all_new_data:
        # 合并并保存新数据
        new_data_df = pd.concat(all_new_data, ignore_index=True)
        save_data(new_data_df)
        logger.info(f"增量更新完成：更新了 {updated_count} 只股票，共 {len(new_data_df)} 条新记录")
        return True
    else:
        logger.info("没有需要更新的数据")
        return False


def restore_all_data(fetch_function):
    """
    恢复所有数据：如果数据丢失，重新获取并保存所有数据
    
    Args:
        fetch_function: 数据获取函数
    """
    logger.warning("开始恢复所有数据...")
    
    # 清空现有数据
    if check_database_exists():
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM stock_daily')
        conn.commit()
        conn.close()
        logger.info("已清空现有数据")
    
    # 重新获取所有数据
    from data_fetcher import fetch_all_stock_data, format_data_for_storage
    
    logger.info("开始获取所有股票数据（这可能需要很长时间）...")
    all_data = fetch_all_stock_data(days=365, delay=0.1)
    
    if not all_data.empty:
        # 格式化数据
        formatted_data = format_data_for_storage(all_data)
        
        # 保存数据
        save_data(formatted_data)
        logger.info("数据恢复完成")
        return True
    else:
        logger.error("数据恢复失败：没有获取到数据")
        return False


def get_data_statistics():
    """
    获取数据统计信息
    
    Returns:
        dict: 包含统计信息的字典
    """
    if not check_database_exists():
        return {}
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    stats = {}
    
    # 总记录数
    cursor.execute('SELECT COUNT(*) FROM stock_daily')
    stats['total_records'] = cursor.fetchone()[0]
    
    # 股票数量
    cursor.execute('SELECT COUNT(DISTINCT ts_code) FROM stock_daily')
    stats['stock_count'] = cursor.fetchone()[0]
    
    # 日期范围
    cursor.execute('SELECT MIN(trade_date), MAX(trade_date) FROM stock_daily')
    result = cursor.fetchone()
    stats['date_range'] = {
        'start': result[0],
        'end': result[1]
    }
    
    conn.close()
    return stats


if __name__ == '__main__':
    # 测试代码
    print("测试数据存储功能...")
    
    # 初始化数据库
    init_database()
    
    # 检查数据
    if check_data_exists():
        print("数据库中已有数据")
        stats = get_data_statistics()
        print(f"统计信息：{stats}")
    else:
        print("数据库中没有数据")

