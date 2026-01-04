"""
数据获取模块
使用akshare获取A股日频行情数据
"""
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import time
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_all_stock_codes():
    """
    获取所有A股股票代码列表
    
    Returns:
        list: 股票代码列表，格式如 ['000001', '000002', ...]
    """
    try:
        logger.info("正在获取A股股票代码列表...")
        # 获取A股代码和名称
        stock_info = ak.stock_info_a_code_name()
        
        # 提取股票代码（去掉后缀，只保留6位数字代码）
        stock_codes = stock_info['code'].tolist()
        
        logger.info(f"成功获取 {len(stock_codes)} 只股票代码")
        return stock_codes
    except Exception as e:
        logger.error(f"获取股票代码失败：{str(e)}")
        raise


def fetch_stock_daily_data(stock_code, start_date=None, end_date=None):
    """
    获取单只股票的日频行情数据
    
    Args:
        stock_code (str): 股票代码，如 '000001'
        start_date (str): 开始日期，格式 'YYYYMMDD'，默认为1年前
        end_date (str): 结束日期，格式 'YYYYMMDD'，默认为今天
    
    Returns:
        pd.DataFrame: 包含股票数据的DataFrame，如果失败返回None
    """
    try:
        # 如果没有指定日期，默认获取近1年数据
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
        
        if start_date is None:
            # 计算1年前的日期
            one_year_ago = datetime.now() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y%m%d')
        
        # 获取股票数据
        # 注意：akshare的接口可能需要调整，这里使用通用接口
        df = ak.stock_zh_a_hist(
            symbol=stock_code,
            period="daily",
            start_date=start_date.replace('-', ''),
            end_date=end_date.replace('-', ''),
            adjust=""
        )
        
        if df is not None and not df.empty:
            # 标准化列名
            df.columns = df.columns.str.strip()
            
            # 确保包含必要的列
            required_columns = ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额']
            if all(col in df.columns for col in required_columns):
                # 添加股票代码列
                df['股票代码'] = stock_code
                return df
            else:
                logger.warning(f"股票 {stock_code} 数据列不完整")
                return None
        else:
            logger.warning(f"股票 {stock_code} 没有数据")
            return None
            
    except Exception as e:
        logger.warning(f"获取股票 {stock_code} 数据失败：{str(e)}")
        return None


def fetch_all_stock_data(days=365, delay=0.1):
    """
    获取所有A股近指定天数的日频行情数据
    
    Args:
        days (int): 获取多少天的数据，默认365天（近1年）
        delay (float): 每次请求之间的延迟（秒），避免请求过快
    
    Returns:
        pd.DataFrame: 包含所有股票数据的DataFrame
    """
    logger.info(f"开始获取所有A股近 {days} 天的数据...")
    
    # 获取所有股票代码
    stock_codes = get_all_stock_codes()
    
    # 计算日期范围
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
    
    all_data = []
    success_count = 0
    fail_count = 0
    
    total = len(stock_codes)
    
    for idx, stock_code in enumerate(stock_codes, 1):
        try:
            logger.info(f"正在获取 [{idx}/{total}] {stock_code} 的数据...")
            
            df = fetch_stock_daily_data(stock_code, start_date, end_date)
            
            if df is not None and not df.empty:
                all_data.append(df)
                success_count += 1
            else:
                fail_count += 1
            
            # 延迟，避免请求过快
            if delay > 0:
                time.sleep(delay)
                
        except Exception as e:
            logger.error(f"处理股票 {stock_code} 时出错：{str(e)}")
            fail_count += 1
            continue
    
    logger.info(f"数据获取完成：成功 {success_count} 只，失败 {fail_count} 只")
    
    if all_data:
        # 合并所有数据
        result_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"合并后共有 {len(result_df)} 条记录")
        return result_df
    else:
        logger.warning("没有获取到任何数据")
        return pd.DataFrame()


def format_data_for_storage(df):
    """
    格式化数据以便存储到数据库
    
    Args:
        df (pd.DataFrame): 原始数据DataFrame
    
    Returns:
        pd.DataFrame: 格式化后的DataFrame
    """
    if df is None or df.empty:
        return pd.DataFrame()
    
    # 创建新的DataFrame，使用标准列名
    formatted_df = pd.DataFrame()
    
    # 映射列名
    column_mapping = {
        '日期': 'trade_date',
        '股票代码': 'ts_code',
        '开盘': 'open',
        '收盘': 'close',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'volume',
        '成交额': 'amount'
    }
    
    # 重命名列
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            formatted_df[new_col] = df[old_col]
    
    # 确保日期格式正确
    if 'trade_date' in formatted_df.columns:
        # 将日期转换为字符串格式 YYYYMMDD
        formatted_df['trade_date'] = pd.to_datetime(formatted_df['trade_date']).dt.strftime('%Y%m%d')
    
    # 确保数值列的类型正确
    numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount']
    for col in numeric_columns:
        if col in formatted_df.columns:
            formatted_df[col] = pd.to_numeric(formatted_df[col], errors='coerce')
    
    # 删除包含NaN的行
    formatted_df = formatted_df.dropna()
    
    return formatted_df


if __name__ == '__main__':
    # 测试代码
    print("测试数据获取功能...")
    
    # 测试获取单只股票数据
    test_code = '000001'
    print(f"\n测试获取股票 {test_code} 的数据...")
    test_data = fetch_stock_daily_data(test_code, days=30)
    if test_data is not None:
        print(f"成功获取 {len(test_data)} 条记录")
        print(test_data.head())
    else:
        print("获取失败")
    
    # 注意：获取所有股票数据会花费很长时间，谨慎运行
    # print("\n测试获取所有股票数据（仅获取最近7天）...")
    # all_data = fetch_all_stock_data(days=7, delay=0.2)
    # print(f"总共获取 {len(all_data)} 条记录")

