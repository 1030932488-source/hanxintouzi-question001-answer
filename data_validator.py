"""
数据校验模块
检验数据的完整性和正确性
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'stock_data.db'


def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def validate_data() -> Dict:
    """
    全面校验数据
    
    Returns:
        dict: 包含校验结果的字典
    """
    logger.info("开始数据校验...")
    
    results = {
        'is_valid': True,
        'errors': [],
        'warnings': [],
        'statistics': {}
    }
    
    try:
        # 1. 检查数据库是否存在
        import os
        if not os.path.exists(DB_PATH):
            results['is_valid'] = False
            results['errors'].append("数据库文件不存在")
            return results
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 2. 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_daily'")
        if not cursor.fetchone():
            results['is_valid'] = False
            results['errors'].append("数据表不存在")
            conn.close()
            return results
        
        # 3. 检查数据是否存在
        cursor.execute("SELECT COUNT(*) FROM stock_daily")
        total_count = cursor.fetchone()[0]
        
        if total_count == 0:
            results['is_valid'] = False
            results['errors'].append("数据库中没有数据")
            conn.close()
            return results
        
        results['statistics']['total_records'] = total_count
        
        # 4. 检查数据完整性
        integrity_issues = check_data_integrity(cursor)
        if integrity_issues:
            results['warnings'].extend(integrity_issues)
        
        # 5. 检查数据正确性
        correctness_issues = check_data_correctness(cursor)
        if correctness_issues:
            results['warnings'].extend(correctness_issues)
            # 某些正确性问题可能很严重
            if any('严重' in issue for issue in correctness_issues):
                results['is_valid'] = False
                results['errors'].extend([issue for issue in correctness_issues if '严重' in issue])
        
        # 6. 检查数据一致性
        consistency_issues = check_data_consistency(cursor)
        if consistency_issues:
            results['warnings'].extend(consistency_issues)
        
        # 7. 获取统计信息
        stats = get_detailed_statistics(cursor)
        results['statistics'].update(stats)
        
        conn.close()
        
        logger.info("数据校验完成")
        return results
        
    except Exception as e:
        logger.error(f"数据校验过程中出错：{str(e)}")
        results['is_valid'] = False
        results['errors'].append(f"校验过程出错：{str(e)}")
        return results


def check_data_integrity(cursor) -> List[str]:
    """
    检查数据完整性
    
    Args:
        cursor: 数据库游标
    
    Returns:
        list: 完整性问题的列表
    """
    issues = []
    
    # 检查是否有空值
    cursor.execute('''
        SELECT COUNT(*) 
        FROM stock_daily 
        WHERE ts_code IS NULL OR trade_date IS NULL
    ''')
    null_count = cursor.fetchone()[0]
    if null_count > 0:
        issues.append(f"发现 {null_count} 条记录的关键字段为空")
    
    # 检查是否有重复记录
    cursor.execute('''
        SELECT ts_code, trade_date, COUNT(*) as cnt
        FROM stock_daily
        GROUP BY ts_code, trade_date
        HAVING cnt > 1
    ''')
    duplicates = cursor.fetchall()
    if duplicates:
        issues.append(f"发现 {len(duplicates)} 组重复记录（同一股票同一日期）")
    
    # 检查日期格式
    cursor.execute('''
        SELECT COUNT(*) 
        FROM stock_daily 
        WHERE LENGTH(trade_date) != 8 OR trade_date NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
    ''')
    invalid_date_count = cursor.fetchone()[0]
    if invalid_date_count > 0:
        issues.append(f"发现 {invalid_date_count} 条记录的日期格式不正确")
    
    return issues


def check_data_correctness(cursor) -> List[str]:
    """
    检查数据正确性
    
    Args:
        cursor: 数据库游标
    
    Returns:
        list: 正确性问题的列表
    """
    issues = []
    
    # 检查价格是否合理（应该大于0）
    cursor.execute('''
        SELECT COUNT(*) 
        FROM stock_daily 
        WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
    ''')
    invalid_price_count = cursor.fetchone()[0]
    if invalid_price_count > 0:
        issues.append(f"【严重】发现 {invalid_price_count} 条记录的价格数据不合理（<=0）")
    
    # 检查 high >= low
    cursor.execute('''
        SELECT COUNT(*) 
        FROM stock_daily 
        WHERE high < low
    ''')
    invalid_range_count = cursor.fetchone()[0]
    if invalid_range_count > 0:
        issues.append(f"【严重】发现 {invalid_range_count} 条记录的最高价低于最低价")
    
    # 检查价格是否在合理范围内（假设A股价格在0.01到10000之间）
    cursor.execute('''
        SELECT COUNT(*) 
        FROM stock_daily 
        WHERE open > 10000 OR high > 10000 OR low > 10000 OR close > 10000
           OR open < 0.01 OR high < 0.01 OR low < 0.01 OR close < 0.01
    ''')
    extreme_price_count = cursor.fetchone()[0]
    if extreme_price_count > 0:
        issues.append(f"发现 {extreme_price_count} 条记录的价格超出合理范围（0.01-10000）")
    
    # 检查成交量是否合理（应该 >= 0）
    cursor.execute('''
        SELECT COUNT(*) 
        FROM stock_daily 
        WHERE volume < 0
    ''')
    invalid_volume_count = cursor.fetchone()[0]
    if invalid_volume_count > 0:
        issues.append(f"发现 {invalid_volume_count} 条记录的成交量为负数")
    
    # 检查成交额是否合理（应该 >= 0）
    cursor.execute('''
        SELECT COUNT(*) 
        FROM stock_daily 
        WHERE amount < 0
    ''')
    invalid_amount_count = cursor.fetchone()[0]
    if invalid_amount_count > 0:
        issues.append(f"发现 {invalid_amount_count} 条记录的成交额为负数")
    
    return issues


def check_data_consistency(cursor) -> List[str]:
    """
    检查数据一致性
    
    Args:
        cursor: 数据库游标
    
    Returns:
        list: 一致性问题的列表
    """
    issues = []
    
    # 检查每只股票的数据连续性（是否有缺失的交易日）
    # 这里只做简单检查：检查是否有股票的数据量异常少
    cursor.execute('''
        SELECT ts_code, COUNT(*) as record_count, 
               MIN(trade_date) as min_date, MAX(trade_date) as max_date
        FROM stock_daily
        GROUP BY ts_code
        HAVING record_count < 10
    ''')
    low_record_stocks = cursor.fetchall()
    if low_record_stocks:
        issues.append(f"发现 {len(low_record_stocks)} 只股票的数据记录数少于10条，可能存在数据缺失")
    
    # 检查日期范围是否合理（不应该有未来日期）
    today = datetime.now().strftime('%Y%m%d')
    cursor.execute('''
        SELECT COUNT(*) 
        FROM stock_daily 
        WHERE trade_date > ?
    ''', (today,))
    future_date_count = cursor.fetchone()[0]
    if future_date_count > 0:
        issues.append(f"发现 {future_date_count} 条记录的日期是未来日期")
    
    return issues


def get_detailed_statistics(cursor) -> Dict:
    """
    获取详细统计信息
    
    Args:
        cursor: 数据库游标
    
    Returns:
        dict: 统计信息字典
    """
    stats = {}
    
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
    
    # 平均每天的数据量
    cursor.execute('SELECT COUNT(DISTINCT trade_date) FROM stock_daily')
    distinct_dates = cursor.fetchone()[0]
    if distinct_dates > 0:
        stats['avg_records_per_day'] = stats['total_records'] / distinct_dates
    
    # 数据完整性百分比
    cursor.execute('''
        SELECT COUNT(*) 
        FROM stock_daily 
        WHERE open IS NOT NULL AND high IS NOT NULL 
          AND low IS NOT NULL AND close IS NOT NULL
          AND volume IS NOT NULL AND amount IS NOT NULL
    ''')
    complete_records = cursor.fetchone()[0]
    if stats['total_records'] > 0:
        stats['completeness_percentage'] = (complete_records / stats['total_records']) * 100
    
    return stats


def print_validation_report(results: Dict):
    """
    打印校验报告
    
    Args:
        results (dict): 校验结果字典
    """
    print("\n" + "="*60)
    print("数据校验报告")
    print("="*60)
    
    print(f"\n校验状态: {'✓ 通过' if results['is_valid'] else '✗ 失败'}")
    
    if results['errors']:
        print("\n【错误】:")
        for error in results['errors']:
            print(f"  - {error}")
    
    if results['warnings']:
        print("\n【警告】:")
        for warning in results['warnings']:
            print(f"  - {warning}")
    
    if results['statistics']:
        print("\n【统计信息】:")
        for key, value in results['statistics'].items():
            if isinstance(value, dict):
                print(f"  {key}:")
                for k, v in value.items():
                    print(f"    - {k}: {v}")
            else:
                print(f"  {key}: {value}")
    
    print("\n" + "="*60 + "\n")


if __name__ == '__main__':
    # 测试代码
    print("测试数据校验功能...")
    results = validate_data()
    print_validation_report(results)

