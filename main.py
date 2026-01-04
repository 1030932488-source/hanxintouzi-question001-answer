"""
主程序入口
协调数据获取、存储和校验
"""
import schedule
import time
import logging
from data_fetcher import fetch_all_stock_data, format_data_for_storage
from data_storage import save_data, update_incremental_data, restore_all_data, check_data_exists, init_database
from data_validator import validate_data, print_validation_report
from data_config import DEFAULT_FETCH_DAYS, LOG_LEVEL, LOG_FORMAT

# 配置日志
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

def daily_job():
    """每日定时任务"""
    logger.info("开始执行每日数据更新任务...")
    try:
        # 1. 增量更新数据
        from data_fetcher import fetch_stock_daily_data
        updated = update_incremental_data(fetch_stock_daily_data)
        
        if updated:
            logger.info("数据更新完成")
        else:
            logger.info("无需更新或更新失败")
            
        # 2. 校验数据
        logger.info("执行数据校验...")
        results = validate_data()
        print_validation_report(results)
        
    except Exception as e:
        logger.error(f"每日任务执行失败：{str(e)}")

def initial_setup():
    """初始设置"""
    logger.info("开始初始设置...")
    
    # 初始化数据库
    init_database()
    
    # 检查是否有数据
    if not check_data_exists():
        logger.info("数据库为空，开始首次全量数据获取...")
        # 获取近1年数据
        df = fetch_all_stock_data(days=DEFAULT_FETCH_DAYS)
        if not df.empty:
            formatted_df = format_data_for_storage(df)
            save_data(formatted_df)
            logger.info("首次数据获取完成")
        else:
            logger.error("首次数据获取失败")
    else:
        logger.info("数据库已存在数据，跳过首次获取")

def restore_mode():
    """由于数据丢失进行恢复"""
    logger.warning("进入数据恢复模式...")
    from data_fetcher import fetch_all_stock_data
    restore_all_data(fetch_all_stock_data)

def run():
    """运行主程序"""
    # 初始设置
    initial_setup()
    
    # 立即执行一次校验
    results = validate_data()
    print_validation_report(results)
    
    # 设置定时任务（每天收盘后，例如16:00）
    schedule.every().day.at("16:00").do(daily_job)
    
    logger.info("程序已启动，等待定时任务...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    # 简单的命令行参数处理
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--restore':
        restore_mode()
    else:
        run()
