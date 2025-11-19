"""
批量上传设备数据到数据库
将 state 目录下的所有 JSON 文件中的数据插入到 tiktok_play.uni_devices 表中
"""
import os
import json
import sys
from typing import Dict, Any, List, Optional
from datetime import datetime
from mysql_db import MySQLDB
from config_loader import ConfigLoader
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_jsonl_file(file_path: str) -> List[Dict[str, Any]]:
    """
    读取 JSONL 文件（每行一个 JSON 对象）
    
    Args:
        file_path: 文件路径
    
    Returns:
        JSON 对象列表
    """
    data_list = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    data_list.append(data)
                except json.JSONDecodeError as e:
                    logger.warning(f"文件 {file_path} 第 {line_num} 行 JSON 解析失败: {e}")
                    continue
    except Exception as e:
        logger.error(f"读取文件 {file_path} 失败: {e}")
        raise
    
    return data_list


def read_json_file(file_path: str) -> List[Dict[str, Any]]:
    """
    读取 JSON 文件（每行一个 JSON 对象，类似 JSONL 格式）
    
    Args:
        file_path: 文件路径
    
    Returns:
        JSON 对象列表
    """
    return read_jsonl_file(file_path)


def transform_device_data(device: Dict[str, Any]) -> Dict[str, Any]:
    """
    转换设备数据格式，适配数据库表结构
    
    Args:
        device: 原始设备数据
    
    Returns:
        转换后的设备数据
    """
    # 创建新字典，复制所有字段（排除需要特殊处理的字段）
    transformed = {}
    
    # 先处理 device_create_time：从 JSON 的 create_time 字段读取并转换为时间戳
    # 注意：无论 JSON 中是否已经有 device_create_time 字段，都要从 create_time 转换
    device_create_time_value = 0  # 默认值为 0（秒时间戳）
    if 'create_time' in device and device['create_time']:
        create_time_raw = device['create_time']
        logger.info(f"开始转换 device_create_time, 原始值: {repr(create_time_raw)}, 类型: {type(create_time_raw).__name__}, 设备ID: {device.get('device_id', 'unknown')}")
        try:
            # 尝试解析 create_time 字符串（格式：'2025-11-13 02:35:41'）
            create_time_str = str(create_time_raw).strip()
            logger.info(f"转换 device_create_time, 处理后的字符串: '{create_time_str}', 设备ID: {device.get('device_id', 'unknown')}")
            
            # 解析时间字符串
            dt = datetime.strptime(create_time_str, '%Y-%m-%d %H:%M:%S')
            # 转换为时间戳（秒）
            timestamp = int(dt.timestamp())
            device_create_time_value = timestamp
            logger.info(f"✓ 转换 device_create_time 成功: '{create_time_str}' -> {timestamp} (秒), 设备ID: {device.get('device_id', 'unknown')}")
        except ValueError as e:
            logger.error(f"✗ 转换 device_create_time 失败（时间格式错误）: {e}, create_time: '{create_time_raw}', 设备ID: {device.get('device_id', 'unknown')}")
            logger.error(f"  请检查时间格式是否为 'YYYY-MM-DD HH:MM:SS'")
            device_create_time_value = 0  # 使用 0 作为默认值
        except Exception as e:
            logger.error(f"✗ 转换 device_create_time 失败（其他错误）: {e}, create_time: '{create_time_raw}', 设备ID: {device.get('device_id', 'unknown')}")
            import traceback
            logger.error(f"  异常堆栈: {traceback.format_exc()}")
            device_create_time_value = 0  # 使用 0 作为默认值
    else:
        # 如果 create_time 不存在，device_create_time 设为 0
        logger.warning(f"JSON 中没有 create_time 字段，device_create_time 将使用默认值 0, 设备ID: {device.get('device_id', 'unknown')}")
        device_create_time_value = 0
    
    # 复制所有字段（排除 device_create_time、create_time 和 device_config，这些字段需要特殊处理）
    excluded_keys = {'device_create_time', 'create_time', 'device_config'}  # 排除需要特殊处理的字段
    for key, value in device.items():
        if key not in excluded_keys:
            transformed[key] = value
    
    # 处理 seed 字段：确保从 JSON 中正确提取
    if 'seed' in device:
        transformed['seed'] = device['seed']
    elif 'seed' not in transformed:
        # 如果 JSON 中没有 seed 字段，设为空字符串
        transformed['seed'] = ''
    
    # 处理 seed_type：如果为 null 或 None，则改为空字符串
    if 'seed_type' in transformed:
        if transformed['seed_type'] is None:
            transformed['seed_type'] = ''
        # 确保 seed_type 是字符串类型
        elif not isinstance(transformed['seed_type'], str):
            transformed['seed_type'] = str(transformed['seed_type'])
    else:
        # 如果不存在 seed_type 字段，添加空字符串
        transformed['seed_type'] = ''
    
    # 处理 device_config：将整个设备数据转换为 JSON 字符串
    # 注意：device_config 应该保持原样，不需要修改其中的字段（包括 create_time）
    try:
        transformed['device_config'] = json.dumps(device, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"转换 device_config 失败: {e}, 设备ID: {device.get('device_id', 'unknown')}")
        transformed['device_config'] = json.dumps({}, ensure_ascii=False)
    
    # 强制设置 device_create_time（确保使用转换后的值，覆盖任何已存在的值）
    # 确保是整数类型的时间戳（秒）
    transformed['device_create_time'] = int(device_create_time_value) if device_create_time_value is not None else 0
    logger.info(f"✓ 最终设置 device_create_time = {transformed['device_create_time']} (类型: {type(transformed['device_create_time']).__name__}, 秒时间戳), 设备ID: {device.get('device_id', 'unknown')}")
    
    # 验证 device_create_time 是否在 transformed 中
    if 'device_create_time' not in transformed:
        logger.error(f"✗ 错误：device_create_time 未设置到 transformed 中！设备ID: {device.get('device_id', 'unknown')}")
    else:
        logger.info(f"✓ 验证：device_create_time 已设置到 transformed 中，值: {transformed['device_create_time']}")
    
    # 处理 create_time：设置为当前时间的时间戳（覆盖 JSON 中的 create_time）
    transformed['create_time'] = int(datetime.now().timestamp())
    
    return transformed


def get_table_columns(db: MySQLDB, table_name: str) -> List[str]:
    """
    获取数据库表的列名
    
    Args:
        db: 数据库连接对象
        table_name: 表名
    
    Returns:
        列名列表
    """
    try:
        sql = f"DESCRIBE {table_name}"
        result = db.execute(sql, fetch=True)
        if result:
            columns = [row['Field'] for row in result]
            return columns
        return []
    except Exception as e:
        logger.error(f"获取表 {table_name} 的列信息失败: {e}")
        raise


def batch_upload_devices(
    db: MySQLDB,
    data_list: List[Dict[str, Any]],
    table_name: str = "uni_devices_1",
    batch_size: int = 1000
) -> tuple[int, int]:
    """
    批量上传设备数据到数据库
    
    Args:
        db: 数据库连接对象
        data_list: 设备数据列表
        table_name: 表名
        batch_size: 每批插入的记录数
    
    Returns:
        (成功数量, 失败数量)
    """
    if not data_list:
        logger.warning("没有数据需要上传")
        return 0, 0
    
    # 获取表结构
    logger.info(f"获取表 {table_name} 的列信息...")
    table_columns = get_table_columns(db, table_name)
    logger.info(f"表 {table_name} 的列: {', '.join(table_columns)}")
    
    # 检查是否有类似的字段名（可能是拼写错误）
    similar_fields = [col for col in table_columns if 'create_time' in col.lower() or 'device_create' in col.lower()]
    if similar_fields:
        logger.info(f"找到类似的字段: {', '.join(similar_fields)}")
    if 'device_create_time' not in table_columns and 'devcie_create_time' in table_columns:
        logger.warning(f"⚠ 注意：数据库表中存在 'devcie_create_time'（拼写错误），但代码中使用的是 'device_create_time'")
        logger.warning(f"  请确认数据库表字段名是否正确")
    
    # 转换数据
    logger.info("开始转换数据格式...")
    transformed_list = []
    for device in data_list:
        try:
            transformed = transform_device_data(device)
            transformed_list.append(transformed)
        except Exception as e:
            logger.warning(f"转换设备数据失败: {e}, 设备数据: {device.get('device_id', 'unknown')}")
            continue
    
    if not transformed_list:
        logger.warning("转换后没有有效数据")
        return 0, len(data_list)
    
    # 过滤出表中存在的列
    first_item = transformed_list[0]
    valid_columns = [col for col in first_item.keys() if col in table_columns]
    
    # 检查 device_create_time 是否在表中，并确定实际字段名
    actual_field_name = None
    if 'device_create_time' in table_columns:
        actual_field_name = 'device_create_time'
    elif 'devcie_create_time' in table_columns:
        actual_field_name = 'devcie_create_time'
        logger.warning(f"⚠ 注意：数据库表中使用的是 'devcie_create_time'（拼写错误），将使用此字段名")
    
    if actual_field_name:
        logger.info(f"✓ {actual_field_name} 字段存在于数据库表中")
        # 如果 transformed 中使用的是 device_create_time，但表中是 devcie_create_time，需要为所有数据项映射
        if actual_field_name != 'device_create_time':
            # 为所有 transformed 数据项添加映射字段
            for item in transformed_list:
                if 'device_create_time' in item:
                    item[actual_field_name] = item.get('device_create_time', 0)
            logger.info(f"✓ 已为所有数据项将 device_create_time 的值映射到 {actual_field_name}")
        
        if actual_field_name not in valid_columns:
            valid_columns.append(actual_field_name)
            logger.info(f"✓ 已将 {actual_field_name} 添加到插入列列表")
    else:
        logger.warning(f"✗ device_create_time 字段不存在于数据库表中，请检查表结构")
        logger.warning(f"  数据库表的列: {', '.join(table_columns)}")
        logger.warning(f"  尝试查找包含 'create_time' 的字段: {[col for col in table_columns if 'create_time' in col.lower()]}")
    
    if not valid_columns:
        logger.error("没有匹配的列，请检查数据格式和表结构")
        return 0, len(transformed_list)
    
    logger.info(f"将使用以下列进行插入: {', '.join(valid_columns)}")
    logger.info(f"device_create_time 是否在插入列中: {'device_create_time' in valid_columns}")
    
    # 准备批量插入的数据
    insert_data_list = []
    # 检查实际使用的字段名（可能是 device_create_time 或 devcie_create_time）
    create_time_field = actual_field_name if actual_field_name else None
    
    for idx, item in enumerate(transformed_list):
        insert_item = {col: item.get(col) for col in valid_columns}
        
        # 调试：检查 device_create_time 相关字段的值
        if create_time_field:
            device_create_time_val = insert_item.get(create_time_field)
            # 只在前几条记录中打印详细信息，避免日志过多
            if idx < 3:
                logger.info(f"准备插入的数据 [{idx}]: {create_time_field} = {device_create_time_val} (类型: {type(device_create_time_val).__name__}), 设备ID: {item.get('device_id', 'unknown')}")
            # 确保值不为 None
            if device_create_time_val is None:
                logger.warning(f"⚠ {create_time_field} 为 None，将设置为 0, 设备ID: {item.get('device_id', 'unknown')}")
                insert_item[create_time_field] = 0
            # 确保值是整数类型
            elif not isinstance(device_create_time_val, int):
                logger.warning(f"⚠ {create_time_field} 不是整数类型，将转换为整数, 原值: {device_create_time_val}, 设备ID: {item.get('device_id', 'unknown')}")
                insert_item[create_time_field] = int(device_create_time_val) if device_create_time_val is not None else 0
        else:
            if idx < 3:  # 只在前几条记录中打印警告
                logger.warning(f"⚠ 未找到 device_create_time 相关字段，设备ID: {item.get('device_id', 'unknown')}")
        
        insert_data_list.append(insert_item)
    
    # 批量插入
    success_count = 0
    fail_count = 0
    
    logger.info(f"开始批量插入数据，共 {len(insert_data_list)} 条记录，每批 {batch_size} 条...")
    
    try:
        # 分批插入
        for i in range(0, len(insert_data_list), batch_size):
            batch = insert_data_list[i:i + batch_size]
            batch_num = i // batch_size + 1
            try:
                affected_rows = db.insert_many(table_name, batch, ignore=True)
                success_count += len(batch)
                logger.info(f"批次 {batch_num}: 成功插入 {len(batch)} 条记录（影响行数: {affected_rows}）")
            except Exception as e:
                logger.error(f"批次 {batch_num}: 批量插入失败: {e}，尝试逐条插入...")
                # 批量插入失败时，尝试逐条插入，找出失败的具体记录
                for item in batch:
                    try:
                        db.insert(table_name, item, ignore=True)
                        success_count += 1
                    except Exception as item_error:
                        fail_count += 1
                        logger.warning(f"批次 {batch_num} 单条插入失败: {item_error}, 设备ID: {item.get('device_id', 'unknown')}")
    except Exception as e:
        logger.error(f"批量插入过程中发生错误: {e}")
        raise
    
    return success_count, fail_count


def main():
    """主函数"""
    # 获取 state 目录路径
    state_dir = "state"
    if not os.path.exists(state_dir):
        logger.error(f"目录 {state_dir} 不存在")
        sys.exit(1)
    
    # 查找所有 JSON 文件
    json_files = []
    for filename in os.listdir(state_dir):
        if filename.endswith('.json') or filename.endswith('.jsonl'):
            file_path = os.path.join(state_dir, filename)
            json_files.append(file_path)
    
    if not json_files:
        logger.warning(f"在 {state_dir} 目录下没有找到 JSON 文件")
        sys.exit(1)
    
    logger.info(f"找到 {len(json_files)} 个 JSON 文件: {', '.join(json_files)}")
    
    # 读取所有文件的数据
    all_data = []
    for file_path in json_files:
        logger.info(f"正在读取文件: {file_path}")
        try:
            if file_path.endswith('.jsonl'):
                data = read_jsonl_file(file_path)
            else:
                data = read_json_file(file_path)
            logger.info(f"文件 {file_path} 读取完成，共 {len(data)} 条记录")
            all_data.extend(data)
        except Exception as e:
            logger.error(f"读取文件 {file_path} 失败: {e}")
            continue
    
    if not all_data:
        logger.warning("没有读取到任何数据")
        sys.exit(1)
    
    logger.info(f"总共读取到 {len(all_data)} 条记录")
    
    # 连接数据库
    logger.info("正在连接数据库...")
    try:
        db = MySQLDB()
        logger.info("数据库连接成功")
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        sys.exit(1)
    
    try:
        # 批量上传数据
        success_count, fail_count = batch_upload_devices(
            db=db,
            data_list=all_data,
            table_name="uni_devices_1",
            batch_size=1000
        )
        
        logger.info("=" * 60)
        logger.info("批量上传完成")
        logger.info(f"成功: {success_count} 条")
        logger.info(f"失败: {fail_count} 条")
        logger.info(f"总计: {len(all_data)} 条")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"批量上传失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        db.close()
        logger.info("数据库连接已关闭")


if __name__ == "__main__":
    main()

