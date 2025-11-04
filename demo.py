from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def convert_string_to_cypher_datetime(date_str: str, include_time: bool = False) -> str:
    """
    将日期时间字符串转换为Cypher datetime格式
    
    Args:
        date_str: 日期时间字符串，支持多种格式：
                 - "2024-12-31"
                 - "2024-12-31 10:30:00"
                 - "2024/12/31"
                 - "2024/12/31 10:30:00"
                 - "20241231"
                 - "2024年12月31日"
                 - "2024-12-31T10:30:00"
                 - "2024-12-31T10:30:00Z"
        include_time: 是否包含时间部分，默认False（仅日期）
    
    Returns:
        str: Cypher datetime格式字符串
            - 日期格式: datetime("2024-12-31T00:00:00")
            - 日期时间格式: datetime("2024-12-31T10:30:00")
            如果解析失败，返回空字符串或None
    
    Examples:
        >>> convert_string_to_cypher_datetime("2024-12-31")
        'datetime("2024-12-31T00:00:00")'
        >>> convert_string_to_cypher_datetime("2024-12-31 10:30:00", include_time=True)
        'datetime("2024-12-31T10:30:00")'
    """
    if not date_str:
        return 'NULL'
    
    try:
        # 清理日期字符串
        date_clean = str(date_str).strip()
        
        # 移除引号
        date_clean = date_clean.replace('"', '').replace("'", '')
        
        # 移除中文字符
        import re
        date_clean = re.sub(r'[年月日]', '', date_clean)
        
        # 处理已包含 'T' 的 ISO 格式或包含时间的格式
        has_time_separator = 'T' in date_clean or (':' in date_clean and ' ' in date_clean)
        
        if has_time_separator:
            # 标准化为空格分隔的格式先处理
            if 'T' in date_clean:
                date_clean = date_clean.replace('T', ' ')
            # 移除时区信息（Z, +08:00等）
            date_clean = re.sub(r'[Z\+\-]\d{2}:?\d{2}.*$', '', date_clean).strip()
            # 现在按空格分隔处理
            parts = date_clean.split()
            if len(parts) >= 2:
                # 有日期和时间部分
                date_part = parts[0]
                time_part = parts[1]
                # 确保时间格式完整
                time_parts = time_part.split(':')
                if len(time_parts) < 3:
                    time_part = ':'.join(time_parts + ['00'] * (3 - len(time_parts)))
                date_clean = f"{date_part} {time_part}"
                try:
                    dt = datetime.strptime(date_clean, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        dt = datetime.strptime(date_clean, '%Y-%m-%d %H:%M')
                    except ValueError:
                        # 降级为仅日期
                        dt = datetime.strptime(date_part, '%Y-%m-%d')
            else:
                # 只有日期部分，但包含T或特殊格式
                dt = datetime.strptime(parts[0], '%Y-%m-%d')
        else:
            # 尝试多种日期格式
            date_formats = [
                '%Y-%m-%d %H:%M:%S',      # 2024-12-31 10:30:00
                '%Y/%m/%d %H:%M:%S',      # 2024/12/31 10:30:00
                '%Y-%m-%d',               # 2024-12-31
                '%Y/%m/%d',               # 2024/12/31
                '%Y%m%d',                # 20241231
                '%Y-%m-%d %H:%M',        # 2024-12-31 10:30
                '%Y/%m/%d %H:%M',        # 2024/12/31 10:30
                '%Y-%m',                 # 2024-12
                '%Y/%m',                 # 2024/12
                '%Y'                     # 2024
            ]
            
            dt = None
            for fmt in date_formats:
                try:
                    if fmt == '%Y-%m' or fmt == '%Y/%m':
                        # 如果只有年月，补充日为01
                        dt = datetime.strptime(date_clean + '-01', '%Y-%m-%d')
                    elif fmt == '%Y':
                        # 如果只有年，补充为01-01
                        dt = datetime.strptime(date_clean + '-01-01', '%Y-%m-%d')
                    else:
                        dt = datetime.strptime(date_clean, fmt)
                    break
                except ValueError:
                    continue
            
            if dt is None:
                logger.warning(f"无法解析日期时间格式: {date_str}")
                return 'NULL'
        
        # 如果不包含时间，设置为00:00:00
        if not include_time and dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            # 保持日期格式
            pass
        elif not include_time:
            # 如果原始字符串包含时间但include_time=False，则清零时间
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # 格式化为ISO 8601格式
        iso_format = dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        # 返回Cypher datetime格式
        return f'datetime("{iso_format}")'
        
    except Exception as e:
        logger.warning(f"转换日期时间格式时发生错误: {e}, 输入: {date_str}")
        return 'NULL'

print(convert_string_to_cypher_datetime("2024-12-31", include_time=True))
print(convert_string_to_cypher_datetime("2024-12-31 10:30:00", include_time=True))
print(convert_string_to_cypher_datetime("2024/12/31", include_time=True))

print(convert_string_to_cypher_datetime("20241231", include_time=True))
print(convert_string_to_cypher_datetime("2024年12月31日", include_time=True))
print(convert_string_to_cypher_datetime("2024-12-31T10:30:00", include_time=True))
print(convert_string_to_cypher_datetime("2024-12-31T10:30:00Z", include_time=True))