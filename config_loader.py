"""
配置文件加载模块
支持从 JSON 或 YAML 文件加载配置
"""
import json
import os
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ConfigLoader:
    """配置加载器"""
    
    @staticmethod
    def load_json(config_path: str = "config.json") -> Dict[str, Any]:
        """
        从 JSON 文件加载配置
        
        Args:
            config_path: 配置文件路径
        
        Returns:
            配置字典
        """
        if not os.path.exists(config_path):
            logger.warning(f"配置文件不存在: {config_path}，使用默认配置")
            return {}
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"成功加载配置文件: {config_path}")
            return config
        except json.JSONDecodeError as e:
            logger.error(f"配置文件 JSON 格式错误: {e}")
            raise
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise
    
    @staticmethod
    def load_yaml(config_path: str = "config.yaml") -> Dict[str, Any]:
        """
        从 YAML 文件加载配置
        
        Args:
            config_path: 配置文件路径
        
        Returns:
            配置字典
        """
        try:
            import yaml
        except ImportError:
            logger.error("需要安装 PyYAML: pip install pyyaml")
            raise
        
        if not os.path.exists(config_path):
            logger.warning(f"配置文件不存在: {config_path}，使用默认配置")
            return {}
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"成功加载配置文件: {config_path}")
            return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            raise
    
    @staticmethod
    def _load_config_file(config_path: Optional[str] = None) -> Dict[str, Any]:
        """
        加载配置文件
        
        Args:
            config_path: 配置文件路径，如果为 None，则尝试从默认配置文件加载
        
        Returns:
            配置字典
        """
        if config_path:
            if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                return ConfigLoader.load_yaml(config_path)
            else:
                return ConfigLoader.load_json(config_path)
        else:
            # 尝试加载默认配置文件
            if os.path.exists("config.json"):
                return ConfigLoader.load_json("config.json")
            elif os.path.exists("config.yaml"):
                return ConfigLoader.load_yaml("config.yaml")
            elif os.path.exists("config.yml"):
                return ConfigLoader.load_yaml("config.yml")
            else:
                return {}
    
    @staticmethod
    def get_mysql_config(config_path: Optional[str] = None) -> Dict[str, Any]:
        """
        获取 MySQL 配置
        
        Args:
            config_path: 配置文件路径，如果为 None，则尝试从 config.json 或 config.yaml 加载
        
        Returns:
            MySQL 配置字典
        """
        config = ConfigLoader._load_config_file(config_path)
        return config.get("mysql", {})
    
    @staticmethod
    def get_redis_config(config_path: Optional[str] = None) -> Dict[str, Any]:
        """
        获取 Redis 配置
        
        Args:
            config_path: 配置文件路径，如果为 None，则尝试从 config.json 或 config.yaml 加载
        
        Returns:
            Redis 配置字典
        """
        config = ConfigLoader._load_config_file(config_path)
        return config.get("redis", {})


# 全局配置加载器实例
config_loader = ConfigLoader()

