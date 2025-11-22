"""
配置文件加载模块
支持从 JSON 或 YAML 文件加载配置
支持分环境加载（开发环境/生产环境）
自动根据操作系统判断环境：
- Windows/Mac -> 开发环境 (dev)
- Linux -> 生产环境 (prod)
"""
import json
import os
import platform
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# 全局配置缓存（避免重复加载配置文件）
_config_cache: Dict[str, Dict[str, Any]] = {}

# 环境变量名，用于指定当前环境（如果设置了环境变量，则优先使用环境变量）
ENV_VAR_NAME = "ENV"  # 或 "ENVIRONMENT"


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
    def get_environment() -> str:
        """
        获取当前环境
        优先级：
        1. 环境变量 ENV（如果设置了）
        2. 根据操作系统自动判断：
           - Windows/Mac -> dev（开发环境）
           - Linux -> prod（生产环境）
        
        Returns:
            环境名称（dev/prod）
        """
        # 首先检查环境变量
        env_from_var = os.getenv(ENV_VAR_NAME)
        if env_from_var:
            env = env_from_var.lower()
            # 支持 production 和 prod
            if env == "production":
                env = "prod"
            logger.info(f"从环境变量读取环境: {env}")
            return env
        
        # 根据操作系统自动判断
        system = platform.system().lower()
        if system == "linux":
            env = "prod"
            logger.info(f"检测到 Linux 系统，自动使用生产环境: {env}")
        elif system in ["windows", "darwin"]:  # darwin 是 Mac OS
            env = "dev"
            logger.info(f"检测到 {system} 系统，自动使用开发环境: {env}")
        else:
            # 未知系统，默认使用开发环境
            env = "dev"
            logger.warning(f"未知操作系统: {system}，默认使用开发环境: {env}")
        
        return env
    
    @staticmethod
    def _get_config_file_path(config_path: Optional[str] = None, env: Optional[str] = None) -> str:
        """
        根据环境和指定路径确定实际配置文件路径
        
        Args:
            config_path: 指定的配置文件路径
            env: 环境名称，如果为 None 则从环境变量读取
        
        Returns:
            实际配置文件路径
        """
        if config_path:
            # 如果指定了路径，直接使用
            return config_path
        
        # 获取环境
        if env is None:
            env = ConfigLoader.get_environment()
        
        # 根据环境尝试加载对应的配置文件
        # 优先级：config.{env}.json > config.{env}.yaml > config.json > config.yaml
        config_files = [
            f"config.{env}.json",
            f"config.{env}.yaml",
            f"config.{env}.yml",
            "config.json",
            "config.yaml",
            "config.yml"
        ]
        
        for config_file in config_files:
            if os.path.exists(config_file):
                logger.info(f"找到配置文件: {config_file} (环境: {env})")
                return config_file
        
        # 如果都没找到，返回默认路径
        logger.warning(f"未找到任何配置文件，尝试使用默认路径: config.json")
        return "config.json"
    
    @staticmethod
    def _load_config_file(config_path: Optional[str] = None, env: Optional[str] = None) -> Dict[str, Any]:
        """
        加载配置文件（带缓存，只加载一次）
        支持分环境加载
        
        Args:
            config_path: 配置文件路径，如果为 None，则根据环境自动选择
            env: 环境名称，如果为 None 则从环境变量读取
        
        Returns:
            配置字典
        """
        global _config_cache
        
        # 确定实际使用的配置文件路径
        actual_path = ConfigLoader._get_config_file_path(config_path, env)
        
        # 检查缓存
        if actual_path in _config_cache:
            logger.debug(f"从缓存加载配置: {actual_path}")
            return _config_cache[actual_path]
        
        # 如果文件不存在，返回空配置
        if not os.path.exists(actual_path):
            logger.warning(f"配置文件不存在: {actual_path}，使用空配置")
            return {}
        
        # 加载配置文件
        if actual_path.endswith('.yaml') or actual_path.endswith('.yml'):
            config = ConfigLoader.load_yaml(actual_path)
        else:
            config = ConfigLoader.load_json(actual_path)
        
        # 存入缓存
        _config_cache[actual_path] = config
        logger.info(f"配置文件已加载并缓存: {actual_path} (环境: {ConfigLoader.get_environment()})")
        
        return config
    
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

