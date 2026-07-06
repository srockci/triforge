"""
Version Control Integration for TriForge

支持GitHub、Gitee等平台的版本控制功能，包括推送和拉取项目。
"""

import json
import os
import requests
import subprocess
import tempfile
import shutil
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class PlatformType(Enum):
    GITHUB = "github"
    GITEE = "gitee"
    GITLAB = "gitlab"
    CUSTOM_GIT = "custom_git"

@dataclass
class GitRepository:
    """Git仓库信息"""
    name: str
    full_name: str
    description: str
    html_url: str
    clone_url: str
    default_branch: str
    platform: PlatformType
    owner: str
    private: bool = False

@dataclass
class PlatformConfig:
    """平台配置"""
    platform: PlatformType
    api_url: str
    auth_token: str
    username: Optional[str] = None
    email: Optional[str] = None
    git_url: Optional[str] = None  # 自建Git的URL

class GitOperations:
    """Git操作工具类"""
    
    @staticmethod
    def init_repo(repo_path: Path) -> bool:
        """初始化Git仓库"""
        try:
            subprocess.run(["git", "init"], cwd=repo_path, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to init git repo: {e}")
            return False
    
    @staticmethod
    def add_remote(repo_path: Path, remote_name: str, remote_url: str) -> bool:
        """添加远程仓库"""
        try:
            subprocess.run(["git", "remote", "add", remote_name, remote_url], 
                         cwd=repo_path, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to add remote: {e}")
            return False
    
    @staticmethod
    def commit(repo_path: Path, message: str) -> bool:
        """提交更改"""
        try:
            subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True)
            subprocess.run(["git", "commit", "-m", message], cwd=repo_path, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to commit: {e}")
            return False
    
    @staticmethod
    def push(repo_path: Path, remote_name: str = "origin", branch: str = "main") -> bool:
        """推送到远程仓库"""
        try:
            subprocess.run(["git", "push", remote_name, branch], cwd=repo_path, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to push: {e}")
            return False
    
    @staticmethod
    def clone(repo_url: str, local_path: Path, branch: str = "main") -> bool:
        """克隆仓库"""
        try:
            subprocess.run(["git", "clone", repo_url, str(local_path)], check=True, capture_output=True)
            # 切换到指定分支
            subprocess.run(["git", "checkout", branch], cwd=local_path, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to clone repo: {e}")
            return False
    
    @staticmethod
    def pull(repo_path: Path, branch: str = "main") -> bool:
        """从远程仓库拉取"""
        try:
            subprocess.run(["git", "pull", "origin", branch], cwd=repo_path, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to pull repo: {e}")
            return False
    
    @staticmethod
    def set_user_config(repo_path: Path, name: str, email: str) -> bool:
        """设置Git用户配置"""
        try:
            subprocess.run(["git", "config", "user.name", name], cwd=repo_path, check=True, capture_output=True)
            subprocess.run(["git", "config", "user.email", email], cwd=repo_path, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to set user config: {e}")
            return False
    
    @staticmethod
    def add_all(repo_path: Path) -> bool:
        """添加所有文件到git"""
        try:
            subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to add all files: {e}")
            return False

class VersionControlManager:
    """版本控制管理器"""
    
    def __init__(self, config_path: str = "data/version_control.json"):
        self.config_path = Path(config_path)
        self.configs: Dict[PlatformType, PlatformConfig] = {}
        self.repositories: List[GitRepository] = []
        self.load_config()
    
    def load_config(self):
        """加载配置"""
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.configs = {
                        PlatformType(k): PlatformConfig(
                            platform=PlatformType(k),
                            **v
                        ) for k, v in data.get('platforms', {}).items()
                    }
                    self.repositories = [
                        GitRepository(**repo) for repo in data.get('repositories', [])
                    ]
            except Exception as e:
                logger.error(f"Failed to load version control config: {e}")
    
    def save_config(self):
        """保存配置"""
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, 'w', encoding='utf-8') as f:
                data = {
                    'platforms': {
                        k.value: asdict(v) for k, v in self.configs.items()
                    },
                    'repositories': [asdict(repo) for repo in self.repositories]
                }
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save version control config: {e}")
    
    def add_platform_config(self, config: PlatformConfig) -> bool:
        """添加平台配置"""
        try:
            self.configs[config.platform] = config
            self.save_config()
            return True
        except Exception as e:
            logger.error(f"Failed to add platform config: {e}")
            return False
    
    def remove_platform_config(self, platform: PlatformType) -> bool:
        """移除平台配置"""
        try:
            if platform in self.configs:
                del self.configs[platform]
                self.save_config()
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to remove platform config: {e}")
            return False
    
    def get_platform_config(self, platform: PlatformType) -> Optional[PlatformConfig]:
        """获取平台配置"""
        return self.configs.get(platform)
    
    def list_platforms(self) -> List[PlatformConfig]:
        """列出所有平台配置"""
        return list(self.configs.values())
    
    def add_repository(self, repo: GitRepository) -> bool:
        """添加仓库"""
        try:
            self.repositories.append(repo)
            self.save_config()
            return True
        except Exception as e:
            logger.error(f"Failed to add repository: {e}")
            return False
    
    def remove_repository(self, repo_name: str) -> bool:
        """移除仓库"""
        try:
            self.repositories = [r for r in self.repositories if r.name != repo_name]
            self.save_config()
            return True
        except Exception as e:
            logger.error(f"Failed to remove repository: {e}")
            return False
    
    def list_repositories(self, platform: Optional[PlatformType] = None) -> List[GitRepository]:
        """列出仓库"""
        if platform:
            return [r for r in self.repositories if r.platform == platform]
        return self.repositories
    
    def get_repository(self, repo_name: str) -> Optional[GitRepository]:
        """获取仓库"""
        for repo in self.repositories:
            if repo.name == repo_name:
                return repo
        return None
    
    def push_project_to_repo(self, project_path: Path, repo_name: str, commit_message: str = "TriForge auto push") -> bool:
        """推送项目到仓库"""
        try:
            repo = self.get_repository(repo_name)
            if not repo:
                logger.error(f"Repository not found: {repo_name}")
                return False
            
            config = self.get_platform_config(repo.platform)
            if not config:
                logger.error(f"Platform config not found: {repo.platform}")
                return False
            
            # 对于自建Git平台，可能需要特殊处理
            if repo.platform == PlatformType.CUSTOM_GIT:
                # 使用HTTPS URL包含认证信息
                auth_clone_url = repo.clone_url.replace('https://', f'https://{config.auth_token}@')
            else:
                auth_clone_url = repo.clone_url
            
            # 创建临时目录
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # 克隆仓库
                if not GitOperations.clone(auth_clone_url, temp_path, repo.default_branch):
                    logger.error(f"Failed to clone repository: {repo_name}")
                    return False
                
                # 设置用户配置
                if config.username and config.email:
                    if not GitOperations.set_user_config(temp_path, config.username, config.email):
                        logger.error(f"Failed to set user config for repository: {repo_name}")
                        return False
                
                # 写入 .gitignore 防止泄露凭证
                _GITIGNORE_CONTENT = (
                    "data/settings.json\n"
                    "data/settings.json.bak\n"
                    "data/*.db\n"
                    "data/*.db-journal\n"
                    "data/*.db-wal\n"
                    "data/*.db-shm\n"
                    ".env\n"
                    ".env.local\n"
                    "*.key\n"
                    "*.pem\n"
                    "*.p12\n"
                    "__pycache__/\n"
                    "*.pyc\n"
                )
                (temp_path / ".gitignore").write_text(_GITIGNORE_CONTENT, encoding="utf-8")

                # 复制项目文件到仓库（保留.git目录）
                git_dir = temp_path / ".git"
                for item in project_path.iterdir():
                    if item.is_file():
                        dest = temp_path / item.name
                        shutil.copy2(item, dest)
                    elif item.is_dir() and item.name != ".git":
                        dest = temp_path / item.name
                        if dest.exists():
                            shutil.rmtree(dest)
                        shutil.copytree(item, dest)
                
                # 添加所有文件到git
                if not GitOperations.add_all(temp_path):
                    logger.error(f"Failed to add files to git for repository: {repo_name}")
                    return False
                
                # 提交和推送
                if not GitOperations.commit(temp_path, commit_message):
                    logger.error(f"Failed to commit to repository: {repo_name}")
                    return False
                
                if not GitOperations.push(temp_path, branch=repo.default_branch):
                    logger.error(f"Failed to push to repository: {repo_name}")
                    return False
                
                logger.info(f"Successfully pushed project to repository: {repo_name}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to push project to repository: {repo_name}: {e}")
            return False
    
    def pull_project_from_repo(self, repo_name: str, local_path: Path, branch: str = "main") -> bool:
        """从仓库拉取项目"""
        try:
            repo = self.get_repository(repo_name)
            if not repo:
                logger.error(f"Repository not found: {repo_name}")
                return False
            
            # 安全校验: 只允许在工作区根目录内拉取
            abs_path = Path(local_path).resolve()
            from .config import WORKSPACE_ROOT
            try:
                abs_path.relative_to(WORKSPACE_ROOT.resolve())
            except ValueError:
                logger.error(f"Pull target path {abs_path} is outside workspace root {WORKSPACE_ROOT}")
                return False
            
            # 清空目标目录
            if abs_path.exists():
                shutil.rmtree(abs_path)
            
            # 克隆仓库
            if not GitOperations.clone(repo.clone_url, abs_path, branch):
                logger.error(f"Failed to clone repository: {repo_name}")
                return False
            
            logger.info(f"Successfully pulled project from repository: {repo_name}, branch: {branch}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to pull project from repository: {repo_name}: {e}")
            return False

class GitHubIntegration:
    """GitHub集成"""
    
    def __init__(self, token: str):
        self.token = token
        self.api_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
    
    def get_user_info(self) -> Optional[Dict]:
        """获取用户信息"""
        try:
            url = f"{self.api_url}/user"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"GitHub API error getting user info: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Failed to get GitHub user info: {e}")
            return None
    
    def get_user_repos(self) -> List[Dict]:
        """获取用户仓库列表"""
        try:
            url = f"{self.api_url}/user/repos"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"GitHub API error: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Failed to get GitHub repos: {e}")
            return []
    
    def create_repo(self, name: str, description: str = "", private: bool = False) -> Optional[Dict]:
        """创建仓库"""
        try:
            url = f"{self.api_url}/user/repos"
            data = {
                "name": name,
                "description": description,
                "private": private,
                "auto_init": True
            }
            response = requests.post(url, headers=self.headers, json=data)
            if response.status_code == 201:
                return response.json()
            else:
                logger.error(f"Failed to create GitHub repo: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Failed to create GitHub repo: {e}")
            return None
    
    def get_repo(self, owner: str, repo: str) -> Optional[Dict]:
        """获取仓库信息"""
        try:
            url = f"{self.api_url}/repos/{owner}/{repo}"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get GitHub repo: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Failed to get GitHub repo: {e}")
            return None

class GiteeIntegration:
    """Gitee集成"""
    
    def __init__(self, token: str):
        self.token = token
        self.api_url = "https://gitee.com/api/v5"
        self.headers = {
            "Authorization": f"token {token}"
        }
    
    def get_user_info(self) -> Optional[Dict]:
        """获取用户信息"""
        try:
            url = f"{self.api_url}/user"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Gitee API error getting user info: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Failed to get Gitee user info: {e}")
            return None
    
    def get_user_repos(self) -> List[Dict]:
        """获取用户仓库列表"""
        try:
            url = f"{self.api_url}/user/repos"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Gitee API error: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Failed to get Gitee repos: {e}")
            return []
    
    def create_repo(self, name: str, description: str = "", private: bool = False) -> Optional[Dict]:
        """创建仓库"""
        try:
            url = f"{self.api_url}/user/repos"
            data = {
                "name": name,
                "description": description,
                "private": str(private).lower(),
                "auto_init": True
            }
            response = requests.post(url, headers=self.headers, data=data)
            if response.status_code == 201:
                return response.json()
            else:
                logger.error(f"Failed to create Gitee repo: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Failed to create Gitee repo: {e}")
            return None
    
    def get_repo(self, owner: str, repo: str) -> Optional[Dict]:
        """获取仓库信息"""
        try:
            url = f"{self.api_url}/repos/{owner}/{repo}"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get Gitee repo: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Failed to get Gitee repo: {e}")
            return None

class GitLabIntegration:
    """GitLab集成"""
    
    def __init__(self, token: str):
        self.token = token
        self.api_url = "https://gitlab.com/api/v4"
        self.headers = {
            "Authorization": f"Bearer {token}"
        }
    
    def get_user_info(self) -> Optional[Dict]:
        """获取用户信息"""
        try:
            url = f"{self.api_url}/user"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"GitLab API error getting user info: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Failed to get GitLab user info: {e}")
            return None
    
    def get_user_repos(self) -> List[Dict]:
        """获取用户仓库列表"""
        try:
            url = f"{self.api_url}/projects"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"GitLab API error: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"Failed to get GitLab repos: {e}")
            return []
    
    def create_repo(self, name: str, description: str = "", private: bool = False) -> Optional[Dict]:
        """创建仓库"""
        try:
            url = f"{self.api_url}/projects"
            data = {
                "name": name,
                "description": description,
                "visibility": "private" if private else "public",
                "initialize_with_readme": True
            }
            response = requests.post(url, headers=self.headers, json=data)
            if response.status_code == 201:
                return response.json()
            else:
                logger.error(f"Failed to create GitLab repo: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Failed to create GitLab repo: {e}")
            return None
    
    def get_repo(self, project_id: str) -> Optional[Dict]:
        """获取仓库信息"""
        try:
            url = f"{self.api_url}/projects/{project_id}"
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get GitLab repo: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Failed to get GitLab repo: {e}")
            return None

def get_integration(platform: PlatformType, config: PlatformConfig) -> Optional[Any]:
    """获取平台集成实例"""
    if platform == PlatformType.GITHUB:
        return GitHubIntegration(config.auth_token)
    elif platform == PlatformType.GITEE:
        return GiteeIntegration(config.auth_token)
    elif platform == PlatformType.GITLAB:
        return GitLabIntegration(config.auth_token)
    elif platform == PlatformType.CUSTOM_GIT:
        logger.warning("CustomGitIntegration is not implemented")
        return None
    else:
        logger.warning(f"Unsupported platform: {platform}")
        return None

# CustomGitIntegration was removed — the class was entirely mock data.
# Re-implement against a real Gitea/Gogs API if needed.