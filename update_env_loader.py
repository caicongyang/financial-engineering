#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自动更新整个项目中所有Python文件，
将复杂的环境变量加载方式替换为使用通用的env_loader模块

使用方法:
1. 将此脚本放在项目根目录下
2. 运行: python update_env_loader.py
3. 脚本会自动搜索并更新所有Python文件中的环境变量加载方式

如果路径不正确，脚本会提示输入正确的路径。
"""

import os
import re

def update_file(filepath):
    """更新单个文件中的环境变量加载方式"""
    print(f"处理文件: {filepath}")
    
    # 读取文件内容
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.read()
    except UnicodeDecodeError:
        print(f"  跳过非文本文件: {filepath}")
        return False
    
    # 忽略__pycache__目录中的文件
    if "__pycache__" in filepath:
        print(f"  跳过缓存文件: {filepath}")
        return False
    
    # 检查文件是否包含加载环境变量的代码
    if 'load_dotenv(' not in content:
        print(f"  文件不包含环境变量加载代码，跳过: {filepath}")
        return False
    
    # 1. 检查文件是否已经导入env_loader
    if 'from com.caicongyang.financial.engineering.utils.env_loader import load_env' in content:
        print(f"  文件已更新，跳过: {filepath}")
        return False
    
    # 备份原文件
    backup_path = filepath + '.bak'
    with open(backup_path, 'w', encoding='utf-8') as file:
        file.write(content)
    
    # 2. 添加导入语句
    import_pattern = r'from dotenv import load_dotenv'
    new_import = 'from dotenv import load_dotenv\nfrom com.caicongyang.financial.engineering.utils.env_loader import load_env'
    content = re.sub(import_pattern, new_import, content)
    
    # 3. 替换环境变量加载方式 - 更强大的模式匹配
    # 匹配以下几种模式：
    
    # 复杂的os.path.join模式 (严格模式)
    join_pattern1 = r'# 加载环境变量\nload_dotenv\(os\.path\.join\(.*?\), \'\.env\'\)\)'
    
    # 复杂的os.path.join模式 (宽松模式)
    join_pattern2 = r'load_dotenv\(os\.path\.join\(.*?\.env.*?\)\)'
    
    # 已经更新为优雅方式但不使用env_loader的模式
    elegant_pattern = r'# 加载环境变量.*?current_file = os\.path\.abspath.*?load_dotenv\(dotenv_path\)'
    
    # 通用替换文本
    new_load = '# 加载环境变量 - 使用通用加载模块\nload_env()'
    
    # 应用替换
    original_content = content
    content = re.sub(join_pattern1, new_load, content, flags=re.DOTALL)
    content = re.sub(join_pattern2, 'load_env()', content, flags=re.DOTALL)
    content = re.sub(elegant_pattern, new_load, content, flags=re.DOTALL)
    
    # 如果文件没有变化，则恢复备份并跳过
    if content == original_content:
        print(f"  文件没有需要更新的部分，跳过: {filepath}")
        os.remove(backup_path)
        return False
    
    # 写回文件
    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(content)
    
    print(f"  文件已更新: {filepath}")
    return True

def find_python_files(directory):
    """递归查找目录中的所有Python文件"""
    python_files = []
    for root, dirs, files in os.walk(directory):
        # 排除虚拟环境和.git目录
        dirs[:] = [d for d in dirs if d not in ['venv', '.venv', 'env', '.env', '.git', '__pycache__']]
        
        for file in files:
            if file.endswith('.py'):
                full_path = os.path.join(root, file)
                python_files.append(full_path)
    return python_files

def main():
    # 获取项目根目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = script_dir
    
    # 检查目录是否存在
    if not os.path.exists(project_root):
        print(f"目录不存在: {project_root}")
        project_root = input("请输入项目根目录的完整路径: ")
        if not os.path.exists(project_root):
            print("路径无效，退出程序")
            return
    
    print(f"开始处理项目根目录: {project_root}")
    
    # 查找所有Python文件
    python_files = find_python_files(project_root)
    print(f"找到 {len(python_files)} 个Python文件")
    
    # 更新文件
    updated_count = 0
    for file_path in python_files:
        if update_file(file_path):
            updated_count += 1
    
    print(f"\n共更新了 {updated_count} 个文件")
    print("\n更新完成！现在所有文件都使用了通用的环境变量加载模块。")
    print("备份文件保存为 *.py.bak，如果需要恢复，可以手动重命名。")
    print("如果需要进一步检查，可以手动查看更新后的文件。")

if __name__ == "__main__":
    main() 