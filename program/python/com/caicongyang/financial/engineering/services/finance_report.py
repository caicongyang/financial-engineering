import pandas as pd
import os
import sys
from datetime import datetime

# 添加必要的路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 简化路径处理，直接添加program/python
project_root = os.path.abspath(os.path.join(current_dir, "../../../../../../../"))
python_path = os.path.join(project_root, "program/python")
sys.path.insert(0, python_path)

# 导入模块
from com.caicongyang.financial.engineering.services.db_utils import save_report_to_db, init_db, engine
from com.caicongyang.financial.engineering.services.deepseek_chat import DeepSeekChat

# 初始化数据库
try:
    init_db()
except Exception as e:
    print(f"数据库初始化失败: {e}")
    print("程序将继续运行，但数据库功能将不可用")


def analyze_finance_report(chat: DeepSeekChat, finance_report_content: str, company_name: str) -> str:
    """分析财务报告
    
    Args:
        chat: DeepSeekChat实例
        finance_report_content: 财务报告内容
        company_name: 公司名称
    """

    try:
        # 构建分析请求，包含角色提示
        analysis_request = f"""

帮我将这个{company_name}财报文档生成网页，不要遗漏信息

{finance_report_content}

根据上面内容生成一个 HTML 动态网页

1. 使用Bento Grid风格的视觉设计，纯黑色底配合特斯拉红色#E31937颜色作为高亮

2. 强调超大字体或数字突出核心要点，画面中有超大视觉元素强调重点，与小元素的比例形成反差

3. 中英文混用，中文大字体粗体，英文小字作为点缀

4. 简洁的勾线图形化作为数据可视化或者配图元素

5. 运用高亮色自身透明度渐变制造科技感，但是不同高亮色不要互相渐变

6. 模仿 apple 官网的动效，向下滚动鼠标配合动效

8. 数据可以引用在线的图表组件，样式需要跟主题一致

9. 使用 Framer Motion （通过CDN引入）

10. 使用HTML5、TailwindCSS 3.0+（通过CDN引入）和必要的JavaScript

11. 使用专业图标库如Font Awesome或Material Icons（通过CDN引入）

12. 避免使用emoji作为主要图标

13. 不要省略内容要点
"""

        # 发送请求并获取分析结果
        result = chat.simple_chat(analysis_request)

        # 返回分析结果
        return result

    except Exception as e:
        return f"分析过程中发生错误: {str(e)}"


def save_to_html(content: str, company_name: str) -> str:
    """将分析结果保存为HTML文件"""
    try:
        # 创建文件名，包含公司名称和当前日期时间
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{company_name}_finance_report_{now}.html"

        # 写入文件
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)

        return filename
    except Exception as e:
        print(f"保存HTML文件时出错: {e}")
        return None


def save_finance_analysis_to_db(content: str, company_name: str, company_ticker: str = None, report_summary=None) -> str:
    """将财务分析结果保存到MySQL数据库
    
    Args:
        content: 报告内容 (HTML格式)
        company_name: 公司名称
        company_ticker: 公司股票代码
        report_summary: 相关财报数据摘要
        
    Returns:
        report_id: 报告ID
    """
    try:
        # 生成标题
        title = f"{company_name}财务报告分析 {datetime.now().strftime('%Y-%m-%d')}"

        # 构建标签
        tags = f'财务报告,公司分析,{company_name}'
        if company_ticker:
            tags += f',{company_ticker}'

        # 保存到数据库
        report_id = save_report_to_db(
            report_type='finance',
            code=company_ticker if company_ticker else company_name,
            title=title,
            content=content,
            related_data=report_summary,  # 直接传入字典，db_utils会处理JSON转换
            tags=tags
        )

        return report_id
    except Exception as e:
        print(f"保存财务分析到数据库时出错: {e}")
        return None


def generate_finance_report_html(finance_report_content=None, company_name=None, company_ticker=None):
    """生成财务报告分析HTML网页
    
    Args:
        finance_report_content: 财务报告内容
        company_name: 公司名称
        company_ticker: 公司股票代码
    
    Returns:
        HTML内容
    """
    try:
        # 检查参数
        if finance_report_content is None or company_name is None:
            raise ValueError("缺少必要参数: 需要财务报告内容和公司名称")

        print(f"开始分析 {company_name} 的财务报告...")

        # 准备数据摘要
        data_summary = {
            "company_name": company_name,
            "company_ticker": company_ticker,
            "analysis_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "content_length": len(finance_report_content),
            "report_type": "财务季报" if "季报" in finance_report_content[:1000] else "财务年报"
        }

        # 初始化 DeepSeekChat 并尝试分析
        try:
            # 分析财务报告
            print("初始化 DeepSeekChat 客户端...")
            chat = DeepSeekChat()
            print("开始分析财务报告数据...")
            result = analyze_finance_report(chat, finance_report_content, company_name)
        except Exception as e:
            print(f"LLM分析失败: {str(e)}，创建基础报告...")
            # 创建一个简单的基础HTML报告
            result = f"""<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{company_name} - 财务报告分析</title>
    <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
</head>
<body class="bg-black text-white">
    <div class="container mx-auto px-4 py-10">
        <h1 class="text-4xl font-bold mb-8">{company_name} 财务报告分析</h1>
        
        <div class="bg-gray-900 p-6 rounded-lg mb-6">
            <h2 class="text-2xl font-bold mb-4">分析状态</h2>
            <p class="text-red-500 mb-4">由于LLM服务连接失败，无法提供详细的财务分析。错误信息: {str(e)}</p>
            <p>这是一个基础数据收集报告，仅提供基本信息。</p>
        </div>
        
        <div class="bg-gray-900 p-6 rounded-lg">
            <h2 class="text-2xl font-bold mb-4">报告基本信息</h2>
            <ul class="space-y-2">
                <li><strong>公司名称:</strong> {company_name}</li>
                <li><strong>分析时间:</strong> {data_summary['analysis_timestamp']}</li>
                <li><strong>报告类型:</strong> {data_summary['report_type']}</li>
                {f'<li><strong>股票代码:</strong> {company_ticker}</li>' if company_ticker else ''}
            </ul>
        </div>
        
        <div class="mt-8 text-center">
            <p>请稍后再试或联系系统管理员以解决此问题。</p>
        </div>
    </div>
</body>
</html>"""

        # 保存分析结果到MySQL数据库
        report_id = save_finance_analysis_to_db(result, company_name, company_ticker, data_summary)
        if report_id:
            print(f"分析报告已保存到MySQL数据库，ID: {report_id}")

        # 同时也保存到本地文件
        html_file = save_to_html(result, company_name)
        if html_file:
            print(f"分析报告已同时保存到本地文件: {html_file}")

        # 打印分析结果状态
        print("\n" + "=" * 50)
        print(f"{company_name}财务报告分析完成")
        print("=" * 50 + "\n")
        print(f"生成的HTML长度: {len(result)} 字符")
        print(f"HTML文件保存为: {html_file}")
        print("\n" + "=" * 50)

        return result

    except Exception as e:
        print(f"程序运行出错: {str(e)}")
        return None


def main(finance_report_content=None, company_name="特斯拉", company_ticker="TSLA"):
    """主函数，调用生成财务报告分析HTML"""
    if finance_report_content is None:
        # 定义可能的文件位置列表
        file_name = "tsla-20250129-gen.txt"
        possible_locations = [
            # 1. 脚本当前目录
            os.path.join(current_dir, file_name),
            # 2. 项目根目录
            os.path.join(project_root, file_name),
            # 3. 当前工作目录
            os.path.join(os.getcwd(), file_name)
        ]
        
        # 尝试从所有可能的位置读取文件
        for file_path in possible_locations:
            print(f"尝试读取文件: {file_path}")
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    finance_report_content = f.read()
                print(f"成功从 {file_path} 读取文本内容，共 {len(finance_report_content)} 字符")
                break  # 成功读取文件后跳出循环
            except Exception as e:
                print(f"无法从 {file_path} 读取文件: {e}")
        
        # 如果尝试所有位置后仍然没有读取到文件内容，则提示用户手动输入
        if finance_report_content is None:
            print("\n所有默认位置都无法找到文件，请手动提供财务报告内容")
            input_type = input("输入类型 (1: 文本内容, 2: 文件路径): ")
            
            if input_type == "1":
                print("请输入财务报告内容 (输入END单独一行结束):")
                lines = []
                while True:
                    line = input()
                    if line == "END":
                        break
                    lines.append(line)
                finance_report_content = "\n".join(lines)
            elif input_type == "2":
                file_path = input("请输入文件路径: ")
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        finance_report_content = f.read()
                except Exception as e:
                    print(f"读取文件失败: {e}")
                    return None
            else:
                print("无效的输入类型")
                return None
    
    return generate_finance_report_html(finance_report_content, company_name, company_ticker)


# 直接运行脚本时执行
if __name__ == "__main__":
    try:
        print("=" * 50)
        print("特斯拉财报分析HTML生成器")
        print("=" * 50)
        
        result = main()
        
        if result:
            print("\n程序已成功执行!")
            print("HTML报告已生成")
        else:
            print("\n程序执行失败，请检查错误信息")
    except Exception as e:
        print(f"程序执行过程中发生错误: {e}") 