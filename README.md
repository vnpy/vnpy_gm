# VeighNa框架的掘金交易接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7|3.8|3.9|3.10-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

用于对接掘金量化终端开发的掘金交易接口，行情数据由TuShare数据服务提供。

## 安装

安装环境推荐基于3.3.0版本以上的【[**VeighNa Studio**](https://www.vnpy.com)】。

直接使用pip命令：

```
pip install vnpy_gm
```

或者下载源代码后，解压后在cmd中运行：

```
pip install .
```

## 使用

以脚本方式启动（script/run.py）：

```
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_gm import GmGateway


def main():
    """主入口函数"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(GmGateway)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
```

## 连接

请注意：

1. 连接vnpy_gm的token参数可从掘金量化终端的【系统设置】-【密钥管理】处获取，连接vnpy_gm的账户ID参数可从掘金量化终端的【账户管理】-【账户名称】处获取。

2. 连接前请在【全局配置】处配置好TuShare数据服务。

3. 使用时请保持掘金量化终端连接。
