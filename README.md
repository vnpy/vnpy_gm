# VeighNa框架的掘金交易接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.1.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.10|3.11|3.12|3.13-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

用于对接掘金量化终端开发的掘金数据服务。

## 安装

安装环境推荐基于4.0.0版本以上的【[**VeighNa Studio**](https://www.vnpy.com)】。

直接使用pip命令：

```
pip install vnpy_gm
```

或者下载源代码后，解压后在cmd中运行：

```
pip install .
```

## 使用

在VeighNa中使用米筐RQData时，需要在全局配置中填写以下字段信息：

|名称|含义|必填|举例|
|---------|----|---|---|
|datafeed.name|名称|是|gm|
|datafeed.password|密码|是|(请填写购买或申请试用掘金数据服务后，掘金数据服务提供的token)|

## 连接

请注意：

1. 连接vnpy_gm的token参数可从掘金量化终端的【系统设置】-【密钥管理】处获取。

2. 使用时请保持掘金量化终端连接。
