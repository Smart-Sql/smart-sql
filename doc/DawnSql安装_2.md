# 安装
## 1、环境要求

1. JDK：Oracle JDK8及以上，Open JDK8及以上，IBM JDK8及以上
2. OS：Linux（任何版本），Mac OS X（10.6及以上），Windows(XP及以上)，Windows Server（2008及以上），Oracle Solaris
3. 网络：没有限制（建议10G甚至更快的网络带宽）
4. 架构：x86，x64，SPARC，PowerPC

## 2、下载地址
### 2.1、Java 下载地址：https://www.java.com/en/download/
<br/>
### 2.2、DawnSql 下载地址：https://share.weiyun.com/vRJ7a1Ss
<br/>
### 2.3、Dbeaver 下载地址：https://dbeaver.io/download/
<br/>
### 2.4、DawnSqlClient 下载地址：
**DawnSqlClient 是我们开源的一个 web 的 Sql 编辑器，它可以取代桌面的 Dbeaver ，同时也能够快速方便的扩展成其它的应用。**

## 3、安装并激活 DawnSql

下载: DawnSql(压缩包) 
  在命令行中转到安装文件夹的bin目录：
```shell
> cd {DawnSql}/bin/ 
```

Linux Mac 下启动：<br/>
```shell
> ./DawnSql.sh
```

Windows 下启动：<br/>
```shell
> ./DawnSql.bat
```

激活集群<br/>
Linux/Mac 下激活集群：<br/>
```shell
> ./control.sh --activate
```

Windows 下激活集群：<br/>
```shell
> ./control.bat --activate
```