# Change Log

## 2018-09-06

- 更新`NotLoginError`的处理方式
- 创建默认配置文件`default.cfg`，只作为配置模板，不包含个人和服务器信息
- 需创建实际配置文件`myconfig.cfg`，并修改其中的内容，不加入git仓库
- 配置文件中加入爬取结果过期时间选项，目前为60天
- 爬取结果存入MongoDB时，由插入改为替换
