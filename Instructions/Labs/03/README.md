﻿# 模块 3 - 有关源文件的数据工程注意事项

在本实验室中，你将在讲师的指导下独自或以小组形式在 20 分钟内通读下面给出的信息。然后回答问题并根据要求在课堂上呈现你的发现。 

## 团队白板活动

Wide World Importers 准备生成一个管道，用于将其销售交易从 Oracle Database 中的表复制到 Data Lake。

**要求**

* 复制数据的管道将按计划每天运行一次。
* 该公司希望以尽可能少的转换工作来引入这些原始数据。
* 他们想要确保 Data Lake 始终包含原始数据的副本，以便下游处理出现计算或转换错误时，依旧可以从原始数据重新计算。
* 此外，他们希望避免文件格式规定哪些工具可以用来检查和处理数据，确保所选的文件格式可以被尽可能广泛的行业标准工具使用。
* 对于此类数据的典型探索性和分析性查询，文件夹结构需要具有高性能。  

**数据示例**

WWI 提供了以下数据示例。可以假设他们手动选择了最能代表数据的行**。

|SaleKey|CityKey|CustomerKey|BillToCustomerKey|StockItemKey|DeliveryDateKey|SalespersonKey|WWIInvoiceID|Description|Package|Quantity|UnitPrice|TaxRate|TotalExcludingTax|TaxAmount|Profit|TotalIncludingTax|TotalDryItems|TotalChillerItems|LineageKey
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- 
|294018|98706|0|0|25|2012-01-04|156|57894|Black and orange, handle with care despatch tape  48mmx75m|Each|144|3.70|15.000|532.80|79.92|345.60|612.72|144|0|14
|294019|98706|0|0|216|2012-01-04|156|57894|USB, food flash drive - sushi roll|Each|5|32.00|15.000|160.00|24.00|100.00|184.00|5|0|14
|294020|98706|0|0|168|2012-01-04|156|57894|IT joke mug - keyboard not found � press F1 to continue (White)|Each|10|13.00|15.000|130.00|19.50|85.00|149.50|10|0|14
|294021|98706|0|0|100|2012-01-04|156|57894|Dinosaur battery-powered slippers (Green) L|Each|4|32.00|15.000|128.00|19.20|96.00|147.20|4|0|14

## 白板

打开活动白板，在活动 1 区域内提供以下问题的答案。

*以下问题已在提供的白板模板中呈现。*

问题

1. 应使用什么文件格式来保存原始数据？为什么推荐这种文件格式，请说出至少两个理由？

2. WWI 应使用哪些特定设置来配置数据集序列化到磁盘的方式（特别注意 `Description` 字段）？为什么建议这些设置？

3. 用关系图表示你建议在分层文件系统中使用的文件夹结构。请务必指明文件系统、文件夹和文件，并描述每一层（文件系统、文件夹和文件）的名称是如何产生的。

4. 你的文件夹结构如何支持对这类数据进行典型的探索性和分析性查询的性能？

5. WWI 将这些数据视为机密，因为如果它落入竞争者手中，将会造成无法弥补的损害。用关系图说明你将如何部署 Data Lake 并保护对 Data Lake 终结点的访问？请务必说明数据如何在 Azure Synapse Analytics 工作区和 Data Lake 之间流动，并解释为什么这可以满足 WWI 的要求。使用调色板中提供的图标来绘制解决方案。

6. WWI 希望强制要求对销售数据的任何修改只能在本年度进行，同时允许所有授权用户查询全部数据。关于之前向 WWI 推荐的文件夹结构，如何使用 RBAC 和 ACL 来实现？请说明在 2020 年初和年底需要进行哪些操作。请使用提供的调色板绘制关系图说明你的安全组、内置角色和访问权限。