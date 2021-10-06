# DP-203T00: Azure 上的数据工程

欢迎学习课程 DP-203：Azure 上的数据工程。为了支持这门课程，我们需要对课程内容进行更新，使其与本课程中使用的 Azure 服务保持一致。  我们将在 GitHub 上发布实验室说明和实验室文件，以允许课程作者和 MCT 之间的开放式协作，从而让内容与 Azure 平台中的更改保持同步。

## 实验室概述

以下为每个模块的实验室目标摘要：

### 第 1 天

#### [模块 00：实验室环境设置](Instructions/Labs/LAB_00_lab_setup_instructions.md)

完成本课程的实验室环境设置。

#### [模块 01：探索用于数据工程工作负载的计算和存储选项](Instructions/Labs/LAB_01_compute_and_storage_options.md)

本实验室介绍用于构造 Data Lake 以及优化用于探索、流式处理和批处理工作负载的文件的方法。学生将学习在通过批处理和流式处理转换文件时如何将 Data Lake 整理到数据细化级别。学生还将体验在 Azure Synapse Analytics 中使用 Apache Spark。  他们将了解如何对其数据集（例如 CSV、JSON 和 Parquet 文件）创建索引，并使用包括 Hyperspace 和 MSSParkUtils 在内的 Spark 库将这些索引用于潜在的查询和工作负载加速。

#### [模块 02：使用 Azure Synapse Analytics 无服务器 SQL 池运行交互式查询](Instructions/Labs/LAB_02_queries_using_serverless_sql_pools.md)

在本实验室中，学生将了解如何通过 Azure Synapse Analytics 中的无服务器 SQL 池执行的 T-SQL 语句使用 Data Lake 中的文件和外部文件源。学生将查询 Data Lake 中的 Parquet 文件，以及外部数据存储中的 CSV 文件。接下来，他们将创建 Azure Active Directory 安全组并通过基于角色的访问控制 (RBAC) 和访问控制列表 (ACL) 强制访问 Data Lake 中的文件。

#### [模块 03：Azure Databricks 中的数据探索和转换](Instructions/Labs/LAB_03_data_transformation_in_databricks.md)

本实验室将指导你如何使用各种 Apache Spark DataFrame 方法探索和转换 Azure Databricks 中的数据。你将了解如何执行标准 DataFrame 方法以探索和转换数据。你还将了解如何执行更高级的任务，例如删除重复数据、操作数据/时间值、重命名列以及聚合数据。他们将预配所选的引入技术，并将其与流分析集成，以创建用于处理流式数据的解决方案。

### 第 2 天

#### [模块 04：使用 Apache Spark 探索、转换数据并将数据加载到数据仓库中](Instructions/Labs/LAB_04_data_warehouse_using_apache_spark.md)

此实验室将指导你如何浏览 Data Lake 中的数据、转换数据以及将数据加载到关系数据存储中。你将浏览 Parquet 和 JSON 文件并使用技术查询和转换具有分层结构的 JSON 文件。然后，使用 Apache Spark 将数据加载到数据仓库中，并将 Data Lake 中的 Parquet 数据与专用 SQL 池中的数据联接起来。

#### [模块 05：将数据引入和加载到数据仓库中](Instructions/Labs/LAB_05_load_data_into_the_data_warehouse.md)

本实验室将教授学生如何通过 T-SQL 脚本和 Synapse Analytics 集成管道将数据引入数据仓库。学生将了解如何使用 T-SQL 通过 PolyBase 和 COPY 将数据加载到 Synapse 专用 SQL 池。还将了解如何使用工作负载管理以及 Azure Synapse 管道中的“复制”活动实现 PB 级数据引入。

#### [模块 06：使用 Azure 数据工厂或 Azure Synapse 管道转换数据](Instructions/Labs/LAB_06_transform_data_with_pipelines.md)

本实验室教学生如何执行以下操作：生成数据集成管道以从多个数据源引入、使用映射数据流和笔记本转换数据、将数据移动到一个或多个数据接收器中。

### 第 3 天

#### [模块 07：将笔记本中的数据与 Azure 数据工厂或 Azure Synapse 管道集成](Instructions/Labs/LAB_07_integrate_data_from_notebooks.md)

在本实验室中，学生将创建笔记本来查询过去 12 个月内他们执行的用户活动和购买项。然后，使用“新建笔记本”活动将笔记本添加到管道并在其构建过程中，在映射数据流之后执行此笔记本。配置时，学生将实现参数，以在控制流中添加动态内容，并验证参数的使用方式。

#### [模块 08：使用 Azure Synapse Analytics 实现端到端安全性](Instructions/Labs/LAB_08_security_with_synapse_analytics.md)

在本实验室中，学生将了解如何保护 Synapse Analytics 工作区及其配套基础结构。学生将观察 SQL Active Directory 管理员，管理 IP 防火墙规则，使用 Azure 密钥保管库管理机密以及通过与密钥保管库关联的服务和管道活动访问这些机密。学生将了解如何使用专用 SQL 池实现列级别安全性、行级别安全性以及动态数据掩码。

#### [模块 09：使用 Azure Synapse Link 支持混合事务分析处理 (HTAP)](Instructions/Labs/LAB_09_htap_with_azure_synapse_link.md)

在此实验室中，你将了解如何使用 Azure Synapse Link 将 Azure Cosmos DB 帐户无缝连接到 Synapse 工作区。你将了解如何启用并配置 Synapse Link，以及如何使用 Apache Spark 和 SQL 无服务器查询 Azure Cosmos DB 分析存储。
### 第 4 天
#### [模块 10：使用流分析进行实时流处理](Instructions/Labs/LAB_10_stream_analytics.md)

本实验室将指导你如何使用 Azure Stream Analytics 处理流式数据。你将车辆遥测数据引入事件中心，然后使用 Azure 流分析中的各种开窗函数实时处理该数据。将数据输出到 Azure Synapse Analytics。最后，你将学习如何缩放流分析作业以提高吞吐量。

#### [模块 11：使用事件中心和 Azure Databricks 创建流式处理解决方案](Instructions/Labs/LAB_11_stream_with_azure_databricks.md)

在此实验室中，你将了解如何在 Azure Databricks 中使用事件中心和 Spark 结构化流大规模引入和处理流式数据。你将了解结构化流的主要功能和使用方式。你将实现滑动窗口以聚合数据块并应用水印以删除过时数据。最后，将连接到事件中心以读取流和写入流。

- **你是 MCT 吗？** - 请参阅 [GitHub MCT 用户指南](https://microsoftlearning.github.io/MCT-User-Guide/)。
                                                                       
## 我应该如何使用与已发布 MOC 文件相关的这些文件？

- 在讲授课程内容时，主要参考的仍将是讲师手册和 PowerPoint。

- GitHub 上的这些文件适合与学生手册结合使用，不过在 GitHub 中作为中央存储库使用，因此 MCT 和课程作者可以获得一个用于共享最新实验室文件的共享源。

- 可在 /Instructions/Labs 文件夹中查看各个模块的实验室说明。该位置中的每个子文件夹是指每个模块。例如，Lab01 与 module01 等相关。每个文件夹中都包含一个 README.md 文件，其中包含学生将遵循的实验室说明。

- 对于每次授课，建议培训师查看 GitHub 中的内容，了解是否为支持最新 Azure 服务而进行了更改，并获取最新文件来进行授课。

- 请注意，你在这些实验室说明中看到的某些图像不一定会反映你将在此课程中使用的实验室环境的状态。例如，浏览 Data Lake 中的文件时，你可能会在图像中看到其他文件夹，而你环境中可能不存在这些文件夹。这是设计使然，实验室说明将仍然有效。

## 如何处理对学生手册的更改？

- 我们将每季度审核一次学生手册，并根据需要通过常规 MOC 发布渠道进行更新。

## 如何参与内容编辑？

- 任何 MCT 都可对 GitHub 存储库中的代码或内容提出问题，Microsoft 和课程作者将根据需要进行分类，包括内容和实验室代码更改。

## 课堂材料

强烈建议 MCT 和合作伙伴查看这些材料，然后将它们分别提供给学生。  如果在课程中直接让学生访问 GitHub 来查看实验室步骤，将需要学生访问另一个 UI，这样更容易使其迷惑不解。向学生解释他们收到单独实验室说明的原因，这可以突显基于云的界面和平台具有不断变化的性质。仅为讲授此课程的 MCT 提供有关访问 GitHub 上的文件以及导航 GitHub 站点的 Microsoft Learning 支持。

## 我们要做什么？

- 为了支持本课程，我们需要经常更新课程内容，以使其与课程中使用的 Azure 服务保持同步。  我们将在 GitHub 上发布实验室说明和实验室文件，以允许课程作者和 MCT 之间的开放式协作，从而让内容与 Azure 平台中的更改保持同步。

- 我们希望能在这些实验室中引入前所未有的协作感：如果 Azure 发生更改，而你先在直播讲课时发现这一点，你可以直接在实验室源中进行改进。  这样可以帮助到其他 MCT。

## 我应该如何使用与已发布 MOC 文件相关的这些文件？

- 在讲授课程内容时，主要参考的仍将是讲师手册和 PowerPoint。

- GitHub 上的这些文件适合与学生手册结合使用，不过在 GitHub 中作为中央存储库使用，因此 MCT 和课程作者可以获得一个用于共享最新实验室文件的共享源。

- 对于每次授课，建议培训师查看 GitHub 中的内容，了解是否为支持最新 Azure 服务而进行了更改，并获取最新文件来进行授课。

## 如何处理对学生手册的更改？

- 我们将每季度审核一次学生手册，并根据需要通过常规 MOC 发布渠道进行更新。

## 如何参与内容编辑？

- 每位 MCT 都可向 GitHub 存储库中的代码或内容提交拉取请求，Microsoft 和课程作者将对内容进行鉴别分类，并根据需要采纳内容和实验室代码更改。

- 你可以提交 Bug、更改、改进内容和想法。  比我们先发掘 Azure 的新功能？  提交新的演示吧！

## 备注

### 课堂材料

强烈建议 MCT 和合作伙伴查看这些材料，然后将它们分别提供给学生。  如果在课程中直接让学生访问 GitHub 来查看实验室步骤，将需要学生访问另一个 UI，这样更容易使其迷惑不解。向学生解释他们收到单独实验室说明的原因，这可以突显基于云的界面和平台具有不断变化的性质。仅为讲授此课程的 MCT 提供有关访问 GitHub 上的文件以及导航 GitHub 站点的 Microsoft Learning 支持。
