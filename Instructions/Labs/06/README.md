# 模块 6 - Azure Databricks 中的数据探索和转换

本模块将指导你如何使用各种 Apache Spark DataFrame 方法探索和转换 Azure Databricks 中的数据。学生将了解如何执行标准 DataFrame 方法以探索和转换数据。他们还将了解如何执行更高级的任务，例如删除重复数据、操作日期/时间值、重命名列以及聚合数据。

在本模块中，学生将能够：

- 使用 Azure Databricks 中的 DataFrame 探索和筛选数据
- 缓存 DataFrame 以加快后续查询
- 删除重复数据
- 操作日期/时间值
- 删除和重命名 DataFrame 列
- 聚合存储在 DataFrame 中的数据

## 实验室详细信息

- [模块 6 - Azure Databricks 中的数据探索和转换](#module-6---data-exploration-and-transformation-in-azure-databricks)
  - [实验室详细信息](#lab-details)
  - [实验室 1 - 使用 DataFrame](#lab-1---working-with-dataframes)
    - [动手实验室准备工作](#before-the-hands-on-lab)
      - [任务 1 - 创建并配置 Azure Databricks 工作区](#task-1---create-and-configure-the-azure-databricks-workspace)
    - [练习 1：完成实验室笔记本](#exercise-1-complete-the-lab-notebook)
      - [任务 1：克隆 Databricks 存档](#task-1-clone-the-databricks-archive)
      - [任务 2：完成“描述 DataFrame”笔记本](#task-2-complete-the-describe-a-dataframe-notebook)
    - [练习 2：完成“使用 DataFrame”笔记本](#exercise-2-complete-the-working-with-dataframes-notebook)
    - [练习 3：完成“显示功能”笔记本](#exercise-3-complete-the-display-function-notebook)
    - [练习 4：完成“不同的文章”练习笔记本](#exercise-4-complete-the-distinct-articles-exercise-notebook)
  - [实验室 2 - 使用 DataFrame 高级方法](#lab-2---working-with-dataframes-advanced-methods)
    - [练习 2：完成实验室笔记本](#exercise-2-complete-the-lab-notebook)
      - [任务 1：克隆 Databricks 存档](#task-1-clone-the-databricks-archive-1)
      - [任务 2：完成“日期和时间操作”笔记本](#task-2-complete-the-date-and-time-manipulation-notebook)
    - [练习 3：完成“使用聚合函数”笔记本](#exercise-3-complete-the-use-aggregate-functions-notebook)
    - [练习 4：完成“删除重复数据”练习笔记本](#exercise-4-complete-the-de-duping-data-exercise-notebook)

## 实验室 1 - 使用 DataFrame

Azure Databricks 中的数据处理是通过定义用于读取和处理数据的 DataFrame 来完成的。本实验室将介绍如何使用 Azure Databricks DataFrame 读取数据。你需要在 Databricks 笔记本中完成练习。首先，你需要有权访问 Azure Databricks 工作区。如果没有可用的工作区，请按照以下说明进行操作。否则，可跳到 `Clone the Databricks archive` 步骤。

### 动手实验室准备工作

> **备注：** 如果**不**使用托管实验室环境，而使用你自己的 Azure 订阅，则仅完成`Before the hands-on lab`步骤。否则，请跳转到练习 1。

在分步完成此实验室中的练习时，请确保可访问具有可用群集的 Azure Databricks 工作区。执行以下步骤以配置工作区。

#### 任务 1 - 创建并配置 Azure Databricks 工作区

按照[实验室 06 设置说明](https://github.com/solliancenet/microsoft-data-engineering-ilt-deploy/blob/main/setup/06/lab-01-setup.md)创建并配置工作区。

### 练习 1：完成实验室笔记本

#### 任务 1：克隆 Databricks 存档

1. 如果当前没有打开 Azure Databricks 工作区：在 Azure 门户中，导航到已部署的 Azure Databricks 工作区并选择 **“启动工作区”**。
1. 在左窗格中，依次选择 **“工作区”** > **“用户”**，然后选择用户名（带房屋图标的条目）。
1. 在显示的窗格中，选择名称旁边的箭头，然后选择 **“导入”**。

    ![用于导入存档的菜单选项](media/import-archive.png)

1. 在 **“导入笔记本”** 对话框中，选择 URL 并粘贴以下 URL：

 ```
  https://github.com/solliancenet/microsoft-learning-paths-databricks-notebooks/blob/master/data-engineering/DBC/04-Working-With-Dataframes.dbc?raw=true
 ```

1. 选择 **“导入”**。
1. 选择显示的 **04-Working-With-Dataframes** 文件夹。

#### 任务 2：完成“描述 DataFrame”笔记本

打开 **“1.Describe-a-dataframe”** 笔记本。确保将群集附加到笔记本上，然后按照说明在笔记本中运行单元格。

在笔记本中，你将：

- 熟悉 `DataFrame` API
- 了解类 …
  - `SparkSession`
  - `DataFrame` (aka `Dataset[Row]`)
- 了解操作 …
  - `count()`

完成笔记本后，返回到此屏幕，并继续执行下一步。

### 练习 2：完成“使用 DataFrame”笔记本

在 Azure Databricks 工作区中，打开在用户文件夹中导入的 **04-Working-With-Dataframes** 文件夹。

打开 **2.Use-common-dataframe-methods** 笔记本。确保将群集附加到笔记本上，然后按照说明在笔记本中运行单元格。

在笔记本中，你将：

- 熟悉 `DataFrame` API
- 使用常见的 DataFrame 方法提高性能
- 探索 Spark API 文档

完成笔记本后，返回到此屏幕，并继续执行下一步。

### 练习 3：完成“显示功能”笔记本

在 Azure Databricks 工作区中，打开在用户文件夹中导入的 **04-Working-With-Dataframes** 文件夹。

打开 **3.Display-function** 笔记本。确保将群集附加到笔记本上，然后按照说明在笔记本中运行单元格。

在笔记本中，你将：

- 了解转换 …
  - `limit(..)`
  - `select(..)`
  - `drop(..)`
  - `distinct()`
  - `dropDuplicates(..)`
- 了解操作 …
  - `show(..)`
  - `display(..)`

完成笔记本后，返回到此屏幕，并继续执行下一步。

### 练习 4：完成“不同的文章”练习笔记本

在 Azure Databricks 工作区中，打开在用户文件夹中导入的 **04-Working-With-Dataframes** 文件夹。

打开 **4.练习： “不同的文章”** 笔记本。确保将群集附加到笔记本上，然后按照说明在笔记本中运行单元格。

在本练习中，你会读取 Parquet 文件，应用必要的转换，执行记录的总计数，然后验证所有数据是否都正确加载。另外，请尝试定义与数据匹配的架构，并更新读取操作以使用该架构。

> 备注：你将在 `Solutions` 子文件夹中找到相应的笔记本。这包含该练习已完成的单元格。如果遇到困难或想要查看解决方案，请参阅笔记本。

完成笔记本后，返回到此屏幕，并前往下一个实验室。

## 实验室 2 - 使用 DataFrame 高级方法

此实验室以在上一个实验室中学到的 Azure Databricks DataFrame 概念为基础，探索了数据工程师可以通过使用 DataFrame 读取、写入和转换数据的一些高级方法。

### 练习 2：完成实验室笔记本

#### 任务 1：克隆 Databricks 存档

1. 如果当前没有打开 Azure Databricks 工作区：在 Azure 门户中，导航到已部署的 Azure Databricks 工作区并选择 **“启动工作区”**。
1. 在左窗格中，依次选择 **“工作区”** > **“用户”**，然后选择用户名（带房屋图标的条目）。
1. 在显示的窗格中，选择名称旁边的箭头，然后选择 **“导入”**。

    ![用于导入存档的菜单选项](media/import-archive.png)

1. 在 **“导入笔记本”** 对话框中，选择 URL 并粘贴以下 URL：

 ```
  https://github.com/solliancenet/microsoft-learning-paths-databricks-notebooks/blob/master/data-engineering/DBC/07-Dataframe-Advanced-Methods.dbc?raw=true
 ```

1. 选择 **“导入”**。
1. 选择显示的 **07-Dataframe-Advanced-Methods** 文件夹。

#### 任务 2：完成“日期和时间操作”笔记本

打开 **1.DateTime-Manipulation** 笔记本。确保将群集附加到笔记本上，然后按照说明在笔记本中运行单元格。

在笔记本中，你将：

- 浏览更多 `...sql.functions` 操作
  - 日期和时间函数

完成笔记本后，返回到此屏幕，并继续执行下一步。

### 练习 3：完成“使用聚合函数”笔记本

在 Azure Databricks 工作区中，打开在用户文件夹中导入的 **07-Dataframe-Advanced-Methods** 文件夹。

打开 **2.Use-Aggregate-Functions** 笔记本。确保将群集附加到笔记本上，然后按照说明在笔记本中运行单元格。

在笔记本中，你将了解各种聚合函数。

完成笔记本后，返回到此屏幕，并继续执行下一步。

### 练习 4：完成“删除重复数据”练习笔记本

在 Azure Databricks 工作区中，打开在用户文件夹中导入的 **07-Dataframe-Advanced-Methods** 文件夹。

打开 **3.Exercise-Deduplication-of-Data** 笔记本。确保将群集附加到笔记本上，然后按照说明在笔记本中运行单元格。

本练习的目标是将所学的有关 DataFrame 的部分知识（包括重命名列）运用于实践。笔记本中提供了相关说明，并提供了空单元格供你完成工作。笔记本底部的其他单元格可帮助验证你的工作是否准确。

> 备注：你将在 `Solutions` 子文件夹中找到相应的笔记本。这包含该练习已完成的单元格。如果遇到困难或想要查看解决方案，请参阅笔记本。
