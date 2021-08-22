# 模块 12 - 使用 Azure Synapse Link 支持混合事务分析处理 (HTAP)

在此模块中，学生将了解如何使用 Azure Synapse Link 将 Azure Cosmos DB 帐户无缝连接到 Synapse 工作区。学生将了解如何启用并配置 Synapse Link，以及如何使用 Apache Spark 和 SQL 无服务器查询 Azure Cosmos DB 分析存储。

在本模块中，学生将能够：

- 使用 Azure Cosmos DB 配置 Azure Synapse Link
- 使用 Apache Spark for Synapse Analytics 查询 Azure Cosmos DB
- 使用 Azure Synapse Analytics 中的无服务器 SQL 池查询 Azure Cosmos DB

## 实验室详细信息

- [模块 12 - 使用 Azure Synapse Link 支持混合事务分析处理 (HTAP)](#module-12---support-hybrid-transactional-analytical-processing-htap-with-azure-synapse-link)
  - [实验室详细信息](#lab-details)
  - [实验室设置和先决条件](#lab-setup-and-pre-requisites)
  - [练习 1：实验室设置](#exercise-1-lab-setup)
    - [任务 1：创建链接服务](#task-1-create-linked-service)
    - [任务 2：创建数据集](#task-2-create-dataset)
  - [练习 2：使用 Azure Cosmos DB 配置 Azure Synapse Link](#exercise-2-configuring-azure-synapse-link-with-azure-cosmos-db)
    - [任务 1：启用 Azure Synapse Link](#task-1-enable-azure-synapse-link)
    - [任务 2：创建一个新的 Azure Cosmos DB 容器](#task-2-create-a-new-azure-cosmos-db-container)
    - [任务 3：创建并运行复制管道](#task-3-create-and-run-a-copy-pipeline)
  - [练习 3：使用 Apache Spark for Synapse Analytics 查询 Azure Cosmos DB](#exercise-3-querying-azure-cosmos-db-with-apache-spark-for-synapse-analytics)
    - [任务 1：创建笔记本](#task-1-create-a-notebook)
  - [练习 4：使用 Azure Synapse Analytics 中的无服务器 SQL 池查询 Azure Cosmos DB](#exercise-4-querying-azure-cosmos-db-with-serverless-sql-pool-for-azure-synapse-analytics)
    - [任务 1：创建新的 SQL 脚本](#task-1-create-a-new-sql-script)

## 实验室设置和先决条件

> **备注：** 如果**不**使用托管实验室环境，而是使用自己的 Azure 订阅，则仅完成 `Lab setup and pre-requisites` 步骤。否则，请跳转到练习 1。

**完成此模块的[实验室设置说明](https://github.com/solliancenet/microsoft-data-engineering-ilt-deploy/blob/main/setup/04/README.md)**。

请注意，以下模块使用相同的环境：

- [模块 4](labs/04/README.md)
- [模块 5](labs/05/README.md)
- [模块 7](labs/07/README.md)
- [模块 8](labs/08/README.md)
- [模块 9](labs/09/README.md)
- [模块 10](labs/10/README.md)
- [模块 11](labs/11/README.md)
- [模块 12](labs/12/README.md)
- [模块 13](labs/13/README.md)
- [模块 16](labs/16/README.md)

## 练习 1：实验室设置

### 任务 1：创建链接服务

请完成下面的步骤，创建 Azure Cosmos DB 链接服务。

> **备注**：如果你已在先前模块的此环境中创建了以下内容，可跳过此部分：
> 
> 链接服务：
> - `asacosmosdb01` (Cosmos DB)
> 
> 集成数据集：
> - `asal400_customerprofile_cosmosdb`

1. 打开 Synapse Studio (<https://web.azuresynapse.net/>)，然后导航到 **“管理”** 中心。

    ![图中突出显示了“管理”菜单项。](media/manage-hub.png "Manage hub")

2. 打开 **“链接服务”**，然后选择 **“+ 新建”** 以创建新的链接服务。在选项列表中选择 **“Azure Cosmos DB (SQL API)”**，然后选择 **“继续”**。

    ![图中突出显示了“管理”、“新建”和“Azure Cosmos DB 链接服务”选项。](media/create-cosmos-db-linked-service-step1.png "New linked service")

3. 将链接服务命名为 `asacosmosdb01` **(1)**，选择 **Cosmos DB 帐户名称** (`asacosmosdbSUFFIX`)，然后将 **“数据库名称”** 值设置为 `CustomerProfile` **(2)**。选择 **“测试连接”** 以确保成功 **(3)**，然后选择 **“创建”(4)**。

    ![新建 Azure Cosmos DB 链接服务。](media/create-cosmos-db-linked-service.png "New linked service")

### 任务 2：创建数据集

完成以下步骤以创建 `asal400_customerprofile_cosmosdb` 数据集。

> **演示者注意事项**：如果你已完成模块 4，可跳过此部分。

1. 导航到 **“数据”** 中心。

    ![图中突出显示了“数据”菜单项。](media/data-hub.png "Data hub")

2. 在工具栏中选择 **“+”** **(1)**，然后选择 **“集成数据集”(2)** 以创建新数据集。

    ![创建新的数据集。](media/new-dataset.png "New Dataset")

3. 在列表中选择 **“Azure Cosmos DB (SQL API)”** **(1)**，然后选择 **“继续”(2)**。

    ![图中突出显示了“Azure Cosmos DB SQL API”选项。](media/new-cosmos-db-dataset.png "Integration dataset")

4. 使用以下特征配置数据集，然后选择 **“确定”(4)**：

    - **名称**：输入 `asal400_customerprofile_cosmosdb` **(1)**。
    - **链接服务**：选择 Azure Cosmos DB 链接服务 **(2)**。
    - **集合**：选择 `OnlineUserProfile01` **(3)**。

    ![新建 Azure Cosmos DB 数据集。](media/create-cosmos-db-dataset.png "New Cosmos DB dataset")

5. 创建数据集后，选择 **“连接”** 选项卡下的 **“预览数据”**。

    ![图中突出显示了数据集上的“预览数据”按钮。](media/cosmos-dataset-preview-data-link.png "Preview data")

6. 预览数据查询选中的 Azure Cosmos DB 集合，并返回其中的文档示例。文档以 JSON 格式存储，并包含 `userId` 字段、`cartId`、`preferredProducts`（一些可能为空产品 ID）和 `productReviews`（编写的一些产品评论，可能为空）。

    ![图中显示了 Azure Cosmos DB 数据的预览。](media/cosmos-db-dataset-preview-data.png "Preview data")

7. 依次选择 **“全部发布”** 和 **“发布”**，以保存新资源。

    ![图中突出显示了“全部发布”。](media/publish-all-1.png "Publish all")

## 练习 2：使用 Azure Cosmos DB 配置 Azure Synapse Link

Tailwind Traders 使用 Azure Cosmos DB 存储来自其电子商务网站中的用户资料数据。可借助 Azure Cosmos DB SQL API 提供的 NoSQL 文档来熟悉如何使用 SQL 语法管理这些数据，同时大规模地全局读取和写入文件。

虽然 Tailwind Traders 对 Azure Cosmos DB 的功能和性能很满意，但他们想知道在数据仓库中的多个分区上执行大量分析查询（跨分区查询）会产生多少费用。他们希望能在不增加 Azure Cosmos DB 请求单位 (RU) 的前提下高效访问所有数据。他们已考虑过一些方案，可在 Data Lake 发生变化时通过 Azure Cosmos DB 更改源机制将容器中的数据提取到数据库。此方法的问题是会产生额外的服务和代码依赖项以及需要对解决方案进行长期维护。可以从 Synapse Pipeline 执行批量导出，但其中并不包含任何给定时刻的最新信息。

你决定为 Cosmos DB 启用 Azure Synapse Link，并在 Azure Cosmos DB 容器上启用分析存储。使用此配置，所有事务数据会自动存储在完全隔离的列存储中。此存储可实现针对 Azure Cosmos DB 中操作数据的大规模分析，且不会影响事务工作负载，也不会产生资源单元 (RU) 成本。适用于 Cosmos DB 的 Azure Synapse Link 在 Azure Cosmos DB 与 Azure Synapse Analytics 之间建立了紧密的集成，使 Tailwind Traders 能对其操作数据运行准实时分析，同时无需 ETL 过程，且完全不影响事务工作负载的性能。

通过将 Cosmos DB 的事务处理的分布式缩放与 Azure Synapse Analytics 内置的分析存储和计算能力相结合，Azure Synapse Link 可实现混合事务/分析处理 (HTAP) 体系结构，从而优化 Tailwind Trader 的业务流程。此集成消除了 ETL 过程，使业务分析师、数据工程师和数据科学家能使用自助功能，并对操作数据运行准实时 BI、分析和机器学习管道。

### 任务 1：启用 Azure Synapse Link

1. 导航到 Azure 门户 (<https://portal.azure.com>) 并打开用于实验室环境的资源组。

2. 选择 **“Azure Cosmos DB 帐户”**。

    ![Azure Cosmos DB 帐户已突出显示。](media/resource-group-cosmos.png "Azure Cosmos DB account")

3. 选择左侧菜单中的 **“功能”** **(1)**，然后选择 **“Azure Synapse Link”(2)**。

    ![将显示“功能”边栏选项卡。](media/cosmos-db-features.png "Features")

4. 选择 **“启用”**。

    ![“启用”已突出显示。](media/synapse-link-enable.png "Azure Synapse Link")

    在使用分析存储创建 Azure Cosmos DB 容器之前，必须先启用 Azure Synapse Link。

5. 必须等待此操作完成才能继续，这可能需要一分钟时间。可选择 Azure **“通知”** 图标查看状态。

    ![“启用 Synapse Link”进程正在运行。](media/notifications-running.png "Notifications")

    “启用 Synapse Link”成功完成后，其旁边会显示一个绿色对勾符号。

    ![操作成功完成。](media/notifications-completed.png "Notifications")

### 任务 2：创建一个新的 Azure Cosmos DB 容器

Tailwind Traders 有一个名为 `OnlineUserProfile01` 的 Azure Cosmos DB 容器。由于我们在创建该容器后启用了 Azure Synapse Link 功能，因此无法在该容器上启用分析存储__。我们会创建一个新容器，使其具有相同的分区键并启用分析存储。

创建容器之后，我们将创建一个新的 Synapse Pipeline，以将数据从 `OnlineUserProfile01` 容器复制到另一个容器。

1. 在左侧菜单中，选择 **“数据资源管理器”**。

    ![已选择菜单项。](media/data-explorer-link.png "Data Explorer")

2. 选择 **“新建容器”**。

    ![图中突出显示了按钮。](media/new-container-button.png "New Container")

3. 对于 **“数据库 ID”**，请选择 **“使用现有项”**，然后选择 **`CustomerProfile` (1)**。在 **“容器 ID” (2)** 中输入 **`UserProfileHTAP`**，然后在 **“分区键” (3)** 中输入 **`/userId`**。对于 **“吞吐量”**，请选择 **“自动缩放” (4)**，然后输入 **`4000`** 作为 **“最大 RU/s”** 值。最后，展开 **“高级”** 并将 **“分析存储”** 设为 **“开启” (6)**，然后选择 **“确定”**。

    ![已按照描述配置表单。](media/new-container.png "New container")

    这里我们将 `partition key` 值设置为 `customerId`，因为该字段在查询中使用最频繁并且包含相对较高的基数（唯一值的个数）以实现良好的分区性能。我们将吞吐量设置为自动缩放，且最大值为 4,000 个请求单位 (RU)。这意味着，该容器将至少通过分配获得 400 个 RU（最大值的 10%），并且会在缩放引擎检测到足够高的需求时纵向扩展至最大值 4,000 以保证增加吞吐量。最后，我们会在容器上启用**分析存储**，以充分利用 Synapse Analytics 中的混合事务/分析处理 (HTAP) 体系结构。

    让我们快速查看一下要复制到新容器中的数据。

4. 展开 **CustomerProfile** 数据库下的 `OnlineUserProfile01` 容器，然后选择 **“项”(1)**。选择其中一个文档 **(2)** 并查看其内容 **(3)**。文档以 JSON 格式存储。

    ![将显示容器项。](media/existing-items.png "Container items")

5. 在左侧菜单中选择 **“键”** **(1)**，然后复制 **“分区键”** 值 **(2)** 并将其保存到笔记本或类似的文本编辑器中供以后参考。复制左上角的 Azure Cosmos DB **帐户名** **(3)**，并且也将其保存到笔记本或类似的文本编辑器中供以后使用。

    ![主建已突出显示。](media/cosmos-keys.png "Keys")

    > **备注**：记下这些值。在此演示的最后阶段创建 SQL 视图时将需要此信息。

### 任务 3：创建并运行复制管道

拥有新的 Azure Cosmos DB 容器并启用分析存储后，接下来需要使用 Synapse Pipeline 复制现有容器的内容。

1. 打开 Synapse Studio (<https://web.azuresynapse.net/>)，然后导航到 **“集成”** 中心。

    ![“集成”菜单项已突出显示。](media/integrate-hub.png "Integrate hub")

2. 选择 **“+”(1)**， 然后选择 **“管道”(2)**。

    ![“新建管道”链接已突出显示。](media/new-pipeline.png "New pipeline")

3. 在 “活动”下，展开 `Move & transform` 组，然后将 **“复制数据”** 活动拖到画布上 **(1)**。在“属性”边栏选项卡 **(2)** 中，将 **“名称”** 设置为 **`Copy Cosmos DB Container`**。

    ![此时会显示新的复制活动。](media/add-copy-pipeline.png "Add copy activity")

4. 选择添加到画布的新复制活动，然后选择 **“源”** 选项卡 **(1)**。从列表中选择 **`asal400_customerprofile_cosmosdb`** 源数据集 **(2)**。

    ![选择源。](media/copy-source.png "Source")

5. 选择 **“接收器”** 选项卡 **(1)**，然后选择 **“+ 新建”(2)**。

    ![选择接收器。](media/copy-sink.png "Sink")

6. 选择 **“Azure Cosmos DB (SQL API)”** 数据库类型 **(1)**，然后选择 **“继续”(2)**。

    ![已选择 Azure Cosmos DB。](media/dataset-type.png "New dataset")

7. 对于 **“名称”**，请输入 **`cosmos_db_htap` (1)**。选择 **`asacosmosdb01` (2)**、 **“链接服务”**。选择 **`UserProfileHTAP` (3)**、 **“集合”**。在 **“导入架构”(4)** 下选择 **“从连接/存储”**，然后选择 **“确定”(5)**。

    ![已按照描述配置表单。](media/dataset-properties.png "Set properties")

8. 从刚添加的新接收器数据集下，选择 **“插入”** 写入行为。

    ![显示“接收器”选项卡。](media/sink-insert.png "Sink tab")

9. 选择 **“全部发布”** 和 **“发布”**，以保存新管道。

    ![全部发布。](media/publish-all-1.png "Publish")

10. 在管道画布上方，依次选择 **“添加触发器”(1)**、 **“立即触发”(2)**。选择 **“确定”** 以触发运行。

    ![显示触发器菜单。](media/pipeline-trigger.png "Trigger now")

11. 导航到 **“监视”** 中心。

    ![“监视”中心。](media/monitor-hub.png "Monitor hub")

12. 选择 **“管道运行”(1)** 并等待管道运行成功完成 **(2)**。可能需要多次选择 **“刷新”(3)**。

    ![管道运行显示为成功完成。](media/pipeline-run-status.png "Pipeline runs")

    > 此操作可能需要**大约 4 分钟**才能完成。在此过程中，请阅读其余的实验室说明，以熟悉内容。

## 练习 3：使用 Apache Spark for Synapse Analytics 查询 Azure Cosmos DB

Tailwind Traders 希望使用 Apache Spark 对新的 Azure Cosmos DB 容器运行分析查询。在这一段中，我们将使用 Synapse Studio 的内置笔势快速创建一个 Synapse 笔记本，用于从启用了 HTAP 的容器中加载分析存储数据，同时不影响事务存储。

Tailwind Traders 正在尝试解决如何使用每个用户标识的首选产品列表，以及在他们的评论历史记录中任何匹配的产品 ID，以显示所有首选产品评论的列表。

### 任务 1：创建笔记本

1. 导航到 **“数据”** 中心。

    ![“数据”中心。](media/data-hub.png "Data hub")

2. 选择 **“链接”** 选项卡 **(1)**，展开 **“Azure Cosmos Db”** 部分，然后选择 **asacosmosdb01 (CustomerProfile)** 链接服务 **(2)**。 右键单击 **UserProfileHTAP** 容器 **(3)**，选择 **“新建笔记本”** 笔势 **(4)**，然后选择 **“加载到 DataFrame”(5)**。

    ![“新建笔记本”笔势已突出显示。](media/new-notebook.png "New notebook")

    请注意，我们创建的 `UserProfileHTAP` 容器的图标与其他容器的略有不同。这表示已启用分析存储。

3. 在新的笔记本中，从 **“附加到”** 下拉列表中选择 Spark 池。

    ![“附加到”下拉列表已突出显示。](media/notebook-attach.png "Attach the Spark pool")

4. 选择 **“全部运行”(1)**。

    ![显示新的笔记本，其中包括单元格 1 输出。](media/notebook-cell1.png "Cell 1")

    > Spark 会话首次启动时需要几分钟时间。

    请注意，在“单元格 1”的生成代码中，`spark.read` 格式已设置为 **`cosmos.olap` (2)**。这表示 Synapse Link 将使用容器的分析存储。如果希望连接到事务存储，以便读取更改源中的数据或写入容器，则可改为使用 `cosmos.oltp`。

    > **备注：** 不能写入分析存储，而只能从分析存储中读取数据。如果希望将数据加载到容器，则需要连接到事务存储。

    第一个 `option` 配置 Azure Cosmos DB 链接服务的名称 **(3)**。第二个 `option` 定义要从中读取数据的 Azure Cosmos DB 容器 **(4)**。

5. 选择所执行的单元格下方的 **“+”** 按钮，然后选择 **“</> 代码单元格”**。此操作将在第一个代码单元格下方添加新的代码单元格。

    ![“添加代码”按钮已突出显示。](media/add-code.png "Add code")

6. DataFrame 包含我们不需要的额外列。让我们删除不需要的列，创建一个干净的 DataFrame 版本。若要实现这一点，请在新单元格中输入以下内容并**运行**它：

    ```python
    unwanted_cols = {'_attachments','_etag','_rid','_self','_ts','collectionType','id'}

    # 请从列集合中删除不需要的列
    cols = list(set(df.columns) - unwanted_cols)

    profiles = df.select(cols)

    display(profiles.limit(10))
    ```

    输出现在只包含所需的列。请注意，`preferredProducts` **(1)** 和 `productReviews` **2** 列包含子元素。 展开某一行的值以查看它们。你可能在 Azure Cosmos DB 数据资源管理器中看到过 `UserProfiles01` 容器中的原始 JSON 格式。

    ![显示了单元格输出。](media/cell2.png "Cell 2 output")

7. 我们应了解需要处理多少条记录。若要实现这一点，请在新单元格中输入以下内容并**运行**它：

    ```python
    profiles.count()
    ```

    应会看到计数结果为 100,000。

8. 我们希望对每个用户都使用 `preferredProducts` 和 `productReviews` 列数组，并创建一个产品图表，这些产品来自他们的首选列表，与他们评论过的产品相匹配。为此，我们需要创建两个新的 DataFrame，使其包含这两列中的平展值，以便稍后我们可以联接它们。在新单元格中输入以下内容并 **“运行”** 它：

    ```python
    from pyspark.sql.functions import udf, explode

    preferredProductsFlat=profiles.select('userId',explode('preferredProducts').alias('productId'))
    productReviewsFlat=profiles.select('userId',explode('productReviews').alias('productReviews'))
    display(productReviewsFlat.limit(10))
    ```

    在此单元格中，我们导入了一个特殊的 [`explode` 函数](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=explode#pyspark.sql.functions.explode)，它为数组的每个元素返回一个新行。此函数有助于平展 `preferredProducts` 和 `productReviews` 列，以提高可读性或方便查询。

    ![单元格输出。](media/cell4.png "Cell 4 output")

    观察单元格输出，其中显示了 `productReviewFlat` DataFrame 内容。我们会看到一个新的 `productReviews` 列，该列包含我们希望与用户的首选产品列表匹配的 `productId`，以及我们希望显示或保存的 `reviewText`。

9. 我们来看看 `preferredProductsFlat` DataFrame 内容。若要实现这一点，请在新单元格中输入以下内容并**运行**它：

    ```python
    display(preferredProductsFlat.limit(20))
    ```

    ![单元格输出。](media/cell5.png "Cell 5 results")

    由于我们在首选产品数组上使用了 `explode` 函数，我们已将列值平展到 `userId` 和 `productId` 行，并按用户排序。

10. 现在，我们需要进一步平展 `productReviewFlat` DataFrame 内容，以提取 `productReviews.productId` 和 `productReviews.reviewText` 字段并为每个数据组合创建新行。若要实现这一点，请在新单元格中输入以下内容并**运行**它：

    ```python
    productReviews = (productReviewsFlat.select('userId','productReviews.productId','productReviews.reviewText')
        .orderBy('userId'))

    display(productReviews.limit(10))
    ```

    在输出中，请注意现在每个 `userId` 都有多行。

    ![单元格输出。](media/cell6.png "Cell 6 results")

11. 最后一步是联接 `userId` 和 `productId` 值上的 `preferredProductsFlat` 和 `productReviews` DataFrame，以创建首选产品评论图表。若要实现这一点，请在新单元格中输入以下内容并**运行**它：

    ```python
    preferredProductReviews = (preferredProductsFlat.join(productReviews,
        (preferredProductsFlat.userId == productReviews.userId) &
        (preferredProductsFlat.productId == productReviews.productId))
    )

    display(preferredProductReviews.limit(100))
    ```

    > **备注**：可随意单击表视图中的列标题来对结果集进行排序。

    ![单元格输出。](media/cell7.png "Cell 7 results")

## 练习 4：使用 Azure Synapse Analytics 中的无服务器 SQL 池查询 Azure Cosmos DB

Tailwind Traders 希望使用 T-SQL 浏览 Azure Cosmos DB 分析存储。理想情况下，他们可以创建多个视图，这些视图随后可用于联接其他分析存储容器、Data Lake 中的文件或可被 Power BI 等外部工具使用。

### 任务 1：创建新的 SQL 脚本

1. 导航到 **“开发”** 中心。

    ![“开发”中心。](media/develop-hub.png "Develop hub")

2. 选择 **“+”(1)**，然后选择 **“SQL 脚本”(2)**。

    ![突出显示了“SQL 脚本”按钮。](media/new-script.png "SQL script")

3. 脚本打开时，右侧将显示“ **属性”** 窗格 **(1)**。在 **“名称”** 中输入 **`User Profile HTAP`** **(2)**，然后选择 **“属性”** 按钮关闭该窗格 **(1)**。

    ![显示“属性”窗格。](media/new-script-properties.png "Properties")

4. 验证是否选择了无服务器 SQL 池（**内置**）。

    ![无服务器 SQL 池已选中。](media/built-in-htap.png "Built-in")

5. 粘贴下面的 SQL 查询。在 OPENROWSET 语句中，将 **`YOUR_ACCOUNT_NAME`** 替换为 Azure Cosmos DB 帐户名，并将 **`YOUR_ACCOUNT_KEY`** 替换为你在前面步骤 5 中创建容器之后复制的 Azure Cosmos DB 主密钥值。

    ```sql
    USE master
    GO

    IF DB_ID (N'Profiles') IS NULL
    BEGIN
        CREATE DATABASE Profiles;
    END
    GO

    USE Profiles
    GO

    DROP VIEW IF EXISTS UserProfileHTAP;
    GO

    CREATE VIEW UserProfileHTAP
    AS
    SELECT
        *
    FROM OPENROWSET(
        'CosmosDB',
        N'account=YOUR_ACCOUNT_NAME;database=CustomerProfile;key=YOUR_ACCOUNT_KEY',
        UserProfileHTAP
    )
    WITH (
        userId bigint,
        cartId varchar(50),
        preferredProducts varchar(max),
        productReviews varchar(max)
    ) AS profiles
    CROSS APPLY OPENJSON (productReviews)
    WITH (
        productId bigint,
        reviewText varchar(1000)
    ) AS reviews
    GO
    ```

    已完成的查询应如下所示：

    ![显示查询的“创建视图”部分和结果。](media/htap-view.png "SQL query")

    查询首先创建一个新的无服务器 SQL 池数据库 `Profiles`（如果不存在），然后执行 `USE Profiles` 以对 `Profiles` 数据库运行脚本内容的其余部分。接下来，它会删除 `UserProfileHTAP` 视图（如果存在）。最后，它会执行以下操作：

    - **1.** 创建名为 `UserProfileHTAP` 的 SQL 视图。
    - **2.** 使用 `OPENROWSET` 语句将数据源类型设置为 `CosmosDB`，设置帐户详细信息并指出我们希望对名为 `UserProfileHTAP` 的 Azure Cosmos DB 分析存储容器创建视图。
    - **3.** `WITH` 子句匹配 JSON 文件中的属性名并应用合适的 SQL 数据类型。请注意，我们已将 `preferredProducts` 和 `productReviews` 字段设置为 `varchar(max)`。这是因为这两个属性都包含 JSON 格式的数据。
    - **4.** 由于 JSON 文档中的 `productReviews` 属性包含嵌套子数组，因此我们希望将文档中的属性与数组的所有元素联接起来。借助 Synapse SQL，我们可以对嵌套数组应用 `OPENJSON` 函数，从而平展嵌套结构。平展 `productReviews` 中的值，就像我们在前面的 Synapse 笔记本中使用 Python `explode` 函数执行的操作一样。
    - **5.** 该输出显示已成功执行语句。

6. 导航到 **“数据”** 中心。

    ![“数据”中心。](media/data-hub.png "Data hub")

7. 选择 **“工作区”** 选项卡 **(1)** 并展开“数据库”组。展开 **“属性”** 按需 SQL 数据库 **(2)**。如果列表中未显示此内容，请刷新数据库列表。展开“视图”，然后右键单击 **`UserProfileHTAP`** 视图 **(3)**。选择 **“新建 SQL 脚本”(4)**，然后 **“选择前 100 行”(5)**。

    ![“选择前 100 行”查询选项已突出显示。](media/new-select-query.png "New select query")

8. **运行**查询并记下结果。

    ![视图结果已显示。](media/select-htap-view.png "Select HTAP view")

    查询中包括了 `preferredProducts` **(1)** 和 `productReviews` **(2)** 字段，这两个字段都包含 JSON 格式的值。注意视图中的 CROSS APPLY OPENJSON 语句如何通过将 `productId` 和 `reviewText` 值提取到新字段，成功平展 `productReviews` 字段 **(2)** 中的嵌套子数组值。
