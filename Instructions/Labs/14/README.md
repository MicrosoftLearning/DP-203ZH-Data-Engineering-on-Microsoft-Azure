# 模块 14 - 使用流分析进行实时流处理

在本模块中，学生将学习如何使用 Azure 流分析来处理流式数据。学生将车辆遥测数据引入事件中心，然后使用 Azure 流分析中的各种开窗函数实时处理该数据。他们会将数据输出到 Azure Synapse Analytics。最后，学生将学习如何扩展流分析作业以提高吞吐量。

在本模块中，学生将能够：

- 使用流分析来处理事件中心内的实时数据
- 使用流分析开窗函数来构建聚合并输出到 Synapse Analytics
- 扩展 Azure 流分析作业以通过分区提高吞吐量
- 重新分区流输入以优化并行

## 实验室详细信息

- [模块 14 - 使用流分析进行实时流处理](#module-14---real-time-stream-processing-with-stream-analytics)
  - [实验室详细信息](#lab-details)
  - [技术概述](#technology-overview)
    - [Azure 流分析](#azure-stream-analytics)
    - [Azure 事件中心](#azure-event-hubs)
    - [Power BI](#power-bi)
  - [方案概述](#scenario-overview)
  - [实验室设置和先决条件](#lab-setup-and-pre-requisites)
  - [练习 0：启动专用 SQL 池](#exercise-0-start-the-dedicated-sql-pool)
  - [练习 1：配置服务](#exercise-1-configure-services)
    - [任务 1：配置事件中心](#task-1-configure-event-hubs)
    - [任务 2：Azure Synapse Analytics](#task-2-configure-synapse-analytics)
    - [任务 3：配置流分析](#task-3-configure-stream-analytics)
  - [练习 2：生成和直观呈现数据](#exercise-2-generate-and-visualize-data)
    - [任务 1：运行数据生成器](#task-1-run-data-generator)
    - [任务 2：创建 Power BI 仪表板](#task-2-create-power-bi-dashboard)
    - [任务 3：在 Synapse Analytics 中查看聚合数据](#task-3-view-aggregate-data-in-synapse-analytics)
  - [练习 3：清理](#exercise-3-cleanup)
    - [任务 1：停止数据生成器](#task-1-stop-the-data-generator)
    - [任务 2：停止流分析作业](#task-2-stop-the-stream-analytics-job)
    - [任务 3：暂停专用 SQL 池](#task-3-pause-the-dedicated-sql-pool)

## 技术概述

### Azure 流分析

随着越来越多的数据从各种连接的设备和传感器中生成，将这些数据以准实时的方式转化为可执行的见解和预测现已成为一项必备操作。[Azure 流分析](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-introduction)与实时应用程序体系结构无缝集成，无论数据量如何，都可对数据进行功能强大的实时分析。

借助 Azure 流分析，你可以轻松开发大规模并行复杂事件处理 (CEP) 管道。它使你可以利用 [SQL 之类](https://docs.microsoft.com/stream-analytics-query/stream-analytics-query-language-reference)的非常简单的声明式语言以及对时态逻辑的嵌入式支持来创作功能强大的实时分析解决方案。大量[开箱即用的连接器](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-define-outputs)、高级调试和作业监控功能通过显著减少开发人员所需的技能来帮助降低成本。此外，Azure 流分析具有高度可扩展性，通过对自定义代码的支持以及 [JavaScript 用户定义函数](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-javascript-user-defined-functions)，进一步扩展了用 SQL 编写的流逻辑。

使用 Azure 流分析，几秒钟就可以轻松入门，因为无需担心基础结构，也无需管理服务器、虚拟机或群集。对于任何作业，你都可以立即将[处理能力](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-streaming-unit-consumption)从一个流单元扩展到数百个流单元。你只需为每个作业使用的处理资源付费。

[有保证的事件交付](https://docs.microsoft.com/stream-analytics-query/event-delivery-guarantees-azure-stream-analytics)和企业级 SLA，提供 99.9% 的可用性，确保 Azure 流分析适用于任务关键型工作负载。自动检查点支持容错操作，以及快速重启且不会丢失数据。

借助 Azure 流分析，你可以使用 Power BI 快速构建实时仪表板，以获得实时命令和控制视图。[实时仪表板](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-power-bi-dashboard)有助于将实时数据转换为可操作且富有洞察力的视觉对象，并帮助你专注于你认为最重要的内容。

### Azure 事件中心

[Azure 事件中心](https://docs.microsoft.com/azure/event-hubs/event-hubs-about)是每秒可引入数百万个事件的大数据管道。它使用 HTTPS、AMQP、AMQP over websockets 和 Kafka 等标准协议促进遥测和事件流数据的捕获、保留和重放。数据可以来自多个并发源，并且多达 20 个使用者组允许应用程序按照自己的节奏独立读取整个事件中心。

### Power BI

[Power BI](https://docs.microsoft.com/power-bi/) 是一种商业分析服务，可提供见解以实现快速、明智的决策。它使你能够将数据转换为令人惊叹的视觉对象，并在任何设备上与同事共享。Power BI 提供了功能丰富的画布，可以在其上直观地[浏览和分析数据](https://docs.microsoft.com/power-bi/service-basic-concepts)。在体验过程中还可以针对自定义[仪表板](https://docs.microsoft.com/azure/stream-analytics/stream-analytics-power-bi-dashboard)和交互式报表进行协作和共享，使你能够利用内置的治理功能和安全性在整个组织中进行缩放。

## 方案概述

Contoso Auto 正在收集车辆遥测数据，并希望使用事件中心快速引入和存储原始形式的数据，然后进行一些准实时的处理。最后，他们希望创建一个仪表板，当新数据在经过处理后流入时，该仪表板会自动更新。他们希望在仪表板上看到检测到的异常（例如引擎过热、油压异常和激进驾驶）的各种可视化效果，使用地图等组件来显示与城市相关的异常，以及使用各种图表和图形来清晰地描述这些信息。

在此体验中，你将使用 Azure 事件中心来引入流式车辆遥测数据，作为基于事件中心、Azure 流分析和 Power BI 构建的准实时分析管道的入口点。Azure 流分析从事件中心提取车辆传感器数据，在一段时间内执行聚合，然后将聚合数据发送到 Azure Synapse Analytics 和 Power BI 以进行数据可视化和分析。车辆遥测数据生成器将用于向事件中心发送车辆遥测数据。

## 实验室设置和先决条件

> **备注：** 如果**不**使用托管实验室环境，而是使用自己的 Azure 订阅，则仅完成`Lab setup and pre-requisites`步骤。否则，请跳转到练习 0。

- Azure 订阅
- Power BI 帐户（请在此处注册：<https://powerbi.microsoft.com>）
- [实验室环境设置](https://github.com/solliancenet/microsoft-data-engineering-ilt-deploy/tree/main/setup/14)

## 练习 0：启动专用 SQL 池

本实验室使用专用 SQL 池。第一步是确保它没有暂停。如果暂停，请按照以下说明启动它：

1. 打开 Synapse Studio (<https://web.azuresynapse.net/>)。

2. 选择 **“管理”** 中心。

    ![图中突出显示了“管理”中心。](media/manage-hub.png "Manage hub")

3. 在左侧菜单中，选择 **“SQL 池” (1)**。如果专用 SQL 池已暂停，请将鼠标悬停在池名称上并选择 **“恢复” (2)**。

    ![图中突出显示了专用 SQL 池上的“恢复”按钮。](media/resume-dedicated-sql-pool.png "Resume")

4. 出现提示时，选择 **“恢复”**。恢复池可能需要一到两分钟。

    ![图中突出显示了“恢复”按钮。](media/resume-dedicated-sql-pool-confirm.png "Resume")

> 恢复专用 SQL 池后， **请继续下一个练习**。

## 练习 1：配置服务

### 任务 1：配置事件中心

Azure 事件中心是一种大数据流式处理平台和事件引入服务，每秒能够接收和处理数百万个事件。我们正在使用它来临时存储已经过处理并准备发送到实时仪表板的车辆遥测数据。数据流入事件中心后，Azure 流分析将查询数据、应用聚合和标记异常，然后将其发送到 Azure Synapse Analytics 和 Power BI。

在此任务中，你将在提供的事件中心命名空间内创建并配置新的事件中心。在你稍后创建的 Azure 函数对车辆遥测数据进行处理和丰富后，将使用该事件中心来捕获车辆遥测数据。

1. 导航到 [Azure 门户](https://portal.azure.com)。

2. 在左侧菜单中选择 **“资源组”**。然后，选择名为 **“ms-dataengineering-14”** 的资源组。

3. 从资源组中的资源列表中选择 **“事件中心命名空间”** (`eventhubYOUR_UNIQUE_ID`)。

    ![资源组中已选择“事件中心命名空间”。](media/rg-event-hubs.png "resource group")

4. 在“事件中心命名空间”边栏选项卡上，选择左侧菜单中的 **“事件中心”**。

    ![左侧菜单中已选择“事件中心”链接。](media/event-hubs-link.png 'Event Hubs link')

5. 从列表中选择 **“遥测”** 事件中心。

    ![已选择新创建的遥测事件中心。](media/event-hubs-select.png 'Event hubs')

6. 从左侧菜单中选择 **“共享访问策略”**。

    ![左侧菜单中已选择“共享访问策略”链接。](media/event-hubs-shared-access-policies-link.png 'Shared access policies link')

7. 在顶部工具栏中选择 **“+ 添加”**，创建新的共享访问策略。

    ![“添加”按钮已突出显示。](media/event-hubs-shared-access-policies-add-link.png 'Add')

8. 在 **“添加 SAS 策略”** 边栏选项卡中，进行以下配置：

    - **名称：** 输入“Read”。
    - **托管：** 未选中。
    - **发送：** 未选中。
    - **侦听：** 已选中。

    ![“添加 SAS 策略”表单已使用前面提到的设置填写到相应字段中。](media/event-hubs-add-sas-policy-read.png 'Add SAS Policy')

    > 最佳做法是针对读取、写入和管理事件创建单独的策略。这遵循最低权限原则，以防止服务和应用程序执行未经授权的操作。

9. 输入完值后，选择表单底部的 **“创建”**。

10. 在顶部工具栏中选择 **“+ 添加”**，创建新的共享访问策略。

    ![“添加”按钮已突出显示。](media/event-hubs-shared-access-policies-add-link.png 'Add')

11. 在 **“添加 SAS 策略”** 边栏选项卡中，进行以下配置：

    - **名称：** 输入“Write”。
    - **托管：** 未选中。
    - **发送：** 已选中。
    - **侦听：** 未选中。

    ![“添加 SAS 策略”表单已使用前面提到的设置填写到相应字段中。](media/event-hubs-add-sas-policy-write.png 'Add SAS Policy')

12. 输入完值后，选择表单底部的 **“创建”**。

13. 从列表中选择 **“写入”** 策略。通过选择字段右侧的“复制”按钮，复制 **“连接字符串 - 主密钥”** 值。在记事本或类似的文本编辑器中**保存此值**供以后使用。

    ![选择了“写入策略”，并显示了其边栏选项卡。“连接字符串 - 主密钥”字段旁边的“复制”按钮已突出显示。](media/event-hubs-write-policy-key.png 'SAS Policy: Write')

### 任务 2：Azure Synapse Analytics

Azure Synapse 是一个端到端的分析平台，它在一个单一的集成环境中提供 SQL 数据仓库、大数据分析和数据集成功能。它使用户能够快速访问他们的所有数据并获得见解，使性能和规模达到一个全新的水平，这在业界简直是无与伦比的。

在此任务中，你将在 Synapse 专用 SQL 池中创建一个表，以存储由流分析作业提供的聚合车辆数据，该作业处理事件中心引入的车辆遥测数据。

1. 导航到 [Azure 门户](https://portal.azure.com)。

2. 在左侧菜单中选择 **“资源组”**。然后，选择名为 **“ms-dataengineering-14”** 的资源组。

3. 从资源组中的资源列表中选择 **“Synapse 工作区”** (`asaworkspaceYOUR_UNIQUE_ID`)。

    ![资源组中已选择“Synapse 工作区”。](media/rg-synapse-workspace.png "resource group")

4. 在 **“概述”** 窗格中的 **“打开 Synapse Studio”** 框中选择 **“打开”**。

    ![“打开”链接已突出显示。](media/open-synapse-studio.png "Open Synapse Studio")

5. 在 Synapse Studio 中，选择左侧菜单中的 **“数据”** 以导航到“数据中心”。

    ![图中突出显示了“数据”中心。](media/data-hub.png "Data hub")

6. 选择 **“工作区”** 选项卡 **(1)**，展开“数据库”并右键单击 **“ContosoAuto”(2)**。选择 **“新建 SQL 脚本”(3)**，然后选择 **“空脚本”(4)**。

    ![“新建 SQL 脚本”选项已在 ContosoAuto 上下文菜单中突出显示。](media/synapse-new-script.png "New SQL script")

7. 确保该脚本已连接到 `ContosoAuto`，然后将该脚本替换为以下内容并选择 **“运行”** 以创建新表：

    ```sql
    CREATE TABLE dbo.VehicleAverages
    ( 
        [AverageEngineTemperature] [float] NOT  NULL,
        [AverageSpeed] [float] NOT  NULL,
        [Snapshot] [datetime] NOT  NULL
    )
    WITH
    (
        DISTRIBUTION = ROUND_ROBIN,
        CLUSTERED COLUMNSTORE INDEX
    )
    GO
    ```

    ![显示了脚本。](media/synapse-new-table-script.png "New table script")

### 任务 3：配置流分析

Azure 流分析是一个事件处理引擎，你可以使用它来检查从设备流式传输的大量数据。传入数据可以来自设备、传感器、网站、社交媒体订阅源、应用程序等。它还支持从数据流中提取信息，识别模式和关系。然后，你可以使用这些模式触发下游其他操作，例如创建警报、将信息提供给报告工具，或将其存储以供以后使用。

在此任务中，你将配置流分析，以使用你创建的事件中心作为源，查询和分析该数据，然后将其发送到 Power BI，以向 Azure Synapse Analytics 报告聚合数据。

1. 导航到 [Azure 门户](https://portal.azure.com)。

2. 在左侧菜单中选择 **“资源组”**。然后，选择名为 **“ms-dataengineering-14”** 的资源组。

3. 从资源组中的资源列表中选择 **“流分析作业”** (`asaYOUR_UNIQUE_ID`)。

    ![资源组中已选择“流分析作业”。](media/rg-stream-analytics.png "resource group")

4. 在“流分析作业”中，选择左侧菜单中的 **“存储帐户设置”**，然后选择 **“添加存储帐户”**。由于我们将使用 Synapse Analytics 作为输出之一，因此需要首先配置作业存储帐户。

    ![“存储帐户设置”链接和“添加存储帐户”按钮已突出显示。](media/asa-storage-account.png "Storage account settings")

5. 在 **“存储帐户设置”** 表单中，进行以下配置：

   - **从订阅选择存储帐户：** 已选中。
   - **订阅：** 确保选择了用于本实验室的订阅。
   - **存储帐户：** 选择名为 **“asadatalakeYOUR_UNIQUE_ID”** 的存储帐户。
   - **身份验证模式：** 选择“连接字符串”。

   ![已按照描述配置表单。](media/asa-storage-account-form.png "Storage account settings")

6. 选择 **“保存”**，然后在提示保存存储帐户设置时选择 **“是”**。

7. 在“流分析作业”中，选择左侧菜单中的 **“输入”**。

    ![左侧菜单中已选择“输入”链接。](media/inputs-link.png 'Inputs link')

8. 在顶部工具栏中选择 **“+ 添加流输入”**，然后选择 **“事件中心”** 以创建新的事件中心输入。

    ![“添加流输入”按钮和“事件中心”菜单项已突出显示。](media/stream-analytics-add-input-link.png 'Add stream input - Event Hub')

9. 在 **“新建输入”** 边栏选项卡中，进行以下配置：

    - **名称：** 输入“eventhub”。
    - **从订阅选择事件中心：** 已选中。
    - **订阅：** 确保选择了用于本实验室的订阅。
    - **事件中心命名空间：** 选择用于本实验室的事件中心命名空间。
    - **事件中心名称：** 选择 **“使用现有事件中心”**，然后选择你之前创建的 **“遥测”**。
    - **事件中心使用者组：** 选择 **“使用现有使用者组”**，然后选择 **“$Default”**。
    - **身份验证模式：** 选择 **“连接字符串”**。
    - **事件中心策略名称：** 选择 **“使用现有策略”**，然后选择 **“读取”**。
    - 将所有其他值均保留为其默认值。

    ![“新建输入”表单已使用前面提到的设置填写到相应字段中。](media/stream-analytics-new-input.png 'New Input')

10. 输入完值后，选择表单底部的 **“保存”**。

11. 在“流分析作业”边栏选项卡中，选择左侧菜单中的 **“输出”**。

    ![左侧菜单中已选择“输出”链接。](media/outputs-link.png 'Outputs link')

12. 在顶部工具栏中选择 **“+ 添加”**，然后选择 **“Power BI”** 以创建新的 Power BI 输出。

    ![“添加”按钮和“Power BI”菜单项已突出显示。](media/stream-analytics-add-output-link.png 'Add output - Power BI')

13. 在 **“新建输出”** 边栏选项卡中，选择 **“授权”** 按钮以授权从流分析到 Power BI 帐户的连接。

    ![“授权”按钮在“新建输出”边栏选项卡中已突出显示。](media/stream-analytics-new-output-authorize.png 'New Output')

14. 出现提示时，登录 Power BI 帐户，该帐户使用所提供的用户名和密码，与用于登录 Azure 门户的用户名和密码相同。

    ![显示 Power BI 登录表单。](media/power-bi-sign-in.png 'Power BI Sign In')

15. 成功登录 Power BI 帐户后，“新建输出”边栏选项卡将更新，以显示你当前已获得授权。

    ![“新建输出”边栏选项卡已更新，显示用户已获得使用 Power BI 的授权。](media/stream-analytics-new-output-authorized.png 'Authorized')

16. 在 **“新建输出”** 边栏选项卡中，进行以下配置：

    - **输出别名：** 输入 `powerBIAlerts`。
    - **身份验证模式：** 选择“用户令牌”。
    - **组工作区：** 选择“我的工作区”（如果未显示此选项，请先选择“用户令牌”身份验证模式）。
    - **数据集名称：** 输入 `ContosoAutoVehicleAnomalies`。
    - **表名称：** 输入 `Alerts`。

    ![“新建输出”表单已使用前面提到的设置填写到相应字段中。](media/stream-analytics-new-output.png 'New Output')

17. 输入完值后，选择表单底部的 **“保存”**。

18. 在顶部工具栏中选择 **“+ 添加”**，然后选择 **“Azure Synapse Analytics”** 以创建新的 Synapse Analytics 输出。

    ![“Azure Synapse Analytics”菜单项已突出显示。](media/stream-analytics-add-output-synapse-link.png "Add output - Azure Synapse Analytics")

19. 在 **“新建输出”** 边栏选项卡中，进行以下配置：

    - **输出别名：** 输入“synapse”。
    - **从订阅选择 Azure Synapse Analytics：** 已选中。
    - **订阅：** 选择用于本实验室的订阅。
    - **数据库：** 选择“ContosoAuto”。确保“服务器名称”下显示正确的 Synapse 工作区名称。
    - **表：** 输入 `dbo.VehicleAverages`
    - **身份验证模式：** 选择“连接字符串”。
    - **用户名：** 输入 `asa.sql.admin`
    - **密码：** 输入密码 `P4ssw.rd`、你在部署实验室环境时输入的 SQL 管理员密码值，或者托管实验室环境向你提供的密码。 **备注**： 此密码很可能与用于登录 Azure 门户的密码不同。

    ![“新建输出”表单已使用前面提到的设置填写到相应字段中。](media/synapse-new-output.png "New Output")

20. 输入完值后，选择表单底部的 **“保存”**。

21. 在“流分析作业”边栏选项卡中，选择左侧菜单中的 **“查询”**。

    ![左侧菜单中已选择“查询”链接。](media/query-link.png 'Query link')

22. 清除 **“编辑查询”** 窗口并在其位置粘贴以下内容：

    ```sql
    WITH
        Averages AS (
        select
            AVG(engineTemperature) averageEngineTemperature,
            AVG(speed) averageSpeed
        FROM
            eventhub TIMESTAMP BY [timestamp]
        GROUP BY
            TumblingWindow(Duration(second, 2))
    ),
    Anomalies AS (
        select
            t.vin,
            t.[timestamp],
            t.city,
            t.region,
            t.outsideTemperature,
            t.engineTemperature,
            a.averageEngineTemperature,
            t.speed,
            a.averageSpeed,
            t.fuel,
            t.engineoil,
            t.tirepressure,
            t.odometer,
            t.accelerator_pedal_position,
            t.parking_brake_status,
            t.headlamp_status,
            t.brake_pedal_status,
            t.transmission_gear_position,
            t.ignition_status,
            t.windshield_wiper_status,
            t.abs,
            (case when a.averageEngineTemperature >= 405 OR a.averageEngineTemperature <= 15 then 1 else 0 end) as enginetempanomaly,
            (case when t.engineoil <= 1 then 1 else 0 end) as oilanomaly,
            (case when (t.transmission_gear_position = 'first' OR
                t.transmission_gear_position = 'second' OR
                t.transmission_gear_position = 'third') AND
                t.brake_pedal_status = 1 AND
                t.accelerator_pedal_position >= 90 AND
                a.averageSpeed >= 55 then 1 else 0 end) as aggressivedriving
        from eventhub t TIMESTAMP BY [timestamp]
        INNER JOIN Averages a ON DATEDIFF(second, t, a) BETWEEN 0 And 2
    ),
    VehicleAverages AS (
        select
            AVG(engineTemperature) averageEngineTemperature,
            AVG(speed) averageSpeed,
            System.TimeStamp() as snapshot
        FROM
            eventhub TIMESTAMP BY [timestamp]
        GROUP BY
            TumblingWindow(Duration(minute, 2))
    )
    -- INSERT INTO POWER BI
    SELECT
        *
    INTO
        powerBIAlerts
    FROM
        Anomalies
    where aggressivedriving = 1 OR enginetempanomaly = 1 OR oilanomaly = 1
    -- INSERT INTO SYNAPSE ANALYTICS
    SELECT
        *
    INTO
        synapse
    FROM
        VehicleAverages
    ```

    ![上面的查询已插入到“查询”窗口中。](media/stream-analytics-query.png 'Query window')

    该查询在两秒的时间内对引擎温度和速度取平均值。然后它选择所有遥测数据，包括上一步的平均值，并将以下异常指定为新字段：

    a. **enginetempanomaly**：当平均引擎温度 \>= 405 或 \<= 15 时。

    b. **oilanomaly**：当机油 \<= 1 时。

    c. **aggressivedriving**：当变速器档位为一、二、三档，制动踏板状态为 1，加速踏板位置 \>= 90，平均车速 \>= 55 时。

    查询将异常步骤中的所有字段输出到 `powerBIAlerts` 输出中，其中在报表中 aggressivedriving = 1、enginetempanomaly = 1 或 oilanomaly = 1。该查询还使用 `TumblingWindow(Duration(minute, 2))` 汇总过去两分钟内所有车辆的平均引擎温度和速度，并将这些字段输出到 `synapse` 输出。

23. 完成查询更新后，选择顶部工具栏中的 **“保存查询”**。

24. 在“流分析作业”边栏选项卡中，选择左侧菜单中的 **“概述”**。在“概述”边栏选项卡顶部，选择 **“启动”**。

    ![“启动”按钮已在“概述”边栏选项卡的顶部突出显示。](media/stream-analytics-overview-start-button.png 'Overview')

25. 在显示的“启动作业”边栏选项卡中，对于作业输出启动时间，选择 **“现在”**，然后选择 **“启动”**。这将启动流分析作业，以便稍后开始处理事件并将其发送到 Power BI。

    ![“现在”和“启动”按钮已在“启动作业”边栏选项卡中突出显示。](media/stream-analytics-start-job.png 'Start job')

## 练习 2：生成和直观呈现数据

### 任务 1：运行数据生成器

数据生成器控制台应用程序为一系列车辆（由 VIN（车辆识别号码）表示）创建模拟车辆传感器遥测数据并将这些数据直接发送到事件中心。为此，首先需要使用事件中心连接字符串对其进行配置。

在此任务中，你将配置并运行数据生成器。数据生成器将模拟车辆遥测数据保存到事件中心，提示流分析作业聚合和分析丰富的数据，并将这些数据发送到 Power BI 和 Synapse Analytics。最后一步是在接下来的任务中创建 Power BI 报表。

1. 在实验室 VM 或计算机上，下载 [TransactionGeneratorExecutable.zip](https://solliancepublicdata.blob.core.windows.net/dataengineering/dp-203/TransactionGeneratorExecutable.zip) 文件。

2. 将 zip 文件解压缩到你的机器上，并记下解压缩位置。

3. 打开包含提取的文件的文件夹，然后根据环境打开 `linux-x64`、`osx-x64` 或 `win-x64` 子文件夹。

4. 在相应的子文件夹中，打开 **appsettings.json** 文件。将 `telemetry` 事件中心连接字符串值粘贴到 `EVENT_HUB_CONNECTION_STRING` 旁边。确保在该值周围添加引号 ("")，如图所示。 **“保存”** 文件。

    ![事件中心连接字符串已在 appsettings.json 文件中突出显示。](media/appsettings.png "appsettings.json")

    `SECONDS_TO_LEAD` 是发送车辆遥测数据之前要等待的时间。默认值为 `0`。

    `SECONDS_TO_RUN` 是在停止数据传输之前允许生成器运行的最长时间。默认值为 `1800`。如果你在生成器运行时输入 Ctrl+C，或关闭窗口，数据也将停止传输。

5. 根据你的平台，使用以下方法之一执行数据生成器：

   1. Windows：

      * 只需在 `win-x64` 文件夹中执行 **TransactionGenerator.exe** 即可。

   2. Linux：

      * 导航到 `linux-x64` 文件夹。
      * 通过运行 `chmod 777 DataGenerator` 来访问二进制文件。
      * 运行 `./DataGenerator`。

   3. MacOS：

      * 打开新的终端。
      * 导航到 `osx-x64` 目录。
      * 运行 `./DataGenerator`。

6. 若使用的是 Windows 并在尝试执行数据生成器后收到对话框，请选择 **“详细信息”**，然后选择 **“仍要运行”**。

    ![突出显示了“详细信息”。](media/microsoft-defender-moreinfo.png "Windows protected your PC")

    ![突出显示了“仍要运行”按钮。](media/microsoft-defender-runanyway.png "Run anyway")

6.  随即将打开一个新的控制台窗口，你应该会看到它在几秒钟后开始发送数据。你看到该窗口向事件中心发送数据后，将其最小化并让其在后台运行__。

    ![控制台窗口的屏幕截图。](media/transaction-generator.png 'Console window')

    每当请求发送 500 条记录后，你将看到输出统计信息。

### 任务 2：创建 Power BI 仪表板

在此任务中，你将使用 Power BI 创建报表，以显示捕获的车辆异常数据。然后，将该报表固定到实时仪表板上，以进行准实时的更新。

1. 打开 Web 浏览器并导航到 <https://powerbi.microsoft.com/>。选择右上角的 **“登录”**。

    ![Power BI 主页显示突出显示的“登录”链接。](media/pbi-signin.png 'Power BI home page')

2. 输入在针对流分析创建 Power BI 输出时使用的 Power BI 凭据。

3. 登录后，在左侧菜单中选择 **“我的工作区”**。

    ![左侧菜单中已选择“我的工作区”链接。](media/pbi-my-workspace-link.png 'My Workspace')

4. 选择工作区顶部的 **“数据集 + 数据流”** 选项卡。找到名为 **“ContosoAutoVehicleAnomalies”** 的数据集，然后选择该名称右侧的 **“创建报表”** 操作按钮。如果你没有看到该数据集，则可能需要等待几分钟并刷新页面。

    ![在“我的工作区”中已选择“数据集”选项卡，并且已突出显示 VehicleAnomalies 数据集。](media/pbi-my-workspace.png 'Datasets')

    > **备注：** 可能需要等待数分钟数据集才会显示。可能需要定期刷新页面才能看到“数据集”选项卡。

5. 你应该会看到一个新的空白 VehicleAnomalies 报表，其字段列表位于最右侧。

    ![显示一个新的空白报表，字段列表位于右侧。](media/pbi-blank-report.png 'Blank report')

6. 在右侧的“可视化效果”部分中选择 **“地图”** 可视化效果。

    ![“地图”可视化效果已突出显示。](media/pbi-map-vis.png 'Visualizations')

7. 将 **“city”** 字段拖动到 **“位置”**，将 **“aggressivedriving”** 拖动到 **“大小”**。这将根据激进的驾驶记录数量，在地图中的城市上放置不同大小的点。

    ![屏幕截图显示将字段拖动到地图设置的位置。](media/pbi-map-fields.png 'Map settings')

8. 地图应如下所示：

    ![地图显示在报表中。](media/pbi-map.png 'Map')

9. 选择报表上的空白区域以取消选择地图。现在，选择 **“树状图”** 可视化效果。

    ![“树状图”可视化效果已突出显示。](media/pbi-treemap-vis.png 'Visualization')

10. 将 **“enginetemperature”** 字段拖动到 **“值”**，然后将 **“transmission_gear_position”** 字段拖动到 **“组”**。这将按树状图上的变速器档位对引擎温度值进行分组，以便查看哪些齿轮与最高或最低的引擎温度相关联。树状图根据值调整组的大小，最大的显示在左上角，最小的显示在右下角。

    ![屏幕截图显示将字段拖动到树状图设置的位置。](media/pbi-treemap-fields.png "Treemap settings")

11. 选择 **“值”** 下 **“enginetemperature”** 字段旁边的向下箭头。从菜单中选择 **“平均值”** 以按平均值而不是总和来聚合值。

    ![enginetemperature 值的“平均值”菜单选项已突出显示。](media/pbi-treemap-average.png "Average engine temperature")

12. 树状图应如下所示：

    ![树状图显示在报表中。](media/pbi-treemap.png 'Treemap')

13. 选择报表上的空白区域以取消选择树状图。现在，选择 **“分区图”** 可视化效果。

    ![“分区图”可视化效果已突出显示。](media/pbi-areachart-vis.png 'Area chart visualization')

14. 将 **“region”** 字段拖动到 **“图例”**，将 **“speed”** 字段拖动到 **“值”**，将 **“timestamp”** 字段拖动到 **“轴”**。这将显示一个分区图，其中不同颜色表示区域和驾驶员在该区域中一段时间内的行驶速度。

    ![屏幕截图显示将字段拖动到分区图设置的位置。](media/pbi-areachart-fields.png 'Area chart settings')

15. 选择 **“值”** 下 **“speed”** 字段旁边的向下箭头。从菜单中选择 **“平均值”** 以按平均值而不是总和来聚合值。

    ![speed 值的“平均值”菜单选项已突出显示。](media/pbi-areachart-average.png 'Average speed')

16. 分区图应如下所示：

    ![报表中的分区图。](media/pbi-areachart.png 'Area chart')

17. 选择报表上的空白区域以取消选择分区图。现在，选择 **“多行卡片”** 可视化效果。

    ![“多行卡片”可视化效果已突出显示。](media/pbi-card-vis.png 'Multi-row card visualization')

18. 将 **“aggressivedriving”**、 **“enginetempanomaly”** 和 **“oilanomaly”** 字段拖动到 **“字段”**。

    ![屏幕截图显示将字段拖动到多行卡片设置的位置。](media/pbi-card-fields.png 'Multi-row card settings')

19. 在多行卡片设置中选择 **“格式”** 选项卡，然后展开 **“数据标签”**。将 **“文本大小”** 设置为“30”。展开 **“类别标签”**，并将 **“文本大小”** 设置为“12”。

    ![“格式”选项卡的屏幕截图。](media/pbi-card-format.png 'Multi-row card format')

20. 多行卡片应如下所示：

    ![报表上的多行卡片。](media/pbi-card.png 'Multi-row-card')

21. 选择页面右上角的 **“保存”**。

    ![突出显示了“保存”按钮。](media/pbi-save.png 'Save')

22. 输入名称，例如“Contoso Auto Vehicle Anomalies”，然后选择 **“保存”**。

    ![“保存”对话框的屏幕截图。](media/pbi-save-dialog.png 'Save dialog')

23. 现在，让我们将此报表添加到仪表板。选择报表顶部的 **“固定到仪表板”** （可能需要选择省略号“...”）。

    ![“固定到仪表板”按钮已突出显示。](media/pbi-live.png 'Pin to a dashboard')

24. 选择 **“新建仪表板”**，然后输入名称，例如“Contoso Auto Vehicle Anomalies Dashboard”。选择 **“实时固定”**。出现提示时，选择查看仪表板的选项。你也可以在左侧菜单的“我的工作区”下找到该仪表板。

    ![“固定到仪表板”对话框的屏幕截图。](media/pbi-live-dialog.png 'Pin to dashboard dialog')

25. 在捕获数据时，实时仪表板将自动刷新和更新。可以将鼠标悬停在图表上的任何点上，以查看有关该项的信息。选择平均速度图表上方图例中的区域之一。所有其他图表将自动按该区域进行筛选。再次单击该区域以清除筛选器。

    ![实时仪表板视图。](media/pbi-dashboard.png 'Dashboard')

### 任务 3：在 Synapse Analytics 中查看聚合数据

回想一下，你在流分析中创建查询后，每两分钟聚合一次引擎温度和车速数据，并将这些数据保存到了 Synapse Analytics。此功能演示了流分析查询能够以不同的时间间隔将数据写入多个输出。通过写入 Synapse Analytics 专用 SQL 池，我们能够将历史和当前聚合数据保留为数据仓库的一部分，而无需 ETL/ELT 过程。

在此任务中，你将查看 Synapse Analytics 中的异常数据。

1. 如果尚未完成此操作，请**停止** **TransactionGenerator**。

2. 导航到 [Azure 门户](https://portal.azure.com)。

3. 在左侧菜单中选择 **“资源组”**。然后，选择名为 **“ms-dataengineering-14”** 的资源组。

4. 从资源组中的资源列表中选择 **“Synapse 工作区”** (`asaworkspaceYOUR_UNIQUE_ID`)。

    ![资源组中已选择“Synapse 工作区”。](media/rg-synapse-workspace.png "resource group")

5. 在 **“概述”** 窗格中的 **“打开 Synapse Studio”** 框中选择“打开”。

    ![“打开”链接已突出显示。](media/open-synapse-studio.png "Open Synapse Studio")

6. 在 Synapse Studio 中，选择左侧菜单中的 **“数据”** 以导航到“数据中心”。

    ![图中突出显示了“数据”中心。](media/data-hub.png "Data hub")

7. 选择 **“工作区”** 选项卡 **(1)**，展开 `ContosoAuto` 数据库，展开 `Tables`，然后右键单击 **“dbo.VehicleAverages”** 表 **(2)**。如果你没有看到列出的表，请刷新表列表。选择 **“新建 SQL 脚本”(3)**，然后 **“选择前 100 行”(4)**。

    ![已选中“选择前 100 行”菜单项。](media/select-top-100-rows.png "Select TOP 100 rows")

8. 查看查询结果，并观察 `AverageEngineTemperature` 和 `AverageSpeed` 中存储的聚合数据。`Snapshot` 值在这些记录之间每两分钟更改一次。

   ![显示了 VehicleAverages 表输出。](media/synapse-vehicleaverage-table.png "VehicleAverages results")

9. 在“结果”输出中选择 **“图表”** 视图，然后将图表类型设置为 **“区域”**。此可视化效果显示一段时间内平均引擎温度与平均速度的关系。请随意尝试图表设置。

![显示了图表视图。](media/synapse-vehicleaverage-chart.png "VehicleAverages chart")

## 练习 3：清理

完成以下步骤，停止数据生成器并释放不再需要的资源。

### 任务 1：停止数据生成器

1. 返回运行数据生成器的控制台/终端窗口。关闭窗口以停止生成器。

### 任务 2：停止流分析作业

1. 导航到 Azure 门户中的流分析作业。

2. 在 **“概览”** 窗格中，选择 **“停止”**，然后在出现提示时选择 **“是”**。

    ![突出显示了“停止”按钮。](media/asa-stop.png "Stop")

### 任务 3：暂停专用 SQL 池

1. 打开 Synapse Studio (<https://web.azuresynapse.net/>)。

2. 选择 **“管理”** 中心。

    ![图中突出显示了“管理”中心。](media/manage-hub.png "Manage hub")

3. 在左侧菜单中，选择 **“SQL 池” (1)**。将鼠标悬停在专用 SQL 池的名称上，并选择 **“暂停” (2)**。

    ![突出显示了专用 SQL 池上的“暂停”按钮。](media/pause-dedicated-sql-pool.png "Pause")

4. 出现提示时，选择 **“暂停”**。

    ![突出显示了“暂停”按钮。](media/pause-dedicated-sql-pool-confirm.png "Pause")
