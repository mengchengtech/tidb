declare type Duration = string
declare type Float64 = number
declare type Int32 = number
// 32位系统中表示32位长，64位系统中表示64位长
declare type Int = number
declare type Int64 = number
declare type UInt64 = number

declare interface FullTraceLog {
  /**
   * 执行sql时的当前库名称
   */
  db: string
  /**
   * 执行的sql中用到的所有数据库名称列表。
   */
  dbs: string
  /**
   * 执行sql时使用的账号
   */
  usr: string
  /**
   * 所属租户信息
   */
  tenant: string
  /**
   * 执行sql的客户端的信息
   */
  client: clientInfo
  /**
   * 当前sql是否在事务中
   */
  inTX: boolean
  /**
   * sql语句分类
   */
  cat: string
  /**
   * sql语句类型
   */
  tp: string
  /**
   * sql中指定的跨库查询的数据库
   */
  across: string
  /**
   * 执行sql开始时间，即创建mctech.Context的时间
   */
  at: string
  /**
   * 事务号(显示事务和隐式事务)
   */
  txId: string
  /**
   * sql执行过程中读取/生成的最大行数（与rows不一样，中间过程生成的行数多不代表结果集中的行数多）
   */
  maxAct: Int64
  /**
   * sql 语句模板的hash
   */
  digest: string
  /**
   * 查询语句返回结果行数
   */
  rows: Int64
  /**
   * 该 SQL 查询执行时占用的最大内存空间
   */
  mem: Int64
  /**
   * 该 SQL 查询执行时占用的最大磁盘空间
   */
  disk: Int64
  /**
   * 各种时间信息
   */
  times: LogTimeInfo
  /**
   * 当前sql资源消耗信息
   */
  ru: LogRUStatInfo
  /**
   * max coprocessor相关的信息
   */
  maxCop?: logMaxCopObject
  /**
   * 事务提交相关的信息（显示事务/隐式事务）
   */
  tx?: LogTXInfo
  /**
   * sql执行生成的警告信息
   */
  warnings: logWarningObjects
  /**
   * 执行sql出错时的错误信息
   */
  error?: string
  /**
   * 原始sql语句（片断）
   */
  sql: string
  /**
   * 当sql保存的内容不完整时，保存压缩后的完整sql内容
   */
  compress?: compressSQLObject
}

declare interface compressSQLObject {
  /**
   * 完整sql长度
   */
  len: number
  /**
   * 当sql保存的内容不完整时，保存压缩后的完整sql内容
   */
  data: string
}

declare interface clientInfo {
  /**
   * 客户端ip地址
   */
  address: string
  /**
   * 提取自 /* from:'......' *\/ 一般表示eureka里注册的服务名称。如果找不到服务名称则会用unknown({db})代替
   */
  app: string
  /**
   * 来源与service一样，表示所属产品线，如果找不到则为空。比较旧的服务执行sql里提取到的信息不含product
   */
  product?: string
  /**
   * 提取自 /\* package:'' *\/ 表示当前sql是在某个依赖包中执行的。
   */
  pkg: string
  /**
   * SQL 查询客户端连接 ID (36进制重新编码)
   */
  conn: string
}

declare interface logWarningObjects {
  topN: logWarningObject[]
  total: Int
}

declare interface logWarningObject {
  msg: string
  extra: boolean
}

declare interface LogTXInfo {
  /**
   * 写入 RockDB Key 个数
   */
  keys: Int
  /**
   * 写入数据量Bytes
   */
  size: Int
  /**
   * sql执行结果影响的数据行数
   */
  affected: UInt64
}

declare interface logMaxCopObject {
  /**
   * 用时最长的cop任务所在节点
   */
  procAddr: string
  /**
   * 用时最长的cop任务花费的时间
   */
  procTime: Duration
  /**
   * Coprocessor 请求数
   */
  tasks: Int
}

declare interface LogRUStatInfo {
  /**
   * sql执行消耗的RRU值
   */
  rru: Float64
  /**
   * sql执行消耗的WRU值
   */
  wru: Float64
}

declare interface LogTimeInfo {
  /**
   * 执行总时间，执行 SQL 耗费的自然时间
   */
  all: Duration
  /**
   * 解析语法树用时，含mctech扩展
   */
  parse: Duration
  /**
   * 生成执行计划耗时
   */
  plan: Duration
  /**
   * 通过执行计划算出的在tidb-server里cpu耗时
   */
  tidb: Duration
  /**
   * 首行结果准备好时间(总执行时间除去发送结果耗时)
   */
  ready: Duration
  /**
   * 发送到客户端用时
   */
  send: Duration

  /**
   *  cop task相关的时间
   */
  cop: {
    /**
     * tidb 上等待所有的 cop tasks (tikv, tiflash) 执行完毕耗费的自然时间。直接从ExecDetails.CopTime获取到的时间。如果存在并行任务的话，这个时间一般小于各个并行任务的总时间
     */
    wall: Duration
    /**
     * 从ExecDetails.TimeDetail.ProcessTime获取到的tikv处理请求的过程总共用时。大多数时候都可以替代表示用于CPU的时间
     */
    process: Duration
    /**
     * 从执行计划中汇总统计的TiFlash执行Coprocessor 耗时
     */
    tiflash: Duration
  }
  /**
   * 提交事务相关的信息（含显示事务/隐式事务）
   */
  tx?: {
    /**
     * 事务两阶段提交中第一阶段（prewrite 阶段）的耗时
     */
    prewrite: Duration
    /**
     * 事务两阶段提交中第二阶段（commit 阶段）的耗时
     */
    commit: Duration
  }
}
