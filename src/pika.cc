// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include <memory.h>
#include <sys/resource.h>
#include <csignal>

#include "include/build_version.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_command.h"
#include "include/pika_conf.h"
#include "include/pika_define.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "include/pika_slot_command.h"
#include "include/pika_version.h"
#include "net/include/net_stats.h"
#include "pstd/include/env.h"
#include "pstd/include/pika_codis_slot.h"
#include "pstd/include/pstd_defer.h"

std::unique_ptr<PikaConf> g_pika_conf;
// todo : change to unique_ptr will coredump
PikaServer* g_pika_server = nullptr;
std::unique_ptr<PikaReplicaManager> g_pika_rm;

std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

extern std::unique_ptr<net::NetworkStatistic> g_network_statistic;

static void version() {
  char version[32];
  snprintf(version, sizeof(version), "%d.%d.%d", PIKA_MAJOR, PIKA_MINOR, PIKA_PATCH);
  std::cout << "-----------Pika server----------" << std::endl;
  std::cout << "pika_version: " << version << std::endl;
  std::cout << pika_build_git_sha << std::endl;
  std::cout << "pika_build_compile_date: " << pika_build_compile_date << std::endl;
  // fake version for client SDK
  std::cout << "redis_version: " << version << std::endl;
}

static void PrintPikaLogo() {
  printf(
      "   .............          ....     .....       .....           .....         \n"
      "   #################      ####     #####      #####           #######        \n"
      "   ####         #####     ####     #####    #####            #########       \n"
      "   ####          #####    ####     #####  #####             ####  #####      \n"
      "   ####         #####     ####     ##### #####             ####    #####     \n"
      "   ################       ####     ##### #####            ####      #####    \n"
      "   ####                   ####     #####   #####         #################   \n"
      "   ####                   ####     #####    ######      #####         #####  \n"
      "   ####                   ####     #####      ######   #####           ##### \n");
}

static void PikaConfInit(const std::string& path) {
  printf("path : %s\n", path.c_str());
  g_pika_conf = std::make_unique<PikaConf>(path);
  if (g_pika_conf->Load() != 0) {
    LOG(FATAL) << "pika load conf error";
  }
  version();
  printf("-----------Pika config list----------\n");
  g_pika_conf->DumpConf();
  PrintPikaLogo();
  printf("-----------Pika config end----------\n");
}

static void PikaGlogInit() {
  if (!pstd::FileExists(g_pika_conf->log_path())) {
    pstd::CreatePath(g_pika_conf->log_path());
  }

  if (!g_pika_conf->daemonize()) {
    FLAGS_alsologtostderr = true;
  }
  FLAGS_log_dir = g_pika_conf->log_path();
  FLAGS_minloglevel = 0;
  FLAGS_max_log_size = 1800;
  FLAGS_logbufsecs = 0;
  ::google::InitGoogleLogging("pika");
}

static void daemonize() {
  if (fork()) {
    exit(0); /* parent exits */
  }
  setsid(); /* create a new session */
}

static void close_std() {
  int fd;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    close(fd);
  }
}

static void create_pid_file() {
  /* Try to write the pid file in a best-effort way. */
  std::string path(g_pika_conf->pidfile());

  size_t pos = path.find_last_of('/');
  if (pos != std::string::npos) {
    pstd::CreateDir(path.substr(0, pos));
  } else {
    path = kPikaPidFile;
  }

  FILE* fp = fopen(path.c_str(), "w");
  if (fp) {
    fprintf(fp, "%d\n", static_cast<int>(getpid()));
    fclose(fp);
  }
}

static void IntSigHandle(const int sig) {
  LOG(INFO) << "Catch Signal " << sig << ", cleanup...";
  g_pika_server->Exit();
}

static void PikaSignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

static void usage() {
  char version[32];
  snprintf(version, sizeof(version), "%d.%d.%d", PIKA_MAJOR, PIKA_MINOR, PIKA_PATCH);
  fprintf(stderr,
          "Pika module %s\n"
          "usage: pika [-hv] [-c conf/file]\n"
          "\t-h               -- show this help\n"
          "\t-c conf/file     -- config file \n"
          "\t-v               -- show version\n"
          "  example: ./output/bin/pika -c ./conf/pika.conf\n",
          version);
}
// 主函数入口，处理命令行参数，初始化配置并启动服务
int main(int argc, char* argv[]) {
  // 检查命令行参数的数量，确保为 2 或 3 个参数
  if (argc != 2 && argc != 3) {
    usage();   // 输出使用说明
    exit(-1);  // 参数错误，退出程序
  }

  bool path_opt = false;  // 标志位，用于检查是否指定了配置文件路径
  signed char c;          // 用于存储命令行选项
  char path[1024];        // 配置文件路径
  while (-1 != (c = static_cast<int8_t>(getopt(argc, argv, "c:hv")))) {  // 解析命令行参数
    switch (c) {
      case 'c':                              // 如果有 -c 参数，指定配置文件路径
        snprintf(path, 1024, "%s", optarg);  // 存储配置文件路径
        path_opt = true;                     // 设置路径标志为 true
        break;
      case 'h':  // 如果有 -h 参数，显示帮助信息
        usage();
        return 0;
      case 'v':  // 如果有 -v 参数，显示版本信息
        version();
        return 0;
      default:
        usage();  // 参数无效，输出帮助信息
        return 0;
    }
  }

  // 如果未指定配置文件路径，打印错误并退出
  if (!path_opt) {
    fprintf(stderr, "Please specify the conf file path\n");
    usage();   // 输出使用说明
    exit(-1);  // 退出程序
  }

  // 初始化命令表管理器并加载命令表
  g_pika_cmd_table_manager = std::make_unique<PikaCmdTableManager>();
  g_pika_cmd_table_manager->InitCmdTable();

  // 加载配置文件
  PikaConfInit(path);

  // 配置文件中设置的最大客户端数 + 保留的最小文件描述符数量
  rlimit limit;
  rlim_t maxfiles = g_pika_conf->maxclients() + PIKA_MIN_RESERVED_FDS;

  // 获取当前系统文件描述符限制
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    LOG(WARNING) << "getrlimit error: " << strerror(errno);  // 如果获取失败，记录警告
  } else if (limit.rlim_cur < maxfiles) {                    // 如果当前文件描述符限制小于需要的数量
    rlim_t old_limit = limit.rlim_cur;
    limit.rlim_cur = maxfiles;                     // 更新文件描述符限制
    limit.rlim_max = maxfiles;                     // 设置最大值
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {  // 尝试修改文件描述符限制
      LOG(WARNING) << "your 'limit -n ' of " << old_limit
                   << " is not enough for Redis to start. pika have successfully reconfig it to " << limit.rlim_cur;
    } else {
      LOG(FATAL) << "your 'limit -n ' of " << old_limit
                 << " is not enough for Redis to start. pika can not reconfig it(" << strerror(errno)
                 << "), do it by yourself";  // 如果修改失败，记录致命错误并退出
    }
  }

  // 如果配置文件中设置了守护进程模式，进行守护进程化
  if (g_pika_conf->daemonize()) {
    daemonize();        // 守护进程化
    create_pid_file();  // 创建 PID 文件
  }

  // 初始化 Glog 日志系统
  PikaGlogInit();

  // 设置信号处理
  PikaSignalSetup();

  LOG(INFO) << "Server at: " << path;

  // 创建 PikaServer 和 PikaReplicaManager 对象
  g_pika_server = new PikaServer();
  g_pika_rm = std::make_unique<PikaReplicaManager>();
  g_network_statistic = std::make_unique<net::NetworkStatistic>();

  // 初始化数据库结构
  g_pika_server->InitDBStruct();

  // 确保命令表在初始化统计信息之前已加载
  g_pika_server->InitStatistic(g_pika_cmd_table_manager->GetCmdTable());

  // 初始化 ACL (访问控制列表)
  auto status = g_pika_server->InitAcl();
  if (!status.ok()) {
    LOG(FATAL) << status.ToString();  // 如果初始化失败，记录致命错误并退出
  }

  // 如果配置文件中设置了守护进程模式，关闭标准输入输出
  if (g_pika_conf->daemonize()) {
    close_std();  // 关闭标准输入输出
  }

  // 在程序退出时释放资源
  DEFER {
    delete g_pika_server;  // 删除 PikaServer 实例
    g_pika_server = nullptr;
    g_pika_rm.reset();                  // 重置 PikaReplicaManager 实例
    g_pika_cmd_table_manager.reset();   // 重置 PikaCmdTableManager 实例
    g_network_statistic.reset();        // 重置 NetworkStatistic 实例
    ::google::ShutdownGoogleLogging();  // 关闭 Google 日志
    g_pika_conf.reset();                // 重置配置对象
  };

  // 如果需要清理数据，执行数据清理操作
  if (g_pika_conf->wash_data()) {
    auto dbs = g_pika_server->GetDB();
    for (auto& kv : dbs) {
      if (!kv.second->WashData()) {
        LOG(FATAL) << "write batch error in WashData";  // 如果数据清理失败，记录致命错误并退出
        return 1;
      }
    }
  }

  // 启动 ReplicaManager 和 PikaServer
  g_pika_rm->Start();
  g_pika_server->Start();

  // 如果配置文件中设置了守护进程模式，删除 PID 文件
  if (g_pika_conf->daemonize()) {
    unlink(g_pika_conf->pidfile().c_str());
  }

  // 停止 PikaReplicaManager，确保不再有内部线程引用已死的 PikaServer 实例
  g_pika_rm->Stop();

  return 0;  // 程序正常退出
}
