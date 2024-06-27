--SET 'table.local-time-zone' = 'UTC';
SET 'table.local-time-zone' = 'Asia/Shanghai';
--SET 'table.local-time-zone' = 'America/Los_Angeles';

set 'table.exec.sink.not-null-enforcer' = 'drop';
set taskmanager.numberOfTaskSlots=2;

-- 设置流模式
-- SET 'execution.runtime-mode' = 'streaming';
-- 测试参数, 生成环境不需设置
SET pipeline.operator-chaining = false;
SET 'sql-client.execution.result-mode' = 'tableau';
set pipeline.operator-chaining=true;
--set parallelism.default = 4;


--set 'taskmanager.memory.process.size' = '8192m';
set 'jobmanager.memory.process.size' = '1024m';
set classloader.check-leaked-classloader='false';


SET 'execution.checkpointing.interval' = '3min';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.timeout' = '30min';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.unaligned' = 'false';
set 'sql-client.display.max-column-width' = '100';
set 'state.checkpoints.dir' = 'hdfs://cdh01:8020/user/yarn/flink_program/checkpoint';
set 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';
SET 'table.dml-sync' = 'true';
set 'state.checkpoints.num-retained' = 5;

-- SET 'sql-client.verbose' = 'true';
-- 解决_AskTimeoutException
set 'akka.ask.timeout' = '120s';
set 'web.timeout' = '120000';
set 'state.backend' = 'rocksdb';
set 'state.backend.incremental' = 'true';
set 'state.backend.rocksdb.checkpoint.transfer.thread.num' = '1';
set 'state.backend.rocksdb.timer-service.factory' = 'HEAP';
set 'state.backend.fs.checkpointdir' = 'hdfs://cdh01:8020/user/yarn/flink_program/backstand';








