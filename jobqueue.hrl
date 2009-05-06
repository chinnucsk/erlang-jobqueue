-record(job_count, {func, count}).
-record(job, {job_id, func, arg, uniqkey, insert_time, available_after, priority, failures=[], notes=[]}).
% -record(error, {job_id, func, error_time, message}).
% -record(exitstatus, {job_id, func, status, completion_time, delete_after}).
