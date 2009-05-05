-module(jobqueue_gearman).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([start/0, stop/0]).
-import(gearman_worker, [start/2]).

-include_lib("gearman.hrl").
-include_lib("jobqueue.hrl").

start() ->
    jobqueue:start(),
    gearman_worker:start(
        lists:duplicate(5, {"127.0.0.1"}),
        [
            {"jobqueue.insert_job", serialized_func(fun insert_job/2)},
            {"jobqueue.find_job", serialized_func(fun find_job/2)},
            {"jobqueue.job_completed", serialized_func(fun job_completed/2)},
            {"jobqueue.job_failed", serialized_func(fun job_failed/2)}
        ]).

stop() ->
    jobqueue:stop().

%%

%% Arguments: func, arg, uniqkey, available_after, priority
insert_job(_Task, Args) ->
    Func = table_lookup(Args, "func"),
    Arg = table_lookup(Args, "arg"),
    UniqKey = table_lookup(Args, "uniqkey", ""),
    AvailableAfter = table_lookup(Args, "available_after", 0),
    Priority = table_lookup(Args, "priority", 0),
    case jobqueue:insert_job(Func, Arg, UniqKey, AvailableAfter, Priority) of
        {ok, JobID} ->
            {obj, [
                {"handle", JobID}]}
    end.

find_job(_Task, Args) ->
    Funcs = table_lookup(Args, "funcs"),
    case jobqueue:find_job(Funcs) of
        {fail, _Reason} ->
            null;
        {ok, Job} ->
            {obj, [
                {"handle", Job#job.job_id},
                {"arg", Job#job.arg}]}
    end.

job_completed(_Task, Args) ->
    Handle = table_lookup(Args, "handle"),
    case jobqueue:job_completed(Handle) of
        ok ->
            {obj, [{"success", true}]};
        Else ->
            {obj, [{"success", false}, {"error", list_to_binary(atom_to_list(Else))}]}
    end.

job_failed(_Task, Args) ->
    Handle = table_lookup(Args, "handle"),
    Reason = table_lookup(Args, "reason"),
    case jobqueue:job_failed(Handle, Reason) of
        ok ->
            {obj, [{"success", true}]};
        Else ->
            {obj, [{"success", false}, {"error", list_to_binary(atom_to_list(Else))}]}
    end.

table_lookup(Table, Key) ->
    case lists:keysearch(Key, 1, Table) of
        false ->
            throw("Required key not found table");
        {value, {Key, Value}} ->
            Value
    end.
table_lookup(Table, Key, Default) ->
    case lists:keysearch(Key, 1, Table) of
        false ->
            Default;
        {value, {Key, Value}} ->
            Value
    end.

serialized_func(Func) ->
    fun(Task) ->
        case rfc4627:decode(Task#task.arg) of
            {ok, {obj, Args}, _} ->
                Res = Func(Task, Args),
                rfc4627:encode(Res);
            _ ->
                throw("Received invalid json object for function arguments")
        end
    end.
