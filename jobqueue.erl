-module(jobqueue).
-author('Samuel Stauffer <samuel@lefora.com>').

-export([init_datastore/0, start/0, stop/0, stats/0, job_counts/0, insert_job/5,
         find_jobs/1, find_jobs/3,
         job_completed/1, job_failed/2, job_failed/3, grab_jobs/3, grab_one_job/2]).

-define(DEFAULT_WORK_TIMEOUT, 60*60). %% Default time to hold a job before giving it to another worker

-include_lib("stdlib/include/qlc.hrl").
-include_lib("jobqueue.hrl").

init_datastore() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(job_count,
            [{attributes, record_info(fields, job_count)},
             {disc_copies, [node()]}]),
    mnesia:create_table(job,
            [{attributes, record_info(fields, job)},
             {disc_copies, [node()]},
             {index, [func, uniqkey, available_after]}]),
    mnesia:stop().

start() ->
    crypto:start(),
    mnesia:start(),
    mnesia:wait_for_tables([job], 20000).

stop() ->
    mnesia:stop().

delay_to_timestamp(Delay) when is_integer(Delay)->
    case Delay of
        _ when Delay > 1000000000 ->
            Delay;
        _ ->
            Delay + nows()
    end.

insert_job(Func, Arg, UniqKey, AvailableAfter, Priority) ->
    %% TODO: Make sure there isn't already a job with UniqKey if it's defined
    JobID = new_id(),
    Now = nows(),
    AvailableAfter2 = delay_to_timestamp(AvailableAfter),
    Row = #job{job_id=JobID, func=Func, arg=Arg, uniqkey=UniqKey,
               insert_time=Now, available_after=AvailableAfter2,
               priority=Priority},
    F = fun() ->
            mnesia:write(Row)
        end,
    {atomic, ok} = mnesia:transaction(F),
    mnesia:dirty_update_counter(job_count, Func, 1),
    {ok, JobID}.

stats() ->
    [
        {num_jobs, mnesia:table_info(job, size)},
        {job_counts, job_counts()}
    ].

job_counts() ->
    execute_query(qlc:q(
        [{X#job_count.func, X#job_count.count} || X <- mnesia:table(job_count)])).

% list_jobs(Func) ->

find_jobs(Funcs) ->
    find_jobs(Funcs, 1, 0).
find_jobs(Funcs, Count, 0) ->
    find_jobs(Funcs, Count, ?DEFAULT_WORK_TIMEOUT);
find_jobs(Funcs, Count, Timeout) ->
    Now = nows(),
    Query = qlc:q([X#job.job_id || X <- mnesia:table(job),
                        lists:member(X#job.func, Funcs),
                        X#job.available_after < Now]),
    JobIDs = execute_query(Query, 10),
    grab_jobs(JobIDs, Count, Timeout).

job_completed(JobID) ->
    F = fun() ->
        case mnesia:read({job, JobID}) of
            [] ->
                not_found;
            [Job] ->
                case mnesia:delete({job, JobID}) of
                    ok ->
                        mnesia:dirty_update_counter(job_count, Job#job.func, -1),
                        ok;
                    Else ->
                        Else
                end
        end
    end,
    {atomic, Res} = mnesia:transaction(F),
    Res.

job_failed(JobID, Reason) ->
    job_failed(JobID, Reason, 0).
job_failed(JobID, Reason, DelayRetry) ->
    F = fun() ->
        case mnesia:read({job, JobID}) of
            [] ->
                not_found;
            [Job] ->
                mnesia:write(Job#job{
                    available_after=delay_to_timestamp(DelayRetry),
                    failures=[Reason|Job#job.failures]})
        end
    end,
    {atomic, Res} = mnesia:transaction(F),
    Res.

grab_jobs(_JobIDs, 0, _Timeout) ->
    [];
grab_jobs([], _Count, _Timeout) ->
    [];
grab_jobs([JobID|JobIDs], Count, Timeout) ->
    case grab_one_job(JobID, Timeout) of
        {ok, Job} ->
            [Job|grab_jobs(JobIDs, Count - 1, Timeout)];
        {fail, lost_race} ->
            grab_jobs(JobIDs, Count, Timeout)
    end.

grab_one_job(JobID, Timeout) ->
    Now = nows(),
    F = fun() ->
        [Job] = mnesia:read({job, JobID}),
        if
            Job#job.available_after < Now ->
                ok = mnesia:write(Job#job{available_after=Now + Timeout}),
                {ok, Job};
            true ->
                {fail, lost_race}
        end
    end,
    {atomic, Res} = mnesia:transaction(F),
    Res.

execute_query(Query) ->
    F = fun() -> qlc:e(Query) end,
    {atomic, Val} = mnesia:transaction(F),
    Val.

execute_query(Query, Limit) ->
    F = fun() ->
        C = qlc:cursor(Query),
        R = qlc:next_answers(C, Limit),
        ok = qlc:delete_cursor(C),
        R
    end,
    {atomic, Val} = mnesia:transaction(F),
    Val.

nows() ->
    {MegaSec, Sec, _MicroSec} = now(),
    MegaSec*1000000+Sec. % +(MicroSec/1000000.0).

%% Generate Unique Primary Key

new_id() ->
    list_to_binary(to_string(uuid4())).

uuid4() ->
    B1 = crypto:rand_uniform(0, 16#1000000000000),
    B2 = crypto:rand_uniform(0, 16#1000),
    B3 = crypto:rand_uniform(0, 16#100000000),
    B4 = crypto:rand_uniform(0, 16#40000000),
    <<B1:48, 4:4, B2:12, 2:2, B3:32, B4:30>>.
to_string(U) ->
    lists:flatten(io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~2.16.0b~2.16.0b-~12.16.0b", get_parts(U))).
get_parts(<<TL:32, TM:16, THV:16, CSR:8, CSL:8, N:48>>) ->
    [TL, TM, THV, CSR, CSL, N].
