-module(jobqueue).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([init_datastore/0, start/0, stop/0, insert_job/5, find_job/1,
         job_completed/1, job_failed/2,
         grab_job/1, grab_job/2, grab_one_job/1, grab_one_job/2]).

-define(GRAB_FOR, 60*60). %% Default time to hold a job before giving it to another worker

-include_lib("stdlib/include/qlc.hrl").
-include_lib("jobqueue.hrl").

init_datastore() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(job,
            [{attributes, record_info(fields, job)},
             {disc_only_copies, [node()]},
             {index, [func, uniqkey, available_after]}]),
    mnesia:stop().

start() ->
    crypto:start(),
    mnesia:start(),
    mnesia:wait_for_tables([job], 20000).

stop() ->
    mnesia:stop().

insert_job(Func, Arg, UniqKey, AvailableAfter, Priority) ->
    %% TODO: Make sure there isn't already a job with UniqKey if it's defined
    JobID = new_id(),
    Now = nows(),
    case AvailableAfter of
        _ when AvailableAfter > 1000000000 ->
            AvailableAfter2 = AvailableAfter;
        _ ->
            AvailableAfter2 = Now + AvailableAfter
    end,
    Row = #job{job_id=JobID, func=Func, arg=Arg, uniqkey=UniqKey,
               insert_time=Now, available_after=AvailableAfter2,
               priority=Priority},
    F = fun() ->
            mnesia:write(Row)
        end,
    {atomic, ok} = mnesia:transaction(F),
    {ok, JobID}.

find_job(Funcs) ->
    Now = nows(),
    Query = qlc:q([X#job.job_id || X <- mnesia:table(job),
                        lists:member(X#job.func, Funcs),
                        X#job.available_after < Now]),
    JobIDs = execute_query(Query, 10),
    grab_job(JobIDs).

job_completed(JobID) ->
    F = fun() ->
        case mnesia:read(job, JobID) of
            [] ->
                not_found;
            _ ->
                mnesia:delete({job, JobID})
        end
    end,
    {atomic, Res} = mnesia:transaction(F),
    Res.

job_failed(JobID, Reason) ->
    Now = nows(),
    F = fun() ->
        case mnesia:read(job, JobID) of
            [] ->
                not_found;
            [Job] ->
                mnesia:write(Job#job{available_after=Now, failures=[Reason|Job#job.failures]})
        end
    end,
    {atomic, Res} = mnesia:transaction(F),
    Res.

grab_job(JobIDs) ->
    grab_job(JobIDs, ?GRAB_FOR).
grab_job([], _GrabFor) ->
    {fail, no_jobs};
grab_job([JobID|JobIDs], GrabFor) ->
    case grab_one_job(JobID, GrabFor) of
        {ok, Job} ->
            {ok, Job};
        {fail, lost_race} ->
            grab_job(JobIDs, GrabFor)
    end.

grab_one_job(JobID) ->
    grab_one_job(JobID, ?GRAB_FOR).
grab_one_job(JobID, GrabFor) ->
    Now = nows(),
    F = fun() ->
        [Job] = mnesia:read(job, JobID),
        if
            Job#job.available_after < Now ->
                ok = mnesia:write(Job#job{available_after=Now + GrabFor}),
                {ok, Job};
            true ->
                {fail, lost_race}
        end
    end,
    {atomic, Res} = mnesia:transaction(F),
    Res.

% do(Q) ->
%     F = fun() -> qlc:e(Q) end,
%     {atomic, Val} = mnesia:transaction(F),
%     Val.

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
