-module(jobqueue_gearman).
-author('Samuel Stauffer <samuel@descolada.com>').

% -behaviour(gen_server).

-export([start/0, start/2, stop/0, objectify/1]).

%% gen_server callbacks
% -export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
%          code_change/3]).

-include_lib("gearman.hrl").
-include_lib("jobqueue.hrl").

start() ->
    start([{"127.0.0.1"}], 5).
start(Servers, NumWorkers) ->
    jobqueue:start(),
    start_workers(lists:flatten(lists:duplicate(NumWorkers, Servers))).

start_workers([]) ->
    [];
start_workers([Server|Servers]) ->
    [gearman_worker:start(Server, [{"jobqueue", fun dispatcher/1}])|start_workers(Servers)].

stop() ->
    jobqueue:stop().

%%

dispatcher(Task) ->
    Zlib = zlib:open(),
    {ok, Args} = decode_args(Zlib, Task#task.arg),
    Method = list_to_atom(binary_to_list(table_lookup(Args, "method"))),
    {obj, Params} = table_lookup(Args, "params"),
    Response = dispatch(Method, Params),
    EncResponse = encode_args(Zlib, Response),
    zlib:close(Zlib),
    EncResponse.

dispatch(Method, Params) ->
    try service(Method, Params) of
        {ok, Result} ->
            format_result(Result);
        {error, ErrorType, ErrorMessage} ->
            format_error(ErrorType, ErrorMessage)
    catch
        error:badarg ->
            format_error(<<"DispatchError">>, <<"Invalid set of params">>);
        Exc1:Exc2 ->
            format_error(list_to_binary([atom_to_list(Exc1), <<":">>, atom_to_list(Exc2)]), "Fail")
    end.

format_result(Result) ->
    {obj, [{<<"error">>, null}, {<<"result">>, Result}]}.

format_error(ErrorType, ErrorMessage) ->
    {obj, [
        {<<"result">>, null},
        {<<"error">>,
            {obj, [
                {<<"type">>, ErrorType},
                {<<"message">>, ErrorMessage}
            ]}}
    ]}.

service(stats, _Params) ->
    {ok, objectify(jobqueue:stats())};
service(insert_job, Params) ->
    Func = table_lookup(Params, "func"),
    Arg = table_lookup(Params, "arg"),
    UniqKey = table_lookup(Params, "uniqkey", ""),
    AvailableAfter = table_lookup(Params, "available_after", 0),
    Priority = table_lookup(Params, "priority", 0),
    case jobqueue:insert_job(Func, Arg, UniqKey, AvailableAfter, Priority) of
        {ok, JobID} ->
            {ok, {obj, [
                {"handle", JobID}]}}
    end;
service(find_jobs, Params) ->
    io:format("~p~n", [Params]),
    Funcs = table_lookup(Params, "funcs"),
    Count = table_lookup(Params, "count"),
    Timeout = table_lookup(Params, "timeout", 0),
    case jobqueue:find_jobs(Funcs, Count, Timeout) of
        [] ->
            {ok, []};
        Jobs when is_list(Jobs) ->
            {ok, [{obj, [
                {"handle", Job#job.job_id},
                {"func", Job#job.func},
                {"arg", Job#job.arg},
                {"failures", Job#job.failures}]} || Job <- Jobs]}
    end;
service(job_completed, Params) ->
    Handle = table_lookup(Params, "handle"),
    case jobqueue:job_completed(Handle) of
        ok ->
            {ok, null};
        Else ->
            {error, <<"JobQueueError">>, list_to_binary(atom_to_list(Else))}
    end;
service(job_failed, Params) ->
    Handle = table_lookup(Params, "handle"),
    Reason = table_lookup(Params, "reason"),
    DelayRetry = table_lookup(Params, "delay_retry", 0),
    case jobqueue:job_failed(Handle, Reason, DelayRetry) of
        ok ->
            {ok, null};
        Else ->
            {error, <<"JobQueueError">>, list_to_binary(atom_to_list(Else))}
    end;
service(_Method, _Params) ->
    {error, <<"DispatchError">>, <<"Unknown method">>}.

%% Utility functions

objectify(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
objectify(List) when is_list(List) ->
    {obj, objectify_list(List)};
objectify(Tuple) when is_tuple(Tuple) ->
    list_to_tuple(objectify_list(tuple_to_list(Tuple)));
objectify(Other) ->
    Other.
objectify_list([]) ->
    [];
objectify_list([Head|Rest]) ->
    [objectify(Head)|objectify_list(Rest)].

table_lookup(Table, Key) ->
    case lists:keysearch(Key, 1, Table) of
        false ->
            throw("Required key not found in table");
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

decode_args(Zlib, Data) ->
    ok = zlib:inflateInit(Zlib),
    Data2 = list_to_binary(zlib:inflate(Zlib, Data)),
    zlib:inflateEnd(Zlib),
    case rfc4627:decode(Data2) of
        {ok, {obj, Args}, _} ->
            {ok, Args};
        _ ->
            throw("Received invalid json object for function arguments")
    end.

encode_args(Zlib, Data) ->
    EncData = rfc4627:encode(Data),
    zlib:deflateInit(Zlib),
    CompData = list_to_binary(zlib:deflate(Zlib, EncData, finish)),
    zlib:deflateEnd(Zlib),
    CompData.
