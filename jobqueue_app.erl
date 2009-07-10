-module(jobqueue_app).
-author('Samuel Stauffer <samuel@lefora.com>').

-export([start/0, start/2, stop/0]).

start() ->
    start([{"127.0.0.1", 4730}], 1).
start(Servers, NumWorkers) ->
    jobqueue:start(),
    start_workers(lists:flatten(lists:duplicate(NumWorkers, Servers))).

start_workers([]) ->
    [];
start_workers([Server|Servers]) ->
    [gearman_worker:start(Server, [jobqueue_gearman])|start_workers(Servers)].

stop() ->
    jobqueue:stop().
