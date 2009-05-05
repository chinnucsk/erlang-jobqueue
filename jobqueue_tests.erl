-module(jobqueue_tests).
-author('Samuel Stauffer <samuel@descolada.com>').

-export([test/0]).
% -import(jobqueue, [start/0, ]).

test() ->
    jobqueue:start(),
    {ok, JobID} = jobqueue:insert_job("test", <<"foo">>, "", 0, 0, ""),
    io:format("Created job ~p~n", [JobID]),
    Job = jobqueue:find_job(["test"]),
    io:format("Grabbed job ~p~n", [Job]),
    jobqueue:stop().
