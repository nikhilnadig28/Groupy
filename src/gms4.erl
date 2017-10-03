-module(gms4).
-export([start/1, start/2, init/3, init/4]).

-define(timeout, 1000).
-define(arghh, 100).

start(Id) ->
    Rnd = random:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun()-> init(Id, Rnd, Self) end)}.
 
init(Id, Rnd, Master) ->
    random:seed(Rnd, Rnd, Rnd),
    leader(Id, Master,1, [], [Master],[]).
 
start(Id, Grp) ->
    Rnd = random:uniform(1000),
    Self = self(),
    {ok, spawn_link(fun()-> init(Id, Rnd, Grp, Self) end)}.
 
init(Id, Rnd, Grp, Master) ->
    random:seed(Rnd, Rnd, Rnd),
 
    Self = self(),
    Grp ! {join, Master, Self},
 
    receive
        {view, N, [Leader|Slaves], Group} ->
            Master ! {view, Group},
            erlang:monitor(process, Leader),
            slave(Id, Master, Leader, N + 1, {view, N, [Leader|Slaves], Group}, Slaves, Group)
 
    after timeout ->
        Master ! {error, "no reply from leader"}
    end.
 
 
leader(Id, Master,N, Slaves, Group, MsgList) ->
    receive
        {mcast, Msg} ->          
            bcast(Id, {msg, N, Msg}, Slaves),
            Master ! Msg,
            NewMsgList = [{N,Msg}|MsgList], %%store all multicasted messages
            leader(Id, Master, N+1, Slaves, Group, NewMsgList);
        {join, Wrk, Peer} ->
 
            NextSlave = lists:append(Slaves, [Peer]),
            NextGroup = lists:append(Group, [Wrk]),
            bcast(Id, {view, N, [self()|NextSlave], NextGroup}, NextSlave),
            Master ! {view, NextGroup},
            leader(Id, Master, N + 1, NextSlave, NextGroup, MsgList);
        {resendMessage, I, P} ->         
            case lists:keyfind(I, 1, MsgList) of 
                {S,Message} ->
                    io:format("Leader resending messages~n"),
                    P ! {resend, S,Message},
                    leader(Id, Master, N, Slaves, Group, MsgList);
                false ->
                    io:format("Message not found in leader's List~n"),
                    leader(Id, Master, N, Slaves, Group, MsgList)
                end;
        stop ->
            ok
    end.
 
 
slave(Id, Master, Leader, N, Last, Slaves, Group) ->  
    receive
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, N, Last, Slaves, Group);
        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, N, Last, Slaves, Group);      
        {msg, I, _} when I < N ->             
            slave(Id, Master, Leader, N, Last, Slaves, Group);
        {msg, I, Msg} when I > N ->           
            MessageSequence = lists:seq(N, I),            
            io:format("Msg with ID ~w lost by slave: ~w~n",[MessageSequence,Id]),
            lists:map(fun(Missing) -> Leader ! {resendMessage, Missing, self()}  end, MessageSequence),
            Master ! Msg,
            slave(Id, Master, Leader, I + 1, {msg,I,Msg}, Slaves, Group);
        {msg, I, Msg} ->
            Master ! Msg,
            slave(Id, Master, Leader, I + 1, {msg,I,Msg}, Slaves, Group);         
        {view, I, [Leader|Slaves2], NextGroup} ->
            Master ! {view, NextGroup},
            slave(Id, Master, Leader, I + 1, {view, I, [Leader|Slaves2], NextGroup}, Slaves2, NextGroup);
        {'DOWN', _Ref, process, Leader, _Reason} ->          
            election(Id, Master, N, Last, Slaves, Group);
        {resend, S,_} ->         
            io:format("Lost message with seq numbers ~w received again by slave: ~w~n",[S,Id]),
            slave(Id, Master, Leader, N, Last, Slaves, Group);
        stop ->
            ok
end.
 
bcast(Id, Msg, Nodes) ->
    lists:foreach(fun(Node) ->    
    dropMessage(Node, Msg) end, Nodes).

crash(Id) ->
    case random:uniform(?arghh) of
        ?arghh ->
            io:format("leader ~w: crash~n", [Id]),
            exit(no_luck);
        _ ->
            ok
    end.
 
dropMessage(Node, Msg) ->
    case random:uniform(?arghh) of
        ?arghh ->
            io:format("Message dropped for node ~w~n", [Node]),
            drop;
        _ ->
            Node ! Msg
    end.
 
 
election(Id, Master,N, Last, Slaves, [_|Group]) ->
    Self = self(),
    case Slaves of
        [Self|Rest] ->
            bcast(Id, Last, Rest),
            bcast(Id, {view,N,Slaves,Group},Rest),
            Master ! {view, Group},
            leader(Id, Master, N + 1, Rest, Group,[]);
        [Leader|Rest] ->
            erlang:monitor(process, Leader),
            slave(Id, Master, Leader, N, Last, Rest, Group)
    end.
