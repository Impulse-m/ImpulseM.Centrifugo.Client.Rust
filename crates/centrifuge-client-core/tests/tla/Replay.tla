------------------------------- MODULE Replay -------------------------------
(***************************************************************************)
(* Trace generation for replaying the SDK model into centrifuge-client-core      *)
(* (docs/spec.md §11.1). It extends the model unchanged and only narrows  *)
(* which of its behaviours are generated, to the schedules this SDK        *)
(* implements:                                                             *)
(*                                                                         *)
(* - A subscribe flow starts in the step that makes it possible, once the  *)
(*   client is connected (no optimistic subscriptions in phase 1).         *)
(* - Queued deliveries (recovered publications, server-side subscriptions  *)
(*   of a connect reply) are handed over before anything else is read.    *)
(* - Callbacks don't settle in the window where the model would send a     *)
(*   subscribe before the connect reply (optimistic).                     *)
(* - WebSocket write errors close the transport, so the model's per-       *)
(*   command write error exists only over emulation.                       *)
(*                                                                         *)
(* Every generated behaviour is a behaviour of SDK.tla: Init and every    *)
(* step are the model's own.                                               *)
(***************************************************************************)
EXTENDS SDK

StartFlowNow == cl.st = "connected" /\ \E c \in Chans : Sdk_StartFlow(c)

DeliverNow ==
    \/ \E c \in Chans : Sdk_Deliver(c)
    \/ \E c \in ServerChans : Sdk_SSSubscribed(c) \/ Sdk_SSDeliver(c)

Allowed ==
    /\ act' # "Sdk_StartFlow"
    /\ act' \in {"Cb_GetState", "Cb_SubToken"} => ~(cl.st = "connecting" /\ cl.topen)
    /\ Ordered => act' # "Env_WriteError"

ReplayNext ==
    IF ENABLED StartFlowNow THEN StartFlowNow
    ELSE IF ENABLED DeliverNow THEN DeliverNow
    ELSE Next /\ Allowed

ReplaySpec == Init /\ [][ReplayNext]_vars

=============================================================================
