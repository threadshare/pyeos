#!/usr/bin/env python
# -*- coding:utf-8 -*
import os
from collections import namedtuple

Debug = False
FNull = open(os.devnull, 'w')

EosClientPath = "programs/cleos/cleos"

EosWalletName = "keosd"
EosWalletPath = "programs/keosd/" + EosWalletName

EosServerName = "nodeos"
EosServerPath = "programs/nodeos/" + EosServerName

EosLauncherPath = "programs/eosio-launcher/eosio-launcher"
MongoPath = "mongo"

SyncStrategy=namedtuple("ChainSyncStrategy", "name id arg")

SyncNoneTag="none"
SyncReplayTag="replay"
SyncResyncTag="resync"

SigKillTag="kill"
SigTermTag="term"

systemWaitTimeout=90

# mongoSyncTime: nodeos mongodb plugin seems to sync with a 10-15 seconds delay. This will inject
#  a wait period before the 2nd DB check (if first check fails)
mongoSyncTime=25
