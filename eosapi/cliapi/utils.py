#!/usr/bin/env python
# -*- coding:utf-8 -*
import inspect
import sys
import os


class Utils(object):
    Debug = False
    FNull = open(os.devnull, 'w')

    EosClientPath = "programs/cleos/cleos"

    EosWalletName = "keosd"
    EosWalletPath = "programs/keosd/" + EosWalletName

    EosServerName = "nodeos"
    EosServerPath = "programs/nodeos/" + EosServerName

    EosLauncherPath = "programs/eosio-launcher/eosio-launcher"
    MongoPath = "mongo"

    SyncStrategy = namedtuple("ChainSyncStrategy", "name id arg")

    SyncNoneTag = "none"
    SyncReplayTag = "replay"
    SyncResyncTag = "resync"

    SigKillTag = "kill"
    SigTermTag = "term"

    systemWaitTimeout = 90

    # mongoSyncTime: nodeos mongodb plugin seems to sync with a 10-15 seconds delay.
    # This will inject a wait period before the 2nd DB check (if first check fails)
    mongoSyncTime = 25

    @staticmethod
    def Print(*args, **kwargs):
        stackDepth = len(inspect.stack()) - 2
        s = ' ' * stackDepth
        sys.stdout.write(s)
        print(*args, **kwargs)

    @staticmethod
    def setMongoSyncTime(syncTime):
        Utils.mongoSyncTime = syncTime

    @staticmethod
    def setSystemWaitTimeout(timeout):
        Utils.systemWaitTimeout = timeout

    @staticmethod
    def getChainStrategies():
        chainSyncStrategies = {}

        chainSyncStrategy = Utils.SyncStrategy(Utils.SyncNoneTag, 0, "")
        chainSyncStrategies[chainSyncStrategy.name] = chainSyncStrategy

        chainSyncStrategy = Utils.SyncStrategy(Utils.SyncReplayTag, 1, "--replay-blockchain")
        chainSyncStrategies[chainSyncStrategy.name] = chainSyncStrategy

        chainSyncStrategy = Utils.SyncStrategy(Utils.SyncResyncTag, 2, "--resync-blockchain")
        chainSyncStrategies[chainSyncStrategy.name] = chainSyncStrategy

        return chainSyncStrategies

    @staticmethod
    def checkOutput(cmd):
        assert (isinstance(cmd, list))
        # retStr=subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8")
        retStr = subprocess.check_output(cmd).decode("utf-8")
        return retStr

    @staticmethod
    def errorExit(msg="", raw=False, errorCode=1):
        Utils.Print("ERROR:" if not raw else "", msg)
        exit(errorCode)

    @staticmethod
    def waitForObj(lam, timeout=None):
        if timeout is None:
            timeout = 60

        endTime = time.time() + timeout
        while endTime > time.time():
            ret = lam()
            if ret is not None:
                return ret
            sleepTime = 3
            Utils.Print("cmd: sleep %d seconds, remaining time: %d seconds" %
                        (sleepTime, endTime - time.time()))
            time.sleep(sleepTime)

        return None

    @staticmethod
    def waitForBool(lam, timeout=None):
        myLam = lambda: True if lam() else None
        ret = Utils.waitForObj(myLam, timeout)
        return False if ret is None else ret
