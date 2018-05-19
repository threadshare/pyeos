#!/usr/bin/env python
# -*- coding:utf-8 -*
import datetime
import decimal
import json
import os
import re
import shlex
import subprocess
import time

from eosapi.cliapi.utils import Utils
from eosapi.cliapi.account import Account
from . import CORE_SYMBOL


class Node(object):

    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-many-arguments
    def __init__(self,
                 host,
                 port,
                 pid=None,
                 cmd=None,
                 enableMongo=False,
                 mongoHost="localhost",
                 mongoPort=27017,
                 mongoDb="EOStest"):
        self.host = host
        self.port = port
        self.pid = pid
        self.cmd = cmd
        self.killed = False  # marks node as killed
        self.enableMongo = enableMongo
        self.mongoSyncTime = None if Utils.mongoSyncTime < 1 else Utils.mongoSyncTime
        self.mongoHost = mongoHost
        self.mongoPort = mongoPort
        self.mongoDb = mongoDb
        self.endpointArgs = "--url http://%s:%d" % (self.host, self.port)
        self.mongoEndpointArgs = ""
        if self.enableMongo:
            self.mongoEndpointArgs += "--host %s --port %d %s" % (mongoHost, mongoPort, mongoDb)

    def __str__(self):
        return "Host: %s, Port:%d" % (self.host, self.port)

    @staticmethod
    def validateTransaction(trans):
        assert trans
        assert isinstance(trans, dict), print("Input type is %s" % type(trans))

        def printTrans(trans):
            Utils.Print("ERROR: Failure in transaction validation.")
            Utils.Print("Transaction: %s" % (json.dumps(trans, indent=1)))

        assert trans["processed"]["receipt"]["status"] == "executed", printTrans(trans)

    @staticmethod
    def runCmdReturnJson(cmd, trace=False):
        cmdArr = shlex.split(cmd)
        retStr = Utils.checkOutput(cmdArr)
        jStr = Node.filterJsonObject(retStr)
        if trace: Utils.Print("RAW > %s" % (retStr))
        if trace: Utils.Print("JSON> %s" % (jStr))
        if not jStr:
            msg = "Expected JSON response"
            Utils.Print("ERROR: " + msg)
            Utils.Print("RAW > %s" % retStr)
            raise TypeError(msg)

        try:
            jsonData = json.loads(jStr)
            return jsonData
        except json.decoder.JSONDecodeError as ex:
            Utils.Print(ex)
            Utils.Print("RAW > %s" % retStr)
            Utils.Print("JSON> %s" % jStr)
            raise

    # @staticmethod
    # def __runCmdArrReturnJson(cmdArr, trace=False):
    #     retStr = Utils.checkOutput(cmdArr)
    #     jStr = Node.filterJsonObject(retStr)
    #     if trace: Utils.Print("RAW > %s" % (retStr))
    #     if trace: Utils.Print("JSON> %s" % (jStr))
    #     jsonData = json.loads(jStr)
    #     return jsonData

    @staticmethod
    def runCmdReturnStr(cmd, trace=False):
        cmdArr = shlex.split(cmd)
        retStr = Utils.checkOutput(cmdArr)
        if trace: Utils.Print("RAW > %s" % (retStr))
        return retStr

    @staticmethod
    def filterJsonObject(data):
        firstIdx = data.find('{')
        lastIdx = data.rfind('}')
        retStr = data[firstIdx:lastIdx + 1]
        return retStr

    # @staticmethod
    # def __checkOutput(cmd):
    #     retStr = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8")
    #     # retStr=subprocess.check_output(cmd).decode("utf-8")
    #     return retStr

    # Passes input to stdin, executes cmd. Returns tuple with return code(int),
    #  stdout(byte stream) and stderr(byte stream).
    @staticmethod
    def stdinAndCheckOutput(cmd, subcommand):
        outs = None
        errs = None
        ret = 0
        try:
            popen = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            outs, errs = popen.communicate(input=subcommand.encode("utf-8"))
            ret = popen.wait()
        except subprocess.CalledProcessError as ex:
            msg = ex.output
            return ex.returncode, msg, None

        return ret, outs, errs

    @staticmethod
    def normalizeJsonObject(extJStr):
        tmpStr = extJStr
        tmpStr = re.sub(r'ObjectId\("(\w+)"\)', r'"ObjectId-\1"', tmpStr)
        tmpStr = re.sub(r'ISODate\("([\w|\-|\:|\.]+)"\)', r'"ISODate-\1"', tmpStr)
        return tmpStr

    @staticmethod
    def runMongoCmdReturnJson(cmdArr, subcommand, trace=False):
        retId, outs, _ = Node.stdinAndCheckOutput(cmdArr, subcommand)
        if retId is not 0:
            return None
        outStr = Node.byteArrToStr(outs)
        if not outStr:
            return None
        extJStr = Node.filterJsonObject(outStr)
        if not extJStr:
            return None
        jStr = Node.normalizeJsonObject(extJStr)
        if not jStr:
            return None
        if trace: Utils.Print("RAW > %s" % (outStr))
        # trace and Utils.Print ("JSON> %s"% jStr)
        jsonData = json.loads(jStr)
        return jsonData

    @staticmethod
    def getTransId(trans):
        """Retrieve transaction id from dictionary object."""
        assert trans
        assert isinstance(trans, dict), print("Input type is %s" % type(trans))

        transId = trans["transaction_id"]
        return transId

    @staticmethod
    def byteArrToStr(arr):
        return arr.decode("utf-8")

    def setWalletEndpointArgs(self, args):
        self.endpointArgs = "--url http://%s:%d %s" % (self.host, self.port, args)

    def validateAccounts(self, accounts):
        assert (accounts)
        assert (isinstance(accounts, list))

        for account in accounts:
            assert (account)
            assert (isinstance(account, Account))
            if Utils.Debug: Utils.Print("Validating account %s" % (account.name))
            accountInfo = self.getEosAccount(account.name)
            try:
                assert (accountInfo)
                assert (accountInfo["account_name"] == account.name)
            except (AssertionError, TypeError, KeyError) as _:
                Utils.Print("account validation failed. account: %s" % (account.name))
                raise

    # pylint: disable=too-many-branches
    def getBlock(self, blockNum, retry=True, silentErrors=False):
        """Given a blockId will return block details."""
        assert (isinstance(blockNum, str))
        if not self.enableMongo:
            cmd = "%s %s get block %s" % (Utils.EosClientPath, self.endpointArgs, blockNum)
            if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
            try:
                trans = Node.runCmdReturnJson(cmd)
                return trans
            except subprocess.CalledProcessError as ex:
                if not silentErrors:
                    msg = ex.output.decode("utf-8")
                    Utils.Print("ERROR: Exception during get block. %s" % (msg))
                return None
        else:
            for _ in range(2):
                cmd = "%s %s" % (Utils.MongoPath, self.mongoEndpointArgs)
                subcommand = 'db.Blocks.findOne( { "block_num": %s } )' % (blockNum)
                if Utils.Debug: Utils.Print("cmd: echo '%s' | %s" % (subcommand, cmd))
                try:
                    trans = Node.runMongoCmdReturnJson(cmd.split(), subcommand)
                    if trans is not None:
                        return trans
                except subprocess.CalledProcessError as ex:
                    if not silentErrors:
                        msg = ex.output.decode("utf-8")
                        Utils.Print("ERROR: Exception during get db node get block. %s" % (msg))
                    return None
                if not retry:
                    break
                if self.mongoSyncTime is not None:
                    if Utils.Debug: Utils.Print("cmd: sleep %d seconds" % (self.mongoSyncTime))
                    time.sleep(self.mongoSyncTime)

        return None

    def getBlockById(self, blockId, retry=True, silentErrors=False):
        for _ in range(2):
            cmd = "%s %s" % (Utils.MongoPath, self.mongoEndpointArgs)
            subcommand = 'db.Blocks.findOne( { "block_id": "%s" } )' % (blockId)
            if Utils.Debug: Utils.Print("cmd: echo '%s' | %s" % (subcommand, cmd))
            try:
                trans = Node.runMongoCmdReturnJson(cmd.split(), subcommand)
                if trans is not None:
                    return trans
            except subprocess.CalledProcessError as ex:
                if not silentErrors:
                    msg = ex.output.decode("utf-8")
                    Utils.Print("ERROR: Exception during db get block by id. %s" % (msg))
                return None
            if not retry:
                break
            if self.mongoSyncTime is not None:
                if Utils.Debug: Utils.Print("cmd: sleep %d seconds" % (self.mongoSyncTime))
                time.sleep(self.mongoSyncTime)

        return None

    def doesNodeHaveBlockNum(self, blockNum):
        assert isinstance(blockNum, int)
        assert (blockNum > 0)

        info = self.getInfo(silentErrors=True)
        assert (info)
        last_irreversible_block_num = 0
        try:
            last_irreversible_block_num = int(info["last_irreversible_block_num"])
        except (TypeError, KeyError) as _:
            Utils.Print("Failure in get info parsing. %s" % (info))
            raise

        return True if blockNum <= last_irreversible_block_num else True

    # pylint: disable=too-many-branches
    def getTransaction(self, transId, retry=True, silentErrors=False):
        if not self.enableMongo:
            cmd = "%s %s get transaction %s" % (Utils.EosClientPath, self.endpointArgs, transId)
            if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
            try:
                trans = Node.runCmdReturnJson(cmd)
                return trans
            except subprocess.CalledProcessError as ex:
                msg = ex.output.decode("utf-8")
                if "Failed to connect" in msg:
                    Utils.Print("ERROR: Node is unreachable. %s" % (msg))
                    raise
                if not silentErrors:
                    Utils.Print("ERROR: Exception during transaction retrieval. %s" % (msg))
                return None
        else:
            for _ in range(2):
                cmd = "%s %s" % (Utils.MongoPath, self.mongoEndpointArgs)
                subcommand = 'db.Transactions.findOne( { $and : [ { "transaction_id": "%s" }, {"pending":false} ] } )' % (
                    transId)
                if Utils.Debug: Utils.Print("cmd: echo '%s' | %s" % (subcommand, cmd))
                try:
                    trans = Node.runMongoCmdReturnJson(cmd.split(), subcommand)
                    return trans
                except subprocess.CalledProcessError as ex:
                    if not silentErrors:
                        msg = ex.output.decode("utf-8")
                        Utils.Print("ERROR: Exception during get db node get trans. %s" % (msg))
                    return None
                if not retry:
                    break
                if self.mongoSyncTime is not None:
                    if Utils.Debug: Utils.Print("cmd: sleep %d seconds" % (self.mongoSyncTime))
                    time.sleep(self.mongoSyncTime)

        return None

    def isTransInBlock(self, transId, blockId):
        """Check if transId is within block identified by blockId"""
        assert (transId)
        assert (isinstance(transId, str))
        assert (blockId)
        assert (isinstance(blockId, str))

        block = self.getBlock(blockId)
        transactions = None
        try:
            transactions = block["transactions"]
        except (AssertionError, TypeError, KeyError) as _:
            Utils.Print("Failed to parse block. %s" % (block))
            raise

        if transactions is not None:
            for trans in transactions:
                assert (trans)
                try:
                    myTransId = trans["trx"]["id"]
                    if transId == myTransId:
                        return True
                except (TypeError, KeyError) as _:
                    Utils.Print("Failed to parse block transactions. %s" % (trans))

        return False

    def getBlockIdByTransId(self, transId):
        """Given a transaction Id (string), will return block id (string) containing the transaction"""
        assert (transId)
        assert (isinstance(transId, str))
        trans = self.getTransaction(transId)
        assert (trans)

        refBlockNum = None
        try:
            refBlockNum = trans["trx"]["trx"]["ref_block_num"]
            refBlockNum = int(refBlockNum) + 1
        except (TypeError, ValueError, KeyError) as _:
            Utils.Print("transaction parsing failed. Transaction: %s" % (trans))
            raise

        headBlockNum = self.getIrreversibleBlockNum()
        assert (headBlockNum)
        try:
            headBlockNum = int(headBlockNum)
        except(ValueError) as _:
            Utils.Print("Info parsing failed. %s" % (headBlockNum))

        for blockNum in range(refBlockNum, headBlockNum + 1):
            if self.isTransInBlock(str(transId), str(blockNum)):
                return str(blockNum)

        return None

    def doesNodeHaveTransId(self, transId):
        """Check if transaction (transId) has been finalized."""
        assert (transId)
        assert (isinstance(transId, str))
        blockId = self.getBlockIdByTransId(transId)
        return True if blockId else None

    def getTransByBlockId(self, blockId, retry=True, silentErrors=False):
        for _ in range(2):
            cmd = "%s %s" % (Utils.MongoPath, self.mongoEndpointArgs)
            subcommand = 'db.Transactions.find( { "block_id": "%s" } )' % (blockId)
            if Utils.Debug: Utils.Print("cmd: echo '%s' | %s" % (subcommand, cmd))
            try:
                trans = Node.runMongoCmdReturnJson(cmd.split(), subcommand, True)
                if trans is not None:
                    return trans
            except subprocess.CalledProcessError as ex:
                if not silentErrors:
                    msg = ex.output.decode("utf-8")
                    Utils.Print("ERROR: Exception during db get trans by blockId. %s" % (msg))
                return None
            if not retry:
                break
            if self.mongoSyncTime is not None:
                if Utils.Debug: Utils.Print("cmd: sleep %d seconds" % (self.mongoSyncTime))
                time.sleep(self.mongoSyncTime)

        return None

    def getActionFromDb(self, transId, retry=True, silentErrors=False):
        for _ in range(2):
            cmd = "%s %s" % (Utils.MongoPath, self.mongoEndpointArgs)
            subcommand = 'db.Actions.findOne( { "transaction_id": "%s" } )' % (transId)
            if Utils.Debug: Utils.Print("cmd: echo '%s' | %s" % (subcommand, cmd))
            try:
                trans = Node.runMongoCmdReturnJson(cmd.split(), subcommand)
                if trans is not None:
                    return trans
            except subprocess.CalledProcessError as ex:
                if not silentErrors:
                    msg = ex.output.decode("utf-8")
                    Utils.Print("ERROR: Exception during get db node get message. %s" % (msg))
                return None
            if not retry:
                break
            if self.mongoSyncTime is not None:
                if Utils.Debug: Utils.Print("cmd: sleep %d seconds" % (self.mongoSyncTime))
                time.sleep(self.mongoSyncTime)

        return None

    def getMessageFromDb(self, transId, retry=True, silentErrors=False):
        for _ in range(2):
            cmd = "%s %s" % (Utils.MongoPath, self.mongoEndpointArgs)
            subcommand = 'db.Messages.findOne( { "transaction_id": "%s" } )' % (transId)
            if Utils.Debug: Utils.Print("cmd: echo '%s' | %s" % (subcommand, cmd))
            try:
                trans = Node.runMongoCmdReturnJson(cmd.split(), subcommand)
                if trans is not None:
                    return trans
            except subprocess.CalledProcessError as ex:
                if not silentErrors:
                    msg = ex.output.decode("utf-8")
                    Utils.Print("ERROR: Exception during get db node get message. %s" % (msg))
                return None
            if not retry:
                break
            if self.mongoSyncTime is not None:
                if Utils.Debug: Utils.Print("cmd: sleep %d seconds" % (self.mongoSyncTime))
                time.sleep(self.mongoSyncTime)

        return None

    # Create & initialize account and return creation transactions. Return transaction json object
    def createInitializeAccount(self, account, creatorAccount, stakedDeposit=1000, waitForTransBlock=False):
        cmd = '%s %s system newaccount -j %s %s %s %s --stake-net "100 %s" --stake-cpu "100 %s" --buy-ram-EOS "100 %s"' % (
            Utils.EosClientPath, self.endpointArgs, creatorAccount.name, account.name,
            account.ownerPublicKey, account.activePublicKey,
            CORE_SYMBOL, CORE_SYMBOL, CORE_SYMBOL)

        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        trans = None
        try:
            trans = Node.runCmdReturnJson(cmd)
            transId = Node.getTransId(trans)
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during account creation. %s" % (msg))
            return None

        if stakedDeposit > 0:
            self.waitForTransIdOnNode(
                transId)  # seems like account creation needs to be finlized before transfer can happen
            trans = self.transferFunds(creatorAccount, account, "%0.04f %s" % (stakedDeposit / 10000, CORE_SYMBOL),
                                       "init")
            transId = Node.getTransId(trans)

        if waitForTransBlock and not self.waitForTransIdOnNode(transId):
            return None

        return trans

    # Create account and return creation transactions. Return transaction json object
    # waitForTransBlock: wait on creation transaction id to appear in a block
    def createAccount(self, account, creatorAccount, stakedDeposit=1000, waitForTransBlock=False):
        cmd = "%s %s create account -j %s %s %s %s" % (
            Utils.EosClientPath, self.endpointArgs, creatorAccount.name, account.name,
            account.ownerPublicKey, account.activePublicKey)

        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        trans = None
        try:
            trans = Node.runCmdReturnJson(cmd)
            transId = Node.getTransId(trans)
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during account creation. %s" % (msg))
            return None

        if stakedDeposit > 0:
            self.waitForTransIdOnNode(
                transId)  # seems like account creation needs to be finlized before transfer can happen
            trans = self.transferFunds(creatorAccount, account, "%0.04f %s" % (stakedDeposit / 10000, CORE_SYMBOL),
                                       "init")
            transId = Node.getTransId(trans)

        if waitForTransBlock and not self.waitForTransIdOnNode(transId):
            return None

        return trans

    def getEosAccount(self, name):
        assert (isinstance(name, str))
        cmd = "%s %s get account -j %s" % (Utils.EosClientPath, self.endpointArgs, name)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        try:
            trans = Node.runCmdReturnJson(cmd)
            return trans
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during get account. %s" % (msg))
            return None

    def getEosAccountFromDb(self, name):
        cmd = "%s %s" % (Utils.MongoPath, self.mongoEndpointArgs)
        subcommand = 'db.Accounts.findOne({"name" : "%s"})' % (name)
        if Utils.Debug: Utils.Print("cmd: echo '%s' | %s" % (subcommand, cmd))
        try:
            trans = Node.runMongoCmdReturnJson(cmd.split(), subcommand)
            return trans
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during get account from db. %s" % (msg))
            return None

    def getTable(self, contract, scope, table):
        cmd = "%s %s get table %s %s %s" % (Utils.EosClientPath, self.endpointArgs, contract, scope, table)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        try:
            trans = Node.runCmdReturnJson(cmd)
            return trans
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during table retrieval. %s" % (msg))
            return None

    # def getNodeAccountEosBalance(self, scope):
    def getNodeAccountBalance(self, contract, scope):
        assert (isinstance(contract, str))
        assert (isinstance(scope, str))
        table = "accounts"
        trans = self.getTable(contract, scope, table)
        assert (trans)
        try:
            return trans["rows"][0]["balance"]
        except (TypeError, KeyError) as _:
            print("Transaction parsing failed. Transaction: %s" % (trans))
            raise

    def getCurrencyStats(self, contract, symbol=""):
        cmd = "%s %s get currency0000 stats %s %s" % (Utils.EosClientPath, self.endpointArgs, contract, symbol)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        try:
            trans = Node.runCmdReturnJson(cmd)
            return trans
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during get currency0000 stats. %s" % (msg))
            return None

    # Verifies account. Returns "get account" json return object
    def verifyAccount(self, account):
        if not self.enableMongo:
            ret = self.getEosAccount(account.name)
            if ret is not None:
                account_name = ret["account_name"]
                if account_name is None:
                    Utils.Print("ERROR: Failed to verify account creation.", account.name)
                    return None
                return ret
        else:
            for _ in range(2):
                ret = self.getEosAccountFromDb(account.name)
                if ret is not None:
                    account_name = ret["name"]
                    if account_name is None:
                        Utils.Print("ERROR: Failed to verify account creation.", account.name)
                        return None
                    return ret
                if self.mongoSyncTime is not None:
                    if Utils.Debug: Utils.Print("cmd: sleep %d seconds" % (self.mongoSyncTime))
                    time.sleep(self.mongoSyncTime)

        return None

    def waitForBlockNumOnNode(self, blockNum, timeout=None):
        lam = lambda: self.doesNodeHaveBlockNum(blockNum)
        ret = Utils.waitForBool(lam, timeout)
        return ret

    def waitForTransIdOnNode(self, transId, timeout=None):
        lam = lambda: self.doesNodeHaveTransId(transId)
        ret = Utils.waitForBool(lam, timeout)
        return ret

    def waitForNextBlock(self, timeout=None):
        num = self.getIrreversibleBlockNum()
        lam = lambda: self.getIrreversibleBlockNum() > num
        ret = Utils.waitForBool(lam, timeout)
        return ret

    # Trasfer funds. Returns "transfer" json return object
    def transferFunds(self, source, destination, amount, memo="memo", force=False):
        assert isinstance(amount, str)

        cmd = "%s %s -v transfer -j %s %s" % (
            Utils.EosClientPath, self.endpointArgs, source.name, destination.name)
        cmdArr = cmd.split()
        cmdArr.append(amount)
        cmdArr.append(memo)
        if force:
            cmdArr.append("-f")
        s = " ".join(cmdArr)
        if Utils.Debug: Utils.Print("cmd: %s" % (s))
        trans = None
        try:
            trans = Node.__runCmdArrReturnJson(cmdArr)
            return trans
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during funds transfer. %s" % (msg))
            return None

    def validateSpreadFundsOnNode(self, adminAccount, accounts, expectedTotal):
        actualTotal = self.getAccountEosBalance(adminAccount.name)
        for account in accounts:
            fund = self.getAccountEosBalance(account.name)
            if fund != account.balance:
                Utils.Print("ERROR: validateSpreadFunds> Expected: %d, actual: %d for account %s" %
                            (account.balance, fund, account.name))
                return False
            actualTotal += fund

        if actualTotal != expectedTotal:
            Utils.Print("ERROR: validateSpreadFunds> Expected total: %d , actual: %d" % (
                expectedTotal, actualTotal))
            return False

        return True

    def getSystemBalance(self, adminAccount, accounts):
        balance = self.getAccountEosBalance(adminAccount.name)
        for account in accounts:
            balance += self.getAccountEosBalance(account.name)
        return balance

    # Gets accounts mapped to key. Returns json object
    def getAccountsByKey(self, key):
        cmd = "%s %s get accounts %s" % (Utils.EosClientPath, self.endpointArgs, key)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        try:
            trans = Node.runCmdReturnJson(cmd)
            return trans
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during accounts by key retrieval. %s" % (msg))
            return None

    # Get actions mapped to an account (cleos get actions)
    def getActions(self, account, pos=-1, offset=-1):
        assert (isinstance(account, Account))
        assert (isinstance(pos, int))
        assert (isinstance(offset, int))

        cmd = "%s %s get actions -j %s %d %d" % (Utils.EosClientPath, self.endpointArgs, account.name, pos, offset)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        try:
            actions = Node.runCmdReturnJson(cmd)
            return actions
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during actions by account retrieval. %s" % (msg))
            return None

    # Gets accounts mapped to key. Returns array
    def getAccountsArrByKey(self, key):
        trans = self.getAccountsByKey(key)
        assert (trans)
        assert ("account_names" in trans)
        accounts = trans["account_names"]
        return accounts

    def getServants(self, name):
        cmd = "%s %s get servants %s" % (Utils.EosClientPath, self.endpointArgs, name)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        try:
            trans = Node.runCmdReturnJson(cmd)
            return trans
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during servants retrieval. %s" % (msg))
            return None

    def getServantsArr(self, name):
        trans = self.getServants(name)
        servants = trans["controlled_accounts"]
        return servants

    def getAccountEosBalanceStr(self, scope):
        """Returns EOS currency0000 account balance from cleos get table command. Returned balance is string following syntax "98.0311 SYS". """
        assert isinstance(scope, str)
        if not self.enableMongo:
            amount = self.getNodeAccountBalance("eosio.token", scope)
            if Utils.Debug: Utils.Print("getNodeAccountEosBalance %s %s" % (scope, amount))
            assert isinstance(amount, str)
            return amount
        else:
            if self.mongoSyncTime is not None:
                if Utils.Debug: Utils.Print("cmd: sleep %d seconds" % (self.mongoSyncTime))
                time.sleep(self.mongoSyncTime)

            account = self.getEosAccountFromDb(scope)
            if account is not None:
                balance = account["eos_balance"]
                return balance

        return None

    def getAccountEosBalance(self, scope):
        """Returns EOS currency0000 account balance from cleos get table command. Returned balance is an integer e.g. 980311. """
        balanceStr = self.getAccountEosBalanceStr(scope)
        balanceStr = balanceStr.split()[0]
        balance = int(decimal.Decimal(balanceStr[1:]) * 10000)
        return balance

    # transactions lookup by id. Returns json object
    def getTransactionsByAccount(self, name):
        cmd = "%s %s get transactions -j %s" % (Utils.EosClientPath, self.endpointArgs, name)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        try:
            trans = Node.runCmdReturnJson(cmd)
            return trans
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during transactions by account retrieval. %s" % (msg))
            return None

    # transactions lookup by id. Returns list of transaction ids
    def getTransactionsArrByAccount(self, name):
        trans = self.getTransactionsByAccount(name)
        transactions = trans["transactions"]
        transArr = []
        for transaction in transactions:
            transId = transaction["transaction_id"]
            transArr.append(transId)
        return transArr

    def getAccountCodeHash(self, account):
        cmd = "%s %s get code %s" % (Utils.EosClientPath, self.endpointArgs, account)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        try:
            retStr = Utils.checkOutput(cmd.split())
            # Utils.Print ("get code> %s"% retStr)
            p = re.compile(r'code\shash: (\w+)\n', re.MULTILINE)
            m = p.search(retStr)
            if m is None:
                msg = "Failed to parse code hash."
                Utils.Print("ERROR: " + msg)
                return None

            return m.group(1)
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during code hash retrieval. %s" % (msg))
            return None

    # publish contract and return transaction as json object
    def publishContract(self, account, contractDir, wastFile, abiFile, waitForTransBlock=False, shouldFail=False):
        cmd = "%s %s -v set contract -j %s %s" % (Utils.EosClientPath, self.endpointArgs, account, contractDir)
        cmd += "" if wastFile is None else (" " + wastFile)
        cmd += "" if abiFile is None else (" " + abiFile)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        trans = None
        try:
            trans = Node.runCmdReturnJson(cmd, trace=False)
        except subprocess.CalledProcessError as ex:
            if not shouldFail:
                msg = ex.output.decode("utf-8")
                Utils.Print("ERROR: Exception during code hash retrieval. %s" % (msg))
                return None
            else:
                retMap = {}
                retMap["returncode"] = ex.returncode
                retMap["cmd"] = ex.cmd
                retMap["output"] = ex.output
                # commented below as they are available only in Python3.5 and above
                # retMap["stdout"]=ex.stdout
                # retMap["stderr"]=ex.stderr
                return retMap

        if shouldFail:
            Utils.Print("ERROR: The publish contract did not fail as expected.")
            return None

        Node.validateTransaction(trans)
        transId = Node.getTransId(trans)
        if waitForTransBlock and not self.waitForTransIdOnNode(transId):
            return None
        return trans

    def getTableRows(self, contract, scope, table):
        jsonData = self.getTable(contract, scope, table)
        if jsonData is None:
            return None
        rows = jsonData["rows"]
        return rows

    def getTableRow(self, contract, scope, table, idx):
        if idx < 0:
            Utils.Print("ERROR: Table index cannot be negative. idx: %d" % (idx))
            return None
        rows = self.getTableRows(contract, scope, table)
        if rows is None or idx >= len(rows):
            Utils.Print("ERROR: Retrieved table does not contain row %d" % idx)
            return None
        row = rows[idx]
        return row

    def getTableColumns(self, contract, scope, table):
        row = self.getTableRow(contract, scope, table, 0)
        keys = list(row.keys())
        return keys

    # returns tuple with transaction and
    def pushMessage(self, account, action, data, opts, silentErrors=False):
        cmd = "%s %s push action -j %s %s" % (Utils.EosClientPath, self.endpointArgs, account, action)
        cmdArr = cmd.split()
        if data is not None:
            cmdArr.append(data)
        if opts is not None:
            cmdArr += opts.split()
        s = " ".join(cmdArr)
        if Utils.Debug: Utils.Print("cmd: %s" % (s))
        try:
            trans = Node.__runCmdArrReturnJson(cmdArr)
            return (True, trans)
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            if not silentErrors:
                Utils.Print("ERROR: Exception during push message. %s" % (msg))
            return (False, msg)

    def setPermission(self, account, code, pType, requirement, waitForTransBlock=False):
        cmd = "%s %s set action permission -j %s %s %s %s" % (
            Utils.EosClientPath, self.endpointArgs, account, code, pType, requirement)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        trans = None
        try:
            trans = Node.runCmdReturnJson(cmd)
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during set permission. %s" % (msg))
            return None

        transId = Node.getTransId(trans)
        if waitForTransBlock and not self.waitForTransIdOnNode(transId):
            return None
        return trans

    def getInfo(self, silentErrors=False):
        cmd = "%s %s get info" % (Utils.EosClientPath, self.endpointArgs)
        if Utils.Debug: Utils.Print("cmd: %s" % (cmd))
        try:
            trans = Node.runCmdReturnJson(cmd)
            return trans
        except subprocess.CalledProcessError as ex:
            if not silentErrors:
                msg = ex.output.decode("utf-8")
                Utils.Print("ERROR: Exception during get info. %s" % (msg))
            return None

    def getBlockFromDb(self, idx):
        cmd = "%s %s" % (Utils.MongoPath, self.mongoEndpointArgs)
        subcommand = "db.Blocks.find().sort({\"_id\":%d}).limit(1).pretty()" % (idx)
        if Utils.Debug: Utils.Print("cmd: echo \"%s\" | %s" % (subcommand, cmd))
        try:
            trans = Node.runMongoCmdReturnJson(cmd.split(), subcommand)
            return trans
        except subprocess.CalledProcessError as ex:
            msg = ex.output.decode("utf-8")
            Utils.Print("ERROR: Exception during get db block. %s" % (msg))
            return None

    def checkPulse(self):
        info = self.getInfo(True)
        return False if info is None else True

    def getHeadBlockNum(self):
        """returns head block number(string) as returned by cleos get info."""
        if not self.enableMongo:
            info = self.getInfo()
            if info is not None:
                headBlockNumTag = "head_block_num"
                return info[headBlockNumTag]
        else:
            block = self.getBlockFromDb(-1)
            if block is not None:
                blockNum = block["block_num"]
                return blockNum
        return None

    def getIrreversibleBlockNum(self):
        if not self.enableMongo:
            info = self.getInfo()
            if info is not None:
                return info["last_irreversible_block_num"]
        else:
            block = self.getBlockFromDb(-1)
            if block is not None:
                blockNum = block["block_num"]
                return blockNum
        return None

    def kill(self, killSignal):
        if Utils.Debug: Utils.Print("Killing node: %s" % (self.cmd))
        assert (self.pid is not None)
        try:
            os.kill(self.pid, killSignal)
        except OSError as ex:
            Utils.Print("ERROR: Failed to kill node (%d)." % (self.cmd), ex)
            return False

        # wait for kill validation
        def myFunc():
            try:
                os.kill(self.pid, 0)  # check if process with pid is running
            except OSError as _:
                return True
            return False

        if not Utils.waitForBool(myFunc):
            Utils.Print("ERROR: Failed to kill node (%s)." % (self.cmd))
            return False

        # mark node as killed
        self.pid = None
        self.killed = True
        return True

    # TBD: make nodeId an internal property
    def relaunch(self, nodeId, chainArg):

        running = True
        try:
            os.kill(self.pid, 0)  # check if process with pid is running
        except OSError as _:
            running = False

        if running:
            Utils.Print("WARNING: A process with pid (%d) is already running." % (self.pid))
        else:
            if Utils.Debug: Utils.Print("Launching node process, Id: %d" % (nodeId))
            dataDir = "var/lib/node_%02d" % (nodeId)
            dt = datetime.datetime.now()
            dateStr = "%d_%02d_%02d_%02d_%02d_%02d" % (
                dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)
            stdoutFile = "%s/stdout.%s.txt" % (dataDir, dateStr)
            stderrFile = "%s/stderr.%s.txt" % (dataDir, dateStr)
            with open(stdoutFile, 'w') as sout, open(stderrFile, 'w') as serr:
                cmd = self.cmd + ("" if chainArg is None else (" " + chainArg))
                Utils.Print("cmd: %s" % (cmd))
                popen = subprocess.Popen(cmd.split(), stdout=sout, stderr=serr)
                self.pid = popen.pid

        self.killed = False
        return True
