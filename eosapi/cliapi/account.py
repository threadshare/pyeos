#!/usr/bin/env python
# -*- coding:utf-8 -*


class Account(object):
    def __init__(self, name):
        self.name = name
        self.balance = 0

        self.ownerPrivateKey = None
        self.ownerPublicKey = None
        self.activePrivateKey = None
        self.activePublicKey = None

    def __str__(self):
        return "Name: {}".format(self.name)
