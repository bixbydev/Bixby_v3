#!/usr/bin/env python

import json
import gservice.directoryservice

ds = gservice.directoryservice.DirectoryService()
ou = ds.orgunits()

ou_list = []

for i in range(2, 28):
    cart = "MS 17-%s" % "%02d" %i
    j["name"] = cart
    print json.dumps(j)
    ou_list.append(j)
    u = ou.insert(customerId="my_customer", body=j)
    print u.execute()
