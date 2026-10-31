#!/bin/bash

/root/opskit/sbin/iotdbctl cluster stop object
sleep 5
/root/opskit/sbin/iotdbctl cluster destroy object
/root/opskit/sbin/iotdbctl cluster deploy object
/root/opskit/sbin/iotdbctl cluster start object