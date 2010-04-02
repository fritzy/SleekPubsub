#!/bin/bash
HOST="localhost:8282"
USER="admin"
PASS="secreteating"

#create a node
for i in {0..50000}
do
curl -s -u $USER:$PASS -d "{}" -H "Content-Type: text/json" http://$HOST/node/autotest$i
time curl -s -u $USER:$PASS -X GET http://$HOST/subscribe/autotest$i?jid=fritzy\@recon
#curl -s -u $USER:$PASS -d "<body xmlns='jabber:client'>testmsg</body>" -H "Content-Type: text/xml" http://$HOST/publish/autotest$i
#curl -s -u $USER:$PASS -X DELETE http://$HOST/node/autotest$i
done


#curl -u $USER:$PASS -d "<body xmlns='jabber:client'>Applying Commit $commit
#By: $author
#$log</body>" -H "Content-Type: text/xml" http://localhost:8080/publish/gitpush
