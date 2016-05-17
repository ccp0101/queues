import requests
import sys
import time

# api_base = sys.argv[1]
api_base = "http://localhost:17901"

# home
r = requests.get(api_base + "/")
assert(r.status_code == 200)

with open("/dev/urandom", "rb") as f:
    randbytes = f.read(32)
    qid = randbytes.encode("hex")
    print "Queue: ", qid

r = requests.post(api_base + "/new/" + qid)
assert(r.status_code == 200)
r = requests.post(api_base + "/new/" + qid)
assert(r.status_code == 400)

r = requests.get(api_base + "/queues")
assert(r.status_code == 200)
assert(qid in set(r.content.splitlines()))

r = requests.post(api_base + "/delete/" + qid)
assert(r.status_code == 200)
r = requests.post(api_base + "/delete/" + qid)
assert(r.status_code == 404)

r = requests.post(api_base + "/new/" + qid)
assert(r.status_code == 200)

r = requests.get(api_base + "/show/" + qid)
assert(r.status_code == 200)
assert(r.content.strip() == "Done: 0. Pending: 0. Queued: 0. All: 0.")

r = requests.post(api_base + "/enqueue/" + qid, data={"item": "x1"})
assert(r.status_code == 200)
r = requests.post(api_base + "/enqueue/" + qid, data={"item": "x2"})
assert(r.status_code == 200)
r = requests.post(api_base + "/enqueue/" + qid, data={"item": "x3"})
assert(r.status_code == 200)
r = requests.post(api_base + "/enqueue/" + qid, data={"item": "x4"})
assert(r.status_code == 200)

r = requests.get(api_base + "/show/" + qid)
assert(r.status_code == 200)
assert(r.content.strip() == "Done: 0. Pending: 0. Queued: 4. All: 4.")

r = requests.post(api_base + "/next/" + qid)
assert(r.status_code == 200)
popped = r.content.strip()
assert(len(popped) > 0)

r = requests.post(api_base + "/ttl/" + qid, data={"item": popped})
assert(r.status_code == 200)
print r.content

time.sleep(5)

r = requests.get(api_base + "/show/" + qid + "/pending")
assert(r.status_code == 200)
print r.content

r = requests.post(api_base + "/ttl/" + qid, data={"item": popped})
assert(r.status_code == 200)
print r.content
assert(int(r.content.strip()) <= 300 - 5)

r = requests.post(api_base + "/expire/" + qid, data={"item": popped})
assert(r.status_code == 200)

time.sleep(10)

r = requests.get(api_base + "/show/" + qid + "/pending")
assert(r.status_code == 200)
assert(r.content.strip() == "")


r = requests.post(api_base + "/next/" + qid)
assert(r.status_code == 200)
p1 = r.content.strip()
assert(len(p1) > 0)

r = requests.post(api_base + "/next/" + qid)
assert(r.status_code == 200)
p2 = r.content.strip()
assert(len(p2) > 0)

r = requests.post(api_base + "/next/" + qid)
assert(r.status_code == 200)
p3 = r.content.strip()
assert(len(p3) > 0)

r = requests.post(api_base + "/next/" + qid)
assert(r.status_code == 200)
p4 = r.content.strip()
assert(len(p4) > 0)

r = requests.post(api_base + "/next/" + qid)
assert(r.status_code == 200)
empty = r.content.strip()
assert(len(empty) == 0)

r = requests.post(api_base + "/done/" + qid, data={"item": p1})
assert(r.status_code == 200)

r = requests.get(api_base + "/show/" + qid)
assert(r.status_code == 200)
assert(r.content.strip() == "Done: 1. Pending: 3. Queued: 0. All: 4.")

time.sleep(5)
r = requests.post(api_base + "/extend/" + qid, data={"item": p1})
assert(r.status_code == 200)

r = requests.post(api_base + "/ttl/" + qid, data={"item": p1})
assert(r.status_code == 200)
assert(int(r.content.strip()) > 300-5)
