$KCODE = "u"
require 'rubygems'
require 'mongo'

db = Mongo::Connection.new.db("qrodoo_doc_raw")
coll = db["yisou_all"]
tot_count = coll.count

count = 0
records = {}
coll.find({"status_flag" => 4}).each do |doc|
    doc["status_flag"] = 0
    coll.update({"url_hash" => doc["url_hash"]}, doc)
    printf("%s updated\n", doc["url_hash"])
end


