$KCODE = "u"
require 'rubygems'
require 'mongo'

db = Mongo::Connection.new.db("qrodoo_doc_raw")
coll = db["sohu_star"]
tot_count = coll.count

count = 0
records = {}
coll.find.each do |doc|
    if doc.has_key?("other_info")
        doc["context"] = doc["other_info"]
        doc.delete("other_info")
    end
    if doc.has_key?("parse_failed_time")
        doc["parse_try_times"] = doc["parse_failed_time"]
        doc.delete("parse_failed_time")
    end
    if doc.has_key?("updated_at") and doc.has_key?("page")
        doc["page_crawled_at"] = doc["updated_at"]
    end
    if doc["status_flag"] == 3
        doc["status_flag"] = 1
    end
    doc["stage"] = "star_page"
    if doc["context"].has_key?("page_type") and doc["context"]["page_type"] == "details"
        doc["stage"] = "star_details"
    end
    coll.update( { "url_hash" => doc["url_hash"] }, doc )
    
    count += 1
    if count % 100 == 0
        printf("[%d/%d] processed...\n", count, tot_count)
    end
end

