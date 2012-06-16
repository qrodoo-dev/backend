categories = [ "staryj", "starfw", "starcs", "stargz", "starjf",
               "starmt", "stardzb", "movie_cn", "movie_intl", "movie_focus",
               "movie_reying", "movie_people", "movie_comment", "direct_interview",
               "movie_yejie", "tvdomestic", "tvhaiwaijuchang", "tvzongyi",
               "tvyejie", "musicchinese", "musicworld"]

fo = File.new("../spider/seeds/yahoo_news.xml", "w")
fo.printf("<seeds>\n")
categories.each do |category|
    fo.printf("    <seed>\n")
    fo.printf("        <url>http://ent.cn.yahoo.com/event/%s/index.html?page=1</url>\n", category)
    fo.printf("        <stage>index</stage>\n") 
    fo.printf("        <context>\n")
    fo.printf("            <category>%s</category>\n", category)
    fo.printf("            <page_num type=\"int\">1</page_num>\n")
    fo.printf("        </context>\n") 
    fo.printf("        <update_time_interval>86400</update_time_interval>\n")
    fo.printf("    </seed>\n")
end
fo.printf("</seeds>\n")
