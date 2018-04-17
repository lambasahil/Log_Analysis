import psycopg2
pg=psycopg2.connect("dbname=news")
cursor=pg.cursor()

class LogAnalysis:
    def __init__(self):
        check_query = "SELECT EXISTS (SELECT 1 from information_schema.tables where table_schema = 'public' and table_name ='v1');"
        cursor.execute(check_query)
        valid=cursor.fetchall()[0][0]

    view1="create view v1 as select path,count(*) as num from log group by path order by num desc;"
    cursor.execute(view1)
    view2="create view v2 as select articles.title,v1.num from articles,v1 where v1.path like concat('%',articles.slug) limit 3;"
    cursor.execute(view2)
    view3="create view v3 as select articles.author,articles.slug,v2.num from v2,articles where articles.title=v2.title;"
    cursor.execute(view3)
    view4="create view v4 as select author,sum(num) as view from v3 group by author order by view desc;"
    cursor.execute(view4)
    view5="create view v5 as select authors.name,v4.view from v4,authors where authors.id=v4.author;"
    cursor.execute(view5)
    view6="create view v6 as select date(time),count(status) as error from log where status='404 NOT FOUND' group by date order by date;"
    cursor.execute(view6)
    view7="create view v7 as select date(time),count(status) as status from log group by date order by date;"
    cursor.execute(view7)
    view8="create view v8 as select v7.date,round((100.0*v6.error)/v7.status,2) as percentage from v6,v7 where v6.date=v7.date;"
    cursor.execute(view8)

# for query 1 :-
cursor.execute("select * from v2 limit 3;")

result = cursor.fetchall()

print("THE LIST OF POPULAR ARTICLES ARE:")

for i in range(len(result)):
    print result[i][0],',',result[i][1],'views'
    # print("%s%d" % (result[i][0],result[i][1]))

# for query 2 :-
cursor.execute("select * from v5;")

result = cursor.fetchall()

print ("\n")
print("THE LIST OF POPULAR AUTHORS ARE:")

for i in range(len(result)):
    print result[i][0],',',result[i][1],'views'

# for query 3 :-
cursor.execute("select to_char(date,'Mon DD,YYYY') as date,percentage from v8 where percentage>1.0;")

result = cursor.fetchall()
print ("\n")
print("PERC ERROR MORE THAN 1.0:")

for i in range(len(result)):
    print result[i][0],',',result[i][1],'%'



pg.close()
