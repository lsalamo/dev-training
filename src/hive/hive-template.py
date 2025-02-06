from pyhive import hive


def main():
    query = 'show databases'
    query = """select
                 to_date(`originalTimestamp`) as fecha,
                 anonymousId,
                 properties.ad_id
               from ref_coches_events_behaviour.adfavorited
               where
                   dt = '20210818'
                   and anonymousId <> 'anonymous_user'
               limit 100"""
    cursor = hive.connect('sts-users.pro.di.spain.mpi-internal.com', username='tu.username', password='tu_password', auth='CUSTOM' ).cursor()
    cursor.arraysize = 100000
    cursor.execute(query)
    print (cursor.fetchall())


if __name__=="__main__":
    main()
