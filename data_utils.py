import urllib.request
from urllib.error import HTTPError
import xml.etree.ElementTree as ET
import pandas as pd
import sqlalchemy as sa
import functools
import config

STG_GAME_PROC = 'stg_game_proc'
GAME_PROC = 'game_proc'
PLAYER_BATTING = "player_batting"
STG_PLAYER_BATTING = "stg_player_batting"
STG_PLAYER = "stg_player"
BASE_URL = ('http://gd2.mlb.com/components/game/mlb/'
            'year_{0}/month_{1:02d}/day_{2:02d}/')
GAME_URL = BASE_URL + 'gid_{3}/{4}'


def mark_games_processed(games, etl):
    # TODO: refactor so this runs one statement, instead of one for every game
    col = 'stats_etl_complete' if etl == 'stats' else 'player_etl_complete'
    for game in games:
        if game[2] == 'FINAL':
            sql = '''UPDATE game_proc \
                     SET {col} = True \
                     WHERE game_id = '{game_id}' \
                  '''.format(col=col, game_id=game[1])
            execute_sql(sql)


def get_unprocessed_games_stats():
    query = ''' \
        SELECT game_date, game_id, game_status \
        FROM game_proc \
        WHERE stats_etl_complete = False
        '''
    return fetch_query(query)


def get_unprocessed_games_player():
    query = ''' \
        SELECT game_date, game_id \
        FROM game_proc \
        WHERE player_etl_complete = False \
        '''
    return fetch_query(query)


def join_units(unit_collection):
    return functools.reduce(lambda x, y: x + y, unit_collection)


def insert_to_db(data, table):
    engine = get_engine()
    print('Inserting {count} rows to {table}'.format(
        count=len(data),
        table=table))
    df = pd.DataFrame(data)
    df.to_sql(name=table, con=engine, if_exists='append', index=False)
    print('Insert successful')


def get_engine():
    return sa.create_engine(config.CONN_STR)


def fetch_query(sql):
    engine = get_engine()
    with engine.connect() as conn:
        query_results = conn.execute(sql)
        return query_results.fetchall()


def execute_sql(sql_str):
    print('Executing', sql_str)
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(sa.sql.text(sql_str).execution_options(autocommit=True))


def get_root_xml(url):
    print('Retrieving data from {url}'.format(url=url))
    try:
        xml = urllib.request.urlopen(url).read()
        return ET.fromstring(xml)
    except HTTPError:
        print('XML could not be found', url)


def get_game_url(game, xml_filename):
    date = game[0]
    return GAME_URL.format(
                date.year,
                date.month,
                date.day,
                game[1],
                xml_filename)
