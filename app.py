import urllib
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from urllib.error import HTTPError
import argparse
import pandas as pd
import sqlalchemy as sa

BASE_URL = ('http://gd2.mlb.com/components/game/mlb/'
            'year_{0}/month_{1:02d}/day_{2:02d}/')
GAME_URL = BASE_URL + 'gid_{3}/{4}'
PROPERTY_URL = 'http://mlb.mlb.com/properties/mlb_properties.xml'
ROSTER_URL = 'http://mlb.mlb.com/lookup/json/named.roster_40.bam?team_id={0}'
CONN_STR = "postgresql+psycopg2://sjg:123okdef@aai486zsxs0c9a.cmcbqmsljxw8.us-east-2.rds.amazonaws.com:5432/ebdb"
PLAYER_BATTING = "player_batting"
STG_PLAYER = "stg_player"


def get_root_xml(url):
    xml = urllib.request.urlopen(url).read()
    return ET.fromstring(xml)


def insert_to_db(engine, data, table):
    print('Inserting {count} rows to {table}'.format(
        count=len(data),
        table=table))
    df = pd.DataFrame(data).transpose()
    df.to_sql(name=table, con=engine, if_exists='append', index=False)
    print('Insert successful')


def player_batting_insert(data):
    engine = sa.create_engine(CONN_STR)
    try:
        insert_to_db(engine, data, PLAYER_BATTING)
    except sa.exc.IntegrityError:
        print('This game data already exists')


def player_insert_and_update(data):
    trunc_sql = 'TRUNCATE TABLE ' + STG_PLAYER + ';'
    ins_sql = \
        '''
            INSERT INTO player \
            SELECT * FROM stg_player \
            ON CONFLICT (player_id) DO UPDATE SET \
            first_name = EXCLUDED.first_name, \
            last_name = EXCLUDED.last_name, \
            team_id = EXCLUDED.team_id, \
            display_name= EXCLUDED.display_name;
        '''
    # TODO: fix this mess
    engine = sa.create_engine(CONN_STR)
    conn = engine.connect()
    conn.execute(trunc_sql)
    conn.close()
    insert_to_db(engine, data, STG_PLAYER)
    conn = engine.connect()
    conn.execute(ins_sql)
    conn.close()
    print('Ran update from stg_player to player')


def get_game_ids(year, month, day):
    url = BASE_URL.format(
            year,
            month,
            day) + 'scoreboard.xml'
    print('Retrieving data from {url}'.format(url=url))
    raw_xml_string = urllib.request.urlopen(url).read()
    xml_root = ET.fromstring(raw_xml_string)
    raw_games = [game.findall('game')[0] for game in xml_root]
    game_ids = [
        (game.attrib['id'], game.attrib['status']) for game in raw_games
        ]
    print('Found {game_count} games'.format(game_count=len(game_ids)))
    return game_ids


def get_player_hrs(game_id):
    game_date = get_game_date_from_id(game_id)
    game_datetime = \
        datetime(game_date['year'], game_date['month'], game_date['day'])
    url = get_xml_url(game_id, 'boxscore')
    boxscore = get_root_xml(url)
    batter_hrs = {}
    for team in boxscore.findall('team'):
        for batting in team.findall('batting'):
            batters = batting.findall('batter')
            for batter in batters:
                batter_hrs[int(batter.attrib['id'])] = {
                    'player_id': int(batter.attrib['id']),
                    'team_id': team.attrib['id'],
                    'pos': batter.attrib['pos'],
                    'game_id': game_id,
                    'game_date': game_datetime,
                    'hr': int(batter.attrib['hr']),
                    'ab': int(batter.attrib['ab']),
                    'r': int(batter.attrib['r']),
                    'h': int(batter.attrib['h']),
                    'rbi': int(batter.attrib['rbi']),
                    'bb': int(batter.attrib['bb']),
                    'so': int(batter.attrib['so']),
                    'bis_avg': float(batter.attrib['bis_avg']),
                    'bam_avg': float(batter.attrib['bis_avg']),
                    'bis_obp': float(batter.attrib['bis_obp']),
                    'bam_obp': float(batter.attrib['bam_obp']),
                    'bis_slg': float(batter.attrib['bis_slg']),
                    'bam_slg': float(batter.attrib['bam_obp']),
                }
    return batter_hrs


def get_game_date_from_id(game_id):
    split = game_id.split('_')
    return {
        'year': int(split[0]), 'month': int(split[1]), 'day': int(split[2])
        }


def get_xml_url(game_id, file_type):
    if file_type == 'boxscore':
        file = 'rawboxscore.xml'
    elif file_type == 'players':
        file = 'players.xml'
    date = get_game_date_from_id(game_id)
    return GAME_URL.format(
                        date['year'],
                        date['month'],
                        date['day'],
                        game_id,
                        file
                    )


def get_stats_by_day(year, month, day):
    game_ids = get_game_ids(year, month, day)
    all_player_hrs = {}
    for game in game_ids:
        # game is finished- postponed games have no data
        # but are marked final so we need a try
        if game[1] == 'FINAL':
            try:
                game_hrs = get_player_hrs(game[0])
                for player_id, hr in game_hrs.items():
                    all_player_hrs[player_id] = hr
            except HTTPError:
                print('No XML found for game: ' + game[0])
                pass
    return all_player_hrs


def get_player_data(year, month, day):
    game_ids = get_game_ids(year, month, day)
    all_players = {}
    for game_id in game_ids:
        if game_id[1] == 'FINAL':
            url = get_xml_url(game_id[0], 'players')
            try:
                xml = get_root_xml(url)
            except HTTPError:
                print('XML not found: ' + url)
            print('Parsing XML', url)
            for teams in xml.findall('team'):
                for player in teams.findall('player'):
                    all_players[player.attrib['id']] = {
                        'player_id': player.attrib['id'],
                        'first_name': player.attrib['first'],
                        'last_name': player.attrib['last'],
                        'team_id': int(player.attrib['team_id']),
                        'display_name':
                        player.attrib['first'] + ' ' + player.attrib['last']
                    }
    print('Parsed all player XML')
    return all_players


def run_etl(args):
    if args.date:
        date = datetime.strptime(args.date, '%Y-%m-%d')
    elif not args.date:
        date = datetime.now() - timedelta(days=1)
    if args.batting:
        hitting_data = get_stats_by_day(date.year, date.month, date.day)
        player_batting_insert(hitting_data)
        print(date, 'batting ETL complete!')
    elif args.player:
        player_data = get_player_data(date.year, date.month, date.day)
        player_insert_and_update(player_data)
        print(date, 'player ETL complete!')
    else:
        hitting_data = get_stats_by_day(date.year, date.month, date.day)
        player_batting_insert(hitting_data)
        player_data = get_player_data(date.year, date.month, date.day)
        player_insert_and_update(player_data)
        print(date, 'all ETLs complete!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', help='date to run', type=str)
    parser.add_argument('-a', '--all', help='run all etl', action='store_true')
    parser.add_argument('-b', '--batting', help='run etl for player_batting', action='store_true')
    parser.add_argument('-p', '--player', help='run etl for player dimension', action='store_true')
    run_etl(parser.parse_args())
