import data_utils


def process(date):
    game_data = get_game_ids(date)
    insert_games_to_stg(game_data)
    update_game_proc()
    print('Game processing complete')


def get_unfinished_games():
    # this function is necessary for when a game goes past 12pm EST and the
    # current date will not grab that data from the xml
    query = \
        '''
            SELECT game_date, game_id \
            FROM game_proc \
            WHERE game_status != 'FINAL' \
        '''
    results = data_utils.fetch_query(query)
    return results


def update_game_proc():
    ins_sql = \
        '''
            INSERT INTO game_proc \
            SELECT * FROM stg_game_proc \
            ON CONFLICT (game_id) DO UPDATE SET \
            game_status= EXCLUDED.game_status;
        '''
    data_utils.execute_sql(ins_sql)
    data_utils.execute_sql('TRUNCATE TABLE ' + data_utils.STG_GAME_PROC + ';')


def get_game_ids(date):
    url = data_utils.BASE_URL.format(
            date.year,
            date.month,
            date.day) + 'scoreboard.xml'
    xml = data_utils.get_root_xml(url)
    raw_games = [game.findall('game')[0] for game in xml]
    game_data = [{
        'game_date': date,
        'game_id': game.attrib['id'],
        'game_status': game.attrib['status']
        } for game in raw_games]
    print('Found {game_count} games'.format(game_count=len(game_data)))
    return game_data


def insert_games_to_stg(game_data):
    data_utils.insert_to_db(game_data, data_utils.STG_GAME_PROC)
